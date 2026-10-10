package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"sync/atomic"

	_ "modernc.org/sqlite"
)

type EffectRequest struct {
	JobID string `json:"job_id"`
	Token int64  `json:"token"`
	Value string `json:"value"`
}

type StatsResponse struct {
	TotalRequests    int64          `json:"total_requests"`
	TotalApplied     int64          `json:"total_applied"`
	TotalRejected    int64          `json:"total_rejected"`
	TotalJobs        int64          `json:"total_jobs"`
	DuplicateEffects int64          `json:"duplicate_effects"`
	FencingEnabled   bool           `json:"fencing_enabled"`
	EffectsCount     map[string]int `json:"effects_count,omitempty"`
}

type SinkServer struct {
	noFencing bool
	db        *sql.DB
	mu        sync.RWMutex

	totalRequests atomic.Int64
	totalApplied  atomic.Int64
	totalRejected atomic.Int64
}

func NewSinkServer(dbPath string, noFencing bool) (*SinkServer, error) {
	if dbPath == "" {
		dbPath = ":memory:"
	}
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	_, _ = db.Exec(`PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL; PRAGMA busy_timeout = 5000;`)

	const schema = `
		CREATE TABLE IF NOT EXISTS max_tokens (
			job_id TEXT PRIMARY KEY,
			max_token INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS effects (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			job_id TEXT NOT NULL,
			token INTEGER NOT NULL,
			value TEXT NOT NULL,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);
		CREATE INDEX IF NOT EXISTS idx_effects_job_id ON effects(job_id);
	`
	if _, err := db.Exec(schema); err != nil {
		return nil, fmt.Errorf("init sink schema: %w", err)
	}

	return &SinkServer{
		noFencing: noFencing,
		db:        db,
	}, nil
}

func (s *SinkServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /effects", s.handleEffects)
	mux.HandleFunc("GET /stats", s.handleStats)
	mux.HandleFunc("POST /reset", s.handleReset)
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	return mux
}

func (s *SinkServer) handleEffects(w http.ResponseWriter, r *http.Request) {
	s.totalRequests.Add(1)

	var req EffectRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if req.JobID == "" {
		http.Error(w, "job_id required", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.db.Begin()
	if err != nil {
		http.Error(w, "database error", http.StatusInternalServerError)
		return
	}
	defer tx.Rollback()

	if !s.noFencing {
		var maxToken int64 = 0
		var appliedCount int = 0
		row := tx.QueryRow("SELECT max_token, (SELECT count(*) FROM effects WHERE job_id = ?) FROM max_tokens WHERE job_id = ?", req.JobID, req.JobID)
		err := row.Scan(&maxToken, &appliedCount)
		if err == nil {
			// Kleppmann's fencing check: token must be >= max_token and effect not already applied
			if req.Token < maxToken || appliedCount > 0 {
				s.totalRejected.Add(1)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusConflict)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"error":     "stale fencing token or duplicate effect",
					"token":     req.Token,
					"max_token": maxToken,
				})
				return
			}
		}

		// Update max_token
		_, err = tx.Exec(`
			INSERT INTO max_tokens (job_id, max_token) VALUES (?, ?)
			ON CONFLICT(job_id) DO UPDATE SET max_token = MAX(max_tokens.max_token, excluded.max_token);
		`, req.JobID, req.Token)
		if err != nil {
			http.Error(w, "failed to update max_token", http.StatusInternalServerError)
			return
		}
	}

	// Apply effect: append value to effects table
	_, err = tx.Exec("INSERT INTO effects (job_id, token, value) VALUES (?, ?, ?)", req.JobID, req.Token, req.Value)
	if err != nil {
		http.Error(w, "failed to insert effect", http.StatusInternalServerError)
		return
	}

	if err := tx.Commit(); err != nil {
		http.Error(w, "commit error", http.StatusInternalServerError)
		return
	}

	s.totalApplied.Add(1)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"applied": true,
		"job_id":  req.JobID,
		"token":   req.Token,
	})
}

func (s *SinkServer) handleStats(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Compute duplicates
	rows, err := s.db.Query(`
		SELECT job_id, COUNT(*) as cnt
		FROM effects
		GROUP BY job_id
	`)
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var totalJobs int64
	var duplicateEffects int64
	counts := make(map[string]int)

	for rows.Next() {
		var jobID string
		var cnt int
		if err := rows.Scan(&jobID, &cnt); err != nil {
			continue
		}
		totalJobs++
		if cnt > 1 {
			duplicateEffects++
		}
		counts[jobID] = cnt
	}

	resp := StatsResponse{
		TotalRequests:    s.totalRequests.Load(),
		TotalApplied:     s.totalApplied.Load(),
		TotalRejected:    s.totalRejected.Load(),
		TotalJobs:        totalJobs,
		DuplicateEffects: duplicateEffects,
		FencingEnabled:   !s.noFencing,
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *SinkServer) handleReset(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, _ = s.db.Exec("DELETE FROM effects; DELETE FROM max_tokens;")
	s.totalRequests.Store(0)
	s.totalApplied.Store(0)
	s.totalRejected.Store(0)

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "reset"})
}

func (s *SinkServer) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func main() {
	var (
		addr      = flag.String("addr", ":8081", "HTTP listen address")
		dbPath    = flag.String("db", "sink.db", "SQLite database path")
		noFencing = flag.Bool("no-fencing", false, "Disable fencing check (negative control)")
	)
	flag.Parse()

	if envAddr := os.Getenv("ADDR"); envAddr != "" {
		*addr = envAddr
	}
	if envDB := os.Getenv("DB_PATH"); envDB != "" {
		*dbPath = envDB
	}
	if os.Getenv("NO_FENCING") == "1" || os.Getenv("NO_FENCING") == "true" {
		*noFencing = true
	}

	sink, err := NewSinkServer(*dbPath, *noFencing)
	if err != nil {
		slog.Error("failed to create sink", "error", err)
		os.Exit(1)
	}

	slog.Info("starting sink service", "addr", *addr, "no_fencing", *noFencing, "db", *dbPath)
	if err := http.ListenAndServe(*addr, sink.Handler()); err != nil {
		slog.Error("sink server stopped", "error", err)
	}
}
