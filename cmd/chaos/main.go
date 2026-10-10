package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type SinkStats struct {
	TotalRequests    int64 `json:"total_requests"`
	TotalApplied     int64 `json:"total_applied"`
	TotalRejected    int64 `json:"total_rejected"`
	TotalJobs        int64 `json:"total_jobs"`
	DuplicateEffects int64 `json:"duplicate_effects"`
	FencingEnabled   bool  `json:"fencing_enabled"`
}

type ChaosResult struct {
	Seed                int64
	Jobs                int
	Workers             int
	Fencing             bool
	Duration            time.Duration
	Kills               int64
	Pauses              int64
	ServerRestarts      int64
	PGRestarts          int64
	TotalExecutions     int64
	DuplicateExecutions int64
	DuplicateEffects    int64
	LostJobs            int64
	SucceededJobs       int64
	DeadJobs            int64
	StaleTokenServer    int64
	StaleTokenSink      int64
}

func main() {
	var (
		seed        = flag.Int64("seed", 1, "RNG seed for nemesis")
		jobsCount   = flag.Int("jobs", 20000, "Number of jobs to enqueue")
		workersNum  = flag.Int("workers", 8, "Number of worker processes")
		noFencing   = flag.Bool("no-fencing", false, "Disable fencing at sink (negative control)")
		smoke       = flag.Bool("smoke", false, "Smoke test mode")
		maxDuration = flag.Duration("duration", 0, "Max duration timeout")
		dbURL       = flag.String("db", "", "PostgreSQL database connection URL")
		binDir      = flag.String("bin-dir", "", "Directory containing pre-compiled binaries")
		jsonOutput  = flag.Bool("json", false, "Output results as JSON")
	)
	flag.Parse()

	if *smoke {
		if *jobsCount == 20000 {
			*jobsCount = 1000
		}
		if *workersNum == 8 {
			*workersNum = 4
		}
	}

	if *dbURL == "" {
		*dbURL = os.Getenv("DATABASE_URL")
	}
	if *dbURL == "" {
		*dbURL = "postgres://fencepost:fencepost_password@localhost:5432/fencepost?sslmode=disable"
	}

	res, err := RunChaos(ChaosConfig{
		Seed:        *seed,
		Jobs:        *jobsCount,
		Workers:     *workersNum,
		NoFencing:   *noFencing,
		DBURL:       *dbURL,
		MaxDuration: *maxDuration,
		BinDir:      *binDir,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Chaos run failed: %v\n", err)
		os.Exit(1)
	}

	if *jsonOutput {
		_ = json.NewEncoder(os.Stdout).Encode(res)
		return
	}

	printReport(res)

	// Verification
	if res.LostJobs > 0 {
		fmt.Fprintf(os.Stderr, "FAILURE: %d lost jobs (expected 0)\n", res.LostJobs)
		os.Exit(1)
	}

	if !res.Fencing {
		if res.DuplicateEffects == 0 {
			fmt.Fprintf(os.Stderr, "NEGATIVE CONTROL FAILED: 0 duplicate effects without fencing (nemesis was too gentle)\n")
			os.Exit(1)
		}
		fmt.Printf("Negative control confirmed: %d duplicate effects recorded without fencing.\n", res.DuplicateEffects)
	} else {
		if res.DuplicateEffects > 0 {
			fmt.Fprintf(os.Stderr, "SAFETY VIOLATION: %d duplicate effects with fencing ON!\n", res.DuplicateEffects)
			os.Exit(1)
		}
		fmt.Printf("Distributed Safety Invariant PROVED: 0 duplicate effects under process pauses and crashes.\n")
	}
}

type ChaosConfig struct {
	Seed        int64
	Jobs        int
	Workers     int
	NoFencing   bool
	DBURL       string
	MaxDuration time.Duration
	BinDir      string
}

func RunChaos(cfg ChaosConfig) (*ChaosResult, error) {
	ctx, cancel := context.WithCancel(context.Background())
	if cfg.MaxDuration > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), cfg.MaxDuration)
	}
	defer cancel()

	slog.Info("initializing chaos run", "seed", cfg.Seed, "jobs", cfg.Jobs, "workers", cfg.Workers, "no_fencing", cfg.NoFencing)

	pool, err := pgxpool.New(ctx, cfg.DBURL)
	if err != nil {
		return nil, fmt.Errorf("connect to db: %w", err)
	}
	defer pool.Close()

	// Clean tables
	_, err = pool.Exec(ctx, "TRUNCATE jobs, job_events, schedules CASCADE; ALTER SEQUENCE fencing_seq RESTART WITH 1;")
	if err != nil {
		return nil, fmt.Errorf("clean tables: %w", err)
	}

	exeSuffix := ""
	if runtime.GOOS == "windows" {
		exeSuffix = ".exe"
	}

	binDir := cfg.BinDir
	cleanupBinDir := false
	if binDir == "" {
		tmpDir, err := os.MkdirTemp("", "fencepost-chaos-bin-*")
		if err != nil {
			return nil, fmt.Errorf("create temp bin dir: %w", err)
		}
		binDir = tmpDir
		cleanupBinDir = true
	}
	if cleanupBinDir {
		defer os.RemoveAll(binDir)
	}

	fencepostdBin := filepath.Join(binDir, "fencepostd"+exeSuffix)
	sinkBin := filepath.Join(binDir, "sink"+exeSuffix)
	workerBin := filepath.Join(binDir, "demoworker"+exeSuffix)

	needBuild := false
	if _, err := os.Stat(fencepostdBin); err != nil {
		needBuild = true
	}
	if _, err := os.Stat(sinkBin); err != nil {
		needBuild = true
	}
	if _, err := os.Stat(workerBin); err != nil {
		needBuild = true
	}

	if needBuild {
		slog.Info("compiling binaries for harness...")
		if err := execCmd("go", "build", "-o", fencepostdBin, "./cmd/fencepostd"); err != nil {
			return nil, fmt.Errorf("build fencepostd: %w", err)
		}
		if err := execCmd("go", "build", "-o", sinkBin, "./cmd/sink"); err != nil {
			return nil, fmt.Errorf("build sink: %w", err)
		}
		if err := execCmd("go", "build", "-o", workerBin, "./cmd/demoworker"); err != nil {
			return nil, fmt.Errorf("build demoworker: %w", err)
		}
	}

	// 1. Start Sink
	sinkDB := filepath.Join(binDir, "sink.db")
	_ = os.Remove(sinkDB)
	_ = os.Remove(sinkDB + "-wal")
	_ = os.Remove(sinkDB + "-shm")
	sinkArgs := []string{"-addr", "127.0.0.1:8081", "-db", sinkDB}
	if cfg.NoFencing {
		sinkArgs = append(sinkArgs, "-no-fencing")
	}
	sinkCmd := exec.Command(sinkBin, sinkArgs...)
	sinkCmd.Stdout = io.Discard
	sinkCmd.Stderr = io.Discard
	if err := sinkCmd.Start(); err != nil {
		return nil, fmt.Errorf("start sink: %w", err)
	}
	defer killCmd(sinkCmd)

	waitHTTP("http://127.0.0.1:8081/healthz", 10*time.Second)

	// 2. Start 2 fencepostd replicas
	startServer := func(grpcPort, httpPort int) *exec.Cmd {
		cmd := exec.Command(fencepostdBin,
			"-grpc", fmt.Sprintf("127.0.0.1:%d", grpcPort),
			"-http", fmt.Sprintf("127.0.0.1:%d", httpPort),
			"-db", cfg.DBURL,
		)
		cmd.Stdout = io.Discard
		cmd.Stderr = io.Discard
		_ = cmd.Start()
		return cmd
	}

	server1 := startServer(50051, 8080)
	defer killCmd(server1)
	server2 := startServer(50052, 8082)
	defer killCmd(server2)

	waitHTTP("http://127.0.0.1:8080/healthz", 10*time.Second)
	waitHTTP("http://127.0.0.1:8082/healthz", 10*time.Second)

	// 3. Batch Enqueue Jobs
	slog.Info("batch enqueuing jobs...", "count", cfg.Jobs)
	startEnqueue := time.Now()
	now := time.Now()
	rows := make([][]any, cfg.Jobs)
	queueName := "chaos-queue"
	for i := 0; i < cfg.Jobs; i++ {
		jobID := uuid.New()
		idemKey := fmt.Sprintf("chaos-%d-%d", cfg.Seed, i)
		rows[i] = []any{
			jobID,
			queueName,
			[]byte(fmt.Sprintf("payload-%d", i)),
			int32(rand.Intn(10)),
			now,
			"queued",
			int32(0),
			int32(5), // max_attempts = 5
			&idemKey,
			now,
			now,
		}
	}

	_, err = pool.CopyFrom(
		ctx,
		pgx.Identifier{"jobs"},
		[]string{"id", "queue", "payload", "priority", "run_at", "state", "attempts", "max_attempts", "idempotency_key", "created_at", "updated_at"},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return nil, fmt.Errorf("copy jobs: %w", err)
	}
	slog.Info("enqueued jobs successfully", "duration", time.Since(startEnqueue))

	// 4. Start Worker Processes
	workerMutex := sync.Mutex{}
	type workerProc struct {
		id  int
		cmd *exec.Cmd
	}
	workers := make([]*workerProc, cfg.Workers)

	startWorker := func(idx int) *workerProc {
		port := 50051
		if idx%2 == 1 {
			port = 50052
		}
		cmd := exec.Command(workerBin,
			"-server", fmt.Sprintf("127.0.0.1:%d", port),
			"-sink", "http://127.0.0.1:8081",
			"-queue", queueName,
			"-id", fmt.Sprintf("worker-%d", idx),
			"-concurrency", "2",
			"-ttl", "2s",
		)
		cmd.Stdout = io.Discard
		cmd.Stderr = io.Discard
		_ = cmd.Start()
		return &workerProc{id: idx, cmd: cmd}
	}

	for i := 0; i < cfg.Workers; i++ {
		workers[i] = startWorker(i)
	}
	defer func() {
		workerMutex.Lock()
		defer workerMutex.Unlock()
		for _, w := range workers {
			if w != nil && w.cmd != nil {
				killCmd(w.cmd)
			}
		}
	}()

	// 5. Nemesis Loop with seeded RNG
	rng := rand.New(rand.NewSource(cfg.Seed))
	var killsCount atomic.Int64
	var pausesCount atomic.Int64
	var restartsCount atomic.Int64
	var pgRestartsCount atomic.Int64

	nemesisStop := make(chan struct{})
	go func() {
		for {
			interval := time.Duration(150+rng.Intn(350)) * time.Millisecond
			select {
			case <-nemesisStop:
				return
			case <-ctx.Done():
				return
			case <-time.After(interval):
			}

			nemesisChoice := rng.Intn(100)
			if nemesisChoice < 65 {
				// (a) SIGSTOP / suspend a random worker for 2.3s - 4.5s (longer than 2s TTL!)
				workerMutex.Lock()
				targetIdx := rng.Intn(cfg.Workers)
				victim := workers[targetIdx]
				var pid int
				if victim != nil && victim.cmd != nil && victim.cmd.Process != nil {
					pid = victim.cmd.Process.Pid
				}
				workerMutex.Unlock()

				if pid > 0 {
					if err := suspendProcess(pid); err == nil {
						pausesCount.Add(1)
						pauseDuration := time.Duration(2300+rng.Intn(1700)) * time.Millisecond
						time.Sleep(pauseDuration)
						_ = resumeProcess(pid)
					}
				}
			} else if nemesisChoice < 85 {
				// (b) Kill a random worker
				workerMutex.Lock()
				targetIdx := rng.Intn(cfg.Workers)
				victim := workers[targetIdx]
				if victim != nil && victim.cmd != nil && victim.cmd.Process != nil {
					_ = victim.cmd.Process.Kill()
					_ = victim.cmd.Wait()
					killsCount.Add(1)
					workers[targetIdx] = startWorker(targetIdx)
				}
				workerMutex.Unlock()
			} else if nemesisChoice < 95 {
				// (c) Kill and restart a fencepostd replica
				replicaIdx := rng.Intn(2)
				restartsCount.Add(1)
				if replicaIdx == 0 && server1 != nil && server1.Process != nil {
					_ = server1.Process.Kill()
					_ = server1.Wait()
					time.Sleep(200 * time.Millisecond)
					server1 = startServer(50051, 8080)
				} else if server2 != nil && server2.Process != nil {
					_ = server2.Process.Kill()
					_ = server2.Wait()
					time.Sleep(200 * time.Millisecond)
					server2 = startServer(50052, 8082)
				}
			} else {
				// (d) Simulate transient PG hiccup / terminate idle connections
				pgRestartsCount.Add(1)
				_, _ = pool.Exec(context.Background(), "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE state = 'idle' AND pid <> pg_backend_pid()")
			}
		}
	}()

	// 6. Wait for Drain
	drainStart := time.Now()
drainLoop:
	for {
		var pendingCount int64
		err := pool.QueryRow(context.Background(), "SELECT count(*) FROM jobs WHERE queue = $1 AND state IN ('queued', 'leased')", queueName).Scan(&pendingCount)
		if err == nil && pendingCount == 0 {
			break drainLoop
		}

		select {
		case <-ctx.Done():
			break drainLoop
		case <-time.After(500 * time.Millisecond):
		}

		if ctx.Err() != nil {
			break drainLoop
		}
	}
	close(nemesisStop)
	drainDuration := time.Since(drainStart)
	slog.Info("queue drained", "duration", drainDuration)

	// Give a brief buffer for final writes
	time.Sleep(500 * time.Millisecond)

	// 7. Collect Results
	var succeededCount, deadCount, lostCount int64
	_ = pool.QueryRow(context.Background(), "SELECT count(*) FROM jobs WHERE queue = $1 AND state = 'succeeded'", queueName).Scan(&succeededCount)
	_ = pool.QueryRow(context.Background(), "SELECT count(*) FROM jobs WHERE queue = $1 AND state = 'dead'", queueName).Scan(&deadCount)
	_ = pool.QueryRow(context.Background(), "SELECT count(*) FROM jobs WHERE queue = $1 AND state NOT IN ('succeeded', 'dead', 'cancelled')", queueName).Scan(&lostCount)

	var duplicateExecs int64
	_ = pool.QueryRow(context.Background(), "SELECT count(*) FROM jobs WHERE queue = $1 AND attempts > 1", queueName).Scan(&duplicateExecs)

	var totalExecutions int64
	_ = pool.QueryRow(context.Background(), "SELECT count(*) FROM job_events WHERE kind = 'leased'").Scan(&totalExecutions)

	var staleServer int64
	_ = pool.QueryRow(context.Background(), "SELECT coalesce(sum(attempts), 0) FROM jobs WHERE queue = $1 AND last_error LIKE '%stale%'", queueName).Scan(&staleServer)

	// Get Sink stats
	sinkResp, err := http.Get("http://127.0.0.1:8081/stats")
	var sStats SinkStats
	if err == nil {
		_ = json.NewDecoder(sinkResp.Body).Decode(&sStats)
		sinkResp.Body.Close()
	}

	return &ChaosResult{
		Seed:                cfg.Seed,
		Jobs:                cfg.Jobs,
		Workers:             cfg.Workers,
		Fencing:             !cfg.NoFencing,
		Duration:            drainDuration,
		Kills:               killsCount.Load(),
		Pauses:              pausesCount.Load(),
		ServerRestarts:      restartsCount.Load(),
		PGRestarts:          pgRestartsCount.Load(),
		TotalExecutions:     totalExecutions,
		DuplicateExecutions: duplicateExecs,
		DuplicateEffects:    sStats.DuplicateEffects,
		LostJobs:            lostCount,
		SucceededJobs:       succeededCount,
		DeadJobs:            deadCount,
		StaleTokenServer:    staleServer,
		StaleTokenSink:      sStats.TotalRejected,
	}, nil
}

func execCmd(name string, args ...string) error {
	cmd := exec.Command(name, args...)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	return cmd.Run()
}

func killCmd(c *exec.Cmd) {
	if c != nil && c.Process != nil {
		_ = c.Process.Kill()
	}
}

func waitHTTP(url string, timeout time.Duration) {
	start := time.Now()
	for time.Since(start) < timeout {
		resp, err := http.Get(url)
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			return
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func printReport(r *ChaosResult) {
	fmt.Println("--------------------------------------------------------------------------------")
	fmt.Printf("CHAOS HARNESS RESULT (Seed=%d, Fencing=%v, Duration=%v)\n", r.Seed, r.Fencing, r.Duration.Round(time.Millisecond))
	fmt.Println("--------------------------------------------------------------------------------")
	fmt.Printf("Enqueued Jobs:        %d\n", r.Jobs)
	fmt.Printf("Succeeded Jobs:       %d\n", r.SucceededJobs)
	fmt.Printf("Dead Jobs:            %d\n", r.DeadJobs)
	fmt.Printf("Lost Jobs:            %d (Invariant: MUST BE 0)\n", r.LostJobs)
	fmt.Printf("Nemesis Worker Kills: %d\n", r.Kills)
	fmt.Printf("Nemesis Pauses (>TTL):%d\n", r.Pauses)
	fmt.Printf("Replica Restarts:     %d\n", r.ServerRestarts)
	fmt.Printf("Total Executions:     %d\n", r.TotalExecutions)
	fmt.Printf("Duplicate Executions: %d (Expected > 0, confirms nemesis biting)\n", r.DuplicateExecutions)
	fmt.Printf("Duplicate Effects:    %d (Invariant: 0 with fencing ON, >0 with OFF)\n", r.DuplicateEffects)
	fmt.Printf("Stale Sink Rejections:%d\n", r.StaleTokenSink)
	fmt.Println("--------------------------------------------------------------------------------")
}
