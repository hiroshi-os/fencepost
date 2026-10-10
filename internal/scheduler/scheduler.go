package scheduler

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/hiroshi-os/fencepost/internal/metrics"
	"github.com/hiroshi-os/fencepost/internal/store"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/robfig/cron/v3"
)

const SchedulerAdvisoryLockID int64 = 424242

type CatchupPolicy string

const (
	CatchupNone CatchupPolicy = "none"
	CatchupLast CatchupPolicy = "last"
	CatchupAll  CatchupPolicy = "all"
)

type Schedule struct {
	ID         uuid.UUID
	Queue      string
	Cron       string
	Payload    []byte
	Enabled    bool
	LastFireAt *time.Time
}

type Scheduler struct {
	pool          *pgxpool.Pool
	store         store.Store
	policy        CatchupPolicy
	interval      time.Duration
	cronParser    cron.Parser
	isLeader      atomic.Bool
	dedicatedConn *pgxpool.Conn
}

func New(pool *pgxpool.Pool, st store.Store, policy CatchupPolicy, interval time.Duration) *Scheduler {
	if policy == "" {
		policy = CatchupLast
	}
	if interval <= 0 {
		interval = 1 * time.Second
	}
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	return &Scheduler{
		pool:       pool,
		store:      st,
		policy:     policy,
		interval:   interval,
		cronParser: parser,
	}
}

func (s *Scheduler) IsLeader() bool {
	return s.isLeader.Load()
}

func (s *Scheduler) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	defer s.stepDown(context.Background())

	if err := s.tick(ctx); err != nil && ctx.Err() == nil {
		slog.Error("scheduler initial tick error", "error", err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := s.tick(ctx); err != nil {
				slog.Error("scheduler tick error", "error", err)
			}
		}
	}
}

func (s *Scheduler) tick(ctx context.Context) error {
	if s.pool == nil {
		return nil
	}

	// Ensure dedicated connection for advisory lock
	if s.dedicatedConn == nil {
		conn, err := s.pool.Acquire(ctx)
		if err != nil {
			s.stepDown(ctx)
			return fmt.Errorf("acquire conn: %w", err)
		}
		s.dedicatedConn = conn
	}

	// Check / try acquire advisory lock
	var hasLock bool
	err := s.dedicatedConn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", SchedulerAdvisoryLockID).Scan(&hasLock)
	if err != nil {
		s.stepDown(ctx)
		return fmt.Errorf("query advisory lock: %w", err)
	}

	if !hasLock {
		if s.isLeader.Load() {
			s.stepDown(ctx)
		}
		return nil
	}

	if !s.isLeader.Load() {
		s.isLeader.Store(true)
		metrics.SchedulerIsLeader.Set(1)
		slog.Info("acquired scheduler leadership")
	}

	// Active leader: process schedules
	return s.ProcessSchedules(ctx, time.Now())
}

func (s *Scheduler) stepDown(ctx context.Context) {
	if s.dedicatedConn != nil {
		_, _ = s.dedicatedConn.Exec(ctx, "SELECT pg_advisory_unlock($1)", SchedulerAdvisoryLockID)
		s.dedicatedConn.Release()
		s.dedicatedConn = nil
	}
	if s.isLeader.Load() {
		s.isLeader.Store(false)
		metrics.SchedulerIsLeader.Set(0)
		slog.Info("stepped down from scheduler leadership")
	}
}

func (s *Scheduler) ProcessSchedules(ctx context.Context, now time.Time) error {
	rows, err := s.pool.Query(ctx, "SELECT id, queue, cron, payload, enabled, last_fire_at FROM schedules WHERE enabled = true")
	if err != nil {
		return fmt.Errorf("query schedules: %w", err)
	}
	defer rows.Close()

	var schedules []Schedule
	for rows.Next() {
		var sc Schedule
		if err := rows.Scan(&sc.ID, &sc.Queue, &sc.Cron, &sc.Payload, &sc.Enabled, &sc.LastFireAt); err != nil {
			return fmt.Errorf("scan schedule: %w", err)
		}
		schedules = append(schedules, sc)
	}
	rows.Close()

	for _, sc := range schedules {
		scheduleSpec, err := s.cronParser.Parse(sc.Cron)
		if err != nil {
			slog.Error("invalid cron expression", "schedule_id", sc.ID, "cron", sc.Cron, "error", err)
			continue
		}

		fireTimes := s.calculateFireTimes(sc, scheduleSpec, now)
		for _, ft := range fireTimes {
			idemKey := fmt.Sprintf("%s:%d", sc.ID.String(), ft.Unix())
			_, err := s.store.Enqueue(ctx, store.EnqueueParams{
				Queue:          sc.Queue,
				Payload:        sc.Payload,
				RunAt:          &ft,
				IdempotencyKey: &idemKey,
				MaxAttempts:    3,
			})
			if err != nil {
				slog.Error("failed to enqueue scheduled job", "schedule_id", sc.ID, "fire_time", ft, "error", err)
				continue
			}

			// Update last_fire_at
			_, _ = s.pool.Exec(ctx, "UPDATE schedules SET last_fire_at = $1, updated_at = now() WHERE id = $2", ft, sc.ID)
		}
	}

	return nil
}

func (s *Scheduler) calculateFireTimes(sc Schedule, spec cron.Schedule, now time.Time) []time.Time {
	if sc.LastFireAt == nil {
		// First time: initialize baseline to now
		prev := now.Add(-1 * time.Minute)
		next := spec.Next(prev)
		if !next.After(now) {
			return []time.Time{next}
		}
		return nil
	}

	last := *sc.LastFireAt
	var missed []time.Time
	curr := last
	for {
		next := spec.Next(curr)
		if next.After(now) {
			break
		}
		missed = append(missed, next)
		curr = next
		if len(missed) > 100 { // limit catchup runaway
			break
		}
	}

	if len(missed) == 0 {
		return nil
	}

	switch s.policy {
	case CatchupNone:
		return nil
	case CatchupLast:
		return []time.Time{missed[len(missed)-1]}
	case CatchupAll:
		return missed
	default:
		return []time.Time{missed[len(missed)-1]}
	}
}

var _ = errors.Is
var _ = sql.ErrNoRows
var _ = pgx.ErrNoRows
