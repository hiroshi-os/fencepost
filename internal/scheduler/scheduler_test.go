package scheduler_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hiroshi-os/fencepost/internal/scheduler"
	"github.com/hiroshi-os/fencepost/internal/store/pgstore"
)

func getTestDatabaseURL() string {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		url = "postgres://fencepost:fencepost_password@localhost:5432/fencepost?sslmode=disable"
	}
	return url
}

func TestCatchupPolicies(t *testing.T) {
	ctx := context.Background()
	connStr := getTestDatabaseURL()

	pg, err := pgstore.New(ctx, connStr)
	if err != nil {
		t.Skipf("postgres unavailable: %v", err)
	}
	defer pg.Close()

	// Clean tables
	_, _ = pg.Pool().Exec(ctx, "TRUNCATE jobs, job_events, schedules CASCADE;")

	now := time.Date(2026, 1, 1, 12, 5, 0, 0, time.UTC)
	lastFire := now.Add(-5 * time.Minute) // 5 minutes ago

	tests := []struct {
		name         string
		policy       scheduler.CatchupPolicy
		expectedJobs int
	}{
		{name: "CatchupNone", policy: scheduler.CatchupNone, expectedJobs: 0},
		{name: "CatchupLast", policy: scheduler.CatchupLast, expectedJobs: 1},
		{name: "CatchupAll", policy: scheduler.CatchupAll, expectedJobs: 5},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, _ = pg.Pool().Exec(ctx, "TRUNCATE jobs, job_events, schedules CASCADE;")

			schedID := uuid.New()
			const insertSched = `
				INSERT INTO schedules (id, queue, cron, payload, enabled, last_fire_at, created_at, updated_at)
				VALUES ($1, 'cron-q', '* * * * *', 'data', true, $2, now(), now())
			`
			_, err := pg.Pool().Exec(ctx, insertSched, schedID, lastFire)
			if err != nil {
				t.Fatalf("failed to insert schedule: %v", err)
			}

			s := scheduler.New(pg.Pool(), pg, tc.policy, 1*time.Second)
			err = s.ProcessSchedules(ctx, now)
			if err != nil {
				t.Fatalf("process schedules failed: %v", err)
			}

			var jobCount int
			err = pg.Pool().QueryRow(ctx, "SELECT count(*) FROM jobs WHERE queue = 'cron-q'").Scan(&jobCount)
			if err != nil {
				t.Fatalf("count jobs failed: %v", err)
			}

			if jobCount != tc.expectedJobs {
				t.Errorf("expected %d jobs, got %d", tc.expectedJobs, jobCount)
			}
		})
	}
}

func TestTwoLeadersDoubleFire(t *testing.T) {
	ctx := context.Background()
	connStr := getTestDatabaseURL()

	pg, err := pgstore.New(ctx, connStr)
	if err != nil {
		t.Skipf("postgres unavailable: %v", err)
	}
	defer pg.Close()

	_, _ = pg.Pool().Exec(ctx, "TRUNCATE jobs, job_events, schedules CASCADE;")

	schedID := uuid.New()
	lastFire := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	fireTime := lastFire.Add(1 * time.Minute)

	const insertSched = `
		INSERT INTO schedules (id, queue, cron, payload, enabled, last_fire_at, created_at, updated_at)
		VALUES ($1, 'double-fire-q', '* * * * *', 'payload', true, $2, now(), now())
	`
	_, err = pg.Pool().Exec(ctx, insertSched, schedID, lastFire)
	if err != nil {
		t.Fatalf("insert schedule failed: %v", err)
	}

	// Two separate schedulers simultaneously processing the same schedule
	s1 := scheduler.New(pg.Pool(), pg, scheduler.CatchupLast, 1*time.Second)
	s2 := scheduler.New(pg.Pool(), pg, scheduler.CatchupLast, 1*time.Second)

	err1 := s1.ProcessSchedules(ctx, fireTime)
	err2 := s2.ProcessSchedules(ctx, fireTime)
	if err1 != nil || err2 != nil {
		t.Fatalf("errors processing: err1=%v, err2=%v", err1, err2)
	}

	// Assert only ONE job exists despite two leaders firing
	var jobCount int
	err = pg.Pool().QueryRow(ctx, "SELECT count(*) FROM jobs WHERE queue = 'double-fire-q'").Scan(&jobCount)
	if err != nil {
		t.Fatalf("count jobs failed: %v", err)
	}
	if jobCount != 1 {
		t.Fatalf("expected exactly 1 job (idempotency prevented double-fire), got %d", jobCount)
	}
}

func TestLeaderStepDownOnConnectionTermination(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	connStr := getTestDatabaseURL()
	pg, err := pgstore.New(ctx, connStr)
	if err != nil {
		t.Skipf("postgres unavailable: %v", err)
	}
	defer pg.Close()

	// Ensure any stale advisory locks from previous tests are released
	_, _ = pg.Pool().Exec(ctx, `
		SELECT pg_terminate_backend(pid) 
		FROM pg_locks 
		WHERE locktype = 'advisory' AND objid = $1::oid AND pid <> pg_backend_pid();
	`, uint32(scheduler.SchedulerAdvisoryLockID))
	time.Sleep(100 * time.Millisecond)

	s1 := scheduler.New(pg.Pool(), pg, scheduler.CatchupLast, 100*time.Millisecond)
	s2 := scheduler.New(pg.Pool(), pg, scheduler.CatchupLast, 100*time.Millisecond)

	s1Ctx, s1Cancel := context.WithCancel(ctx)
	defer s1Cancel()
	go func() { _ = s1.Run(s1Ctx) }()

	// Wait for s1 to become leader
	for i := 0; i < 50; i++ {
		if s1.IsLeader() {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !s1.IsLeader() {
		t.Fatalf("s1 failed to become leader")
	}

	// Start s2 in background
	s2Ctx, s2Cancel := context.WithCancel(ctx)
	defer s2Cancel()
	go func() { _ = s2.Run(s2Ctx) }()

	time.Sleep(200 * time.Millisecond)
	if s2.IsLeader() {
		t.Fatalf("s2 should NOT be leader while s1 holds lock")
	}

	// Find the PID of s1's dedicated connection holding the advisory lock and kill it
	var pid int
	err = pg.Pool().QueryRow(ctx, `
		SELECT pid FROM pg_locks
		WHERE locktype = 'advisory' AND objid = $1::oid AND granted = true
		LIMIT 1
	`, uint32(scheduler.SchedulerAdvisoryLockID)).Scan(&pid)
	if err != nil {
		t.Fatalf("failed to find lock holder PID: %v", err)
	}

	t.Logf("Terminating backend PID %d...", pid)
	_, err = pg.Pool().Exec(ctx, "SELECT pg_terminate_backend($1)", pid)
	if err != nil {
		t.Fatalf("terminate backend failed: %v", err)
	}

	// Wait for s2 to take over leadership
	for i := 0; i < 60; i++ {
		if s2.IsLeader() {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if !s2.IsLeader() {
		t.Fatalf("s2 should have taken over leadership after s1's connection was killed")
	}
}
