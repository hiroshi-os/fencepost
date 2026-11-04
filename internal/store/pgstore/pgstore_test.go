package pgstore_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hiroshi-os/fencepost/internal/store"
	"github.com/hiroshi-os/fencepost/internal/store/pgstore"
)

func getTestDatabaseURL() string {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		url = "postgres://fencepost:fencepost_password@localhost:5432/fencepost?sslmode=disable"
	}
	return url
}

func setupTestStore(t *testing.T) (*pgstore.PGStore, context.Context) {
	ctx := context.Background()
	connStr := getTestDatabaseURL()

	s, err := pgstore.New(ctx, connStr)
	if err != nil {
		t.Fatalf("failed to connect to postgres: %v", err)
	}

	// Clean tables
	_, err = s.Pool().Exec(ctx, "TRUNCATE jobs, job_events, schedules CASCADE; UPDATE fencepost_test_clock SET test_now = NULL WHERE id = 1;")
	if err != nil {
		t.Fatalf("failed to clean test tables: %v", err)
	}

	t.Cleanup(func() {
		s.ResetTestClock(ctx)
		s.Close()
	})

	return s, ctx
}

func TestEnqueueAndIdempotency(t *testing.T) {
	s, ctx := setupTestStore(t)

	idemKey := "key-" + uuid.NewString()
	res1, err := s.Enqueue(ctx, store.EnqueueParams{
		Queue:          "default",
		Payload:        []byte("hello world"),
		Priority:       10,
		IdempotencyKey: &idemKey,
		MaxAttempts:    3,
	})
	if err != nil {
		t.Fatalf("enqueue 1 failed: %v", err)
	}
	if res1.Deduplicated {
		t.Errorf("expected res1 to not be deduplicated")
	}

	// Enqueue again with same key
	res2, err := s.Enqueue(ctx, store.EnqueueParams{
		Queue:          "default",
		Payload:        []byte("different payload"),
		Priority:       5,
		IdempotencyKey: &idemKey,
		MaxAttempts:    3,
	})
	if err != nil {
		t.Fatalf("enqueue 2 failed: %v", err)
	}
	if !res2.Deduplicated {
		t.Errorf("expected res2 to be deduplicated")
	}
	if res1.JobID != res2.JobID {
		t.Errorf("expected same job ID, got %s and %s", res1.JobID, res2.JobID)
	}

	// Verify events
	events, err := s.GetEvents(ctx, res1.JobID)
	if err != nil {
		t.Fatalf("failed to get events: %v", err)
	}
	if len(events) != 1 || events[0].Kind != "enqueued" {
		t.Errorf("expected 1 'enqueued' event, got %v", events)
	}
}

func TestLeaseAndFencingToken(t *testing.T) {
	s, ctx := setupTestStore(t)

	res, err := s.Enqueue(ctx, store.EnqueueParams{
		Queue:    "emails",
		Payload:  []byte("send email"),
		Priority: 10,
	})
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	// Lease job
	leased, err := s.Lease(ctx, "emails", "worker-1", 10, 2*time.Second)
	if err != nil {
		t.Fatalf("lease failed: %v", err)
	}
	if len(leased) != 1 {
		t.Fatalf("expected 1 leased job, got %d", len(leased))
	}
	job := leased[0]
	if job.ID != res.JobID {
		t.Fatalf("expected job ID %s, got %s", res.JobID, job.ID)
	}
	if job.FencingToken <= 0 {
		t.Fatalf("expected positive fencing token, got %d", job.FencingToken)
	}
	if job.Attempt != 1 {
		t.Fatalf("expected attempt 1, got %d", job.Attempt)
	}

	// Heartbeat with valid token
	newExpiry, err := s.Heartbeat(ctx, job.ID, job.FencingToken, 5*time.Second)
	if err != nil {
		t.Fatalf("heartbeat failed: %v", err)
	}
	if newExpiry.Before(job.LeaseExpiresAt) {
		t.Errorf("expected new expiry %v to be after %v", newExpiry, job.LeaseExpiresAt)
	}

	// Heartbeat with invalid token
	_, err = s.Heartbeat(ctx, job.ID, job.FencingToken+999, 5*time.Second)
	if err != store.ErrStaleToken {
		t.Fatalf("expected ErrStaleToken, got %v", err)
	}

	// Complete with invalid token
	err = s.Complete(ctx, job.ID, job.FencingToken+999, []byte("ok"))
	if err != store.ErrStaleToken {
		t.Fatalf("expected ErrStaleToken for Complete, got %v", err)
	}

	// Complete with valid token
	err = s.Complete(ctx, job.ID, job.FencingToken, []byte("result-ok"))
	if err != nil {
		t.Fatalf("complete failed: %v", err)
	}

	// Verify terminal state
	j, err := s.Get(ctx, job.ID)
	if err != nil {
		t.Fatalf("get job failed: %v", err)
	}
	if j.State != store.StateSucceeded {
		t.Errorf("expected state succeeded, got %s", j.State)
	}
	if string(j.Result) != "result-ok" {
		t.Errorf("expected result 'result-ok', got %s", string(j.Result))
	}

	// Try completing again -> stale token error
	err = s.Complete(ctx, job.ID, job.FencingToken, []byte("again"))
	if err != store.ErrStaleToken {
		t.Errorf("expected ErrStaleToken on second complete, got %v", err)
	}
}

func TestFailRetryAndExhaustion(t *testing.T) {
	s, ctx := setupTestStore(t)

	res, err := s.Enqueue(ctx, store.EnqueueParams{
		Queue:       "tasks",
		Payload:     []byte("flaky task"),
		MaxAttempts: 2,
	})
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	// Lease 1
	leased1, err := s.Lease(ctx, "tasks", "w1", 1, 10*time.Second)
	if err != nil || len(leased1) != 1 {
		t.Fatalf("lease 1 failed: %v", err)
	}
	token1 := leased1[0].FencingToken

	// Fail retryable
	err = s.Fail(ctx, res.JobID, token1, "network error", true)
	if err != nil {
		t.Fatalf("fail 1 failed: %v", err)
	}

	j1, _ := s.Get(ctx, res.JobID)
	if j1.State != store.StateQueued {
		t.Fatalf("expected state queued after retryable fail, got %s", j1.State)
	}

	// Force run_at to be now so we can lease immediately in test
	now := time.Now()
	_, _ = s.Pool().Exec(ctx, "UPDATE jobs SET run_at = fencepost_now() WHERE id = $1", res.JobID)

	// Lease 2
	leased2, err := s.Lease(ctx, "tasks", "w2", 1, 10*time.Second)
	if err != nil || len(leased2) != 1 {
		t.Fatalf("lease 2 failed: %v", err)
	}
	token2 := leased2[0].FencingToken
	if token2 <= token1 {
		t.Errorf("expected strictly increasing token: token2=%d, token1=%d", token2, token1)
	}
	if leased2[0].Attempt != 2 {
		t.Errorf("expected attempt 2, got %d", leased2[0].Attempt)
	}

	// Worker 1 wakes up late and tries to Complete with token1 -> must fail
	err = s.Complete(ctx, res.JobID, token1, []byte("w1 late complete"))
	if err != store.ErrStaleToken {
		t.Fatalf("expected ErrStaleToken for w1, got %v", err)
	}

	// Worker 2 fails -> attempts exhausted (max_attempts = 2)
	err = s.Fail(ctx, res.JobID, token2, "permanent crash", true)
	if err != nil {
		t.Fatalf("fail 2 failed: %v", err)
	}

	j2, _ := s.Get(ctx, res.JobID)
	if j2.State != store.StateDead {
		t.Fatalf("expected state dead after exhausting attempts, got %s", j2.State)
	}
	_ = now
}

func TestReaperExpiredLease(t *testing.T) {
	s, ctx := setupTestStore(t)

	res, err := s.Enqueue(ctx, store.EnqueueParams{
		Queue:       "reaper-q",
		Payload:     []byte("payload"),
		MaxAttempts: 2,
	})
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	// Lease with 1 second TTL
	leased, err := s.Lease(ctx, "reaper-q", "worker-crashed", 1, 1*time.Second)
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease failed: %v", err)
	}

	// Advance test clock by 5 seconds
	fakeNow := time.Now().Add(5 * time.Second)
	if err := s.SetTestClock(ctx, &fakeNow); err != nil {
		t.Fatalf("set test clock failed: %v", err)
	}

	// Reap
	reaped, err := s.Reap(ctx, 10)
	if err != nil {
		t.Fatalf("reap failed: %v", err)
	}
	if reaped != 1 {
		t.Fatalf("expected 1 job reaped, got %d", reaped)
	}

	// Job should be requeued
	j, _ := s.Get(ctx, res.JobID)
	if j.State != store.StateQueued {
		t.Fatalf("expected state queued, got %s", j.State)
	}

	// Set run_at to fakeNow so it can be leased again
	_, _ = s.Pool().Exec(ctx, "UPDATE jobs SET run_at = fencepost_now() WHERE id = $1", res.JobID)

	// Lease again (attempt 2)
	leased2, err := s.Lease(ctx, "reaper-q", "worker-new", 1, 1*time.Second)
	if err != nil || len(leased2) != 1 {
		t.Fatalf("lease 2 failed: %v", err)
	}

	// Advance clock again past TTL
	fakeNow2 := fakeNow.Add(5 * time.Second)
	_ = s.SetTestClock(ctx, &fakeNow2)

	// Reap again -> attempts exhausted -> dead
	reaped2, err := s.Reap(ctx, 10)
	if err != nil {
		t.Fatalf("reap 2 failed: %v", err)
	}
	if reaped2 != 1 {
		t.Fatalf("expected 1 job reaped, got %d", reaped2)
	}

	jDead, _ := s.Get(ctx, res.JobID)
	if jDead.State != store.StateDead {
		t.Fatalf("expected state dead, got %s", jDead.State)
	}
}
