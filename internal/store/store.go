package store

import (
	"context"
	"errors"
	"math/rand"
	"time"

	"github.com/google/uuid"
)

var (
	ErrStaleToken    = errors.New("stale token or lease lost")
	ErrJobNotFound   = errors.New("job not found")
	ErrTerminalState = errors.New("job is in terminal state")
)

type JobState string

const (
	StateQueued    JobState = "queued"
	StateLeased    JobState = "leased"
	StateSucceeded JobState = "succeeded"
	StateDead      JobState = "dead"
	StateCancelled JobState = "cancelled"
)

type Job struct {
	ID             uuid.UUID
	Queue          string
	Payload        []byte
	Priority       int32
	RunAt          time.Time
	State          JobState
	Attempts       int32
	MaxAttempts    int32
	LeaseOwner     *string
	LeaseExpiresAt *time.Time
	FencingToken   *int64
	IdempotencyKey *string
	Result         []byte
	LastError      *string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

type LeasedJob struct {
	ID             uuid.UUID
	Queue          string
	Payload        []byte
	Attempt        int32
	FencingToken   int64
	LeaseExpiresAt time.Time
}

type JobEvent struct {
	ID           int64
	JobID        uuid.UUID
	Kind         string
	FencingToken *int64
	Worker       *string
	At           time.Time
}

type EnqueueParams struct {
	Queue          string
	Payload        []byte
	RunAt          *time.Time
	Priority       int32
	IdempotencyKey *string
	MaxAttempts    int32
}

type EnqueueResult struct {
	JobID        uuid.UUID
	Deduplicated bool
}

var BackoffFunc = ComputeBackoff

// ComputeBackoff calculates exponential backoff with full jitter, capped at maxCap.
func ComputeBackoff(attempt int32, base time.Duration, maxCap time.Duration) time.Duration {
	if attempt <= 0 {
		attempt = 1
	}
	if base <= 0 {
		base = 100 * time.Millisecond
	}
	if maxCap <= 0 {
		maxCap = 30 * time.Second
	}
	// exponential: base * 2^(attempt-1)
	factor := 1 << min(attempt-1, 15)
	maxDelay := base * time.Duration(factor)
	if maxDelay > maxCap {
		maxDelay = maxCap
	}
	if maxDelay <= 0 {
		return 0
	}
	return time.Duration(rand.Int63n(int64(maxDelay)))
}

type Store interface {
	Enqueue(ctx context.Context, params EnqueueParams) (EnqueueResult, error)
	Lease(ctx context.Context, queue string, workerID string, maxJobs int32, ttl time.Duration) ([]LeasedJob, error)
	Heartbeat(ctx context.Context, jobID uuid.UUID, fencingToken int64, extend time.Duration) (time.Time, error)
	Complete(ctx context.Context, jobID uuid.UUID, fencingToken int64, result []byte) error
	Fail(ctx context.Context, jobID uuid.UUID, fencingToken int64, errStr string, retryable bool) error
	Cancel(ctx context.Context, jobID uuid.UUID) error
	Get(ctx context.Context, jobID uuid.UUID) (*Job, error)
	GetEvents(ctx context.Context, jobID uuid.UUID) ([]JobEvent, error)
	Reap(ctx context.Context, batchSize int32) (int32, error)
	Close() error
}
