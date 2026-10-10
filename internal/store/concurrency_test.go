package store_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hiroshi-os/fencepost/internal/store/pgstore"
	"github.com/jackc/pgx/v5"
)

func TestConcurrencyLeasing(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping concurrency test in short mode")
	}

	ctx := context.Background()
	connStr := getTestDatabaseURL()

	pg, err := pgstore.New(ctx, connStr)
	if err != nil {
		t.Skipf("skipping test, postgres unavailable: %v", err)
	}
	defer pg.Close()

	// Clean tables
	_, err = pg.Pool().Exec(ctx, "TRUNCATE jobs, job_events, schedules CASCADE; ALTER SEQUENCE fencing_seq RESTART WITH 1; UPDATE fencepost_test_clock SET test_now = NULL WHERE id = 1;")
	if err != nil {
		t.Fatalf("failed to clean tables: %v", err)
	}

	const totalJobs = 10000
	const queueName = "concurrency-queue"

	t.Logf("Enqueuing %d jobs using CopyFrom...", totalJobs)
	now := time.Now()
	rows := make([][]any, totalJobs)
	for i := 0; i < totalJobs; i++ {
		rows[i] = []any{
			uuid.New(),
			queueName,
			[]byte(fmt.Sprintf("job-%d", i)),
			int32(0),
			now,
			"queued",
			int32(0),
			int32(3),
			now,
			now,
		}
	}

	copyCount, err := pg.Pool().CopyFrom(
		ctx,
		pgx.Identifier{"jobs"},
		[]string{"id", "queue", "payload", "priority", "run_at", "state", "attempts", "max_attempts", "created_at", "updated_at"},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		t.Fatalf("failed to copy jobs: %v", err)
	}
	t.Logf("Enqueued %d jobs successfully via CopyFrom", copyCount)

	const numWorkers = 64
	var completedCount atomic.Int64
	var mu sync.Mutex
	leasedTokens := make(map[uuid.UUID]map[int64]bool)

	var wg sync.WaitGroup
	startSignal := make(chan struct{})

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		workerID := fmt.Sprintf("worker-%02d", w)
		go func(wID string) {
			defer wg.Done()
			<-startSignal

			for {
				if completedCount.Load() >= totalJobs {
					return
				}

				leased, err := pg.Lease(ctx, queueName, wID, 10, 30*time.Second)
				if err != nil {
					t.Errorf("worker %s lease error: %v", wID, err)
					return
				}
				if len(leased) == 0 {
					if completedCount.Load() >= totalJobs {
						return
					}
					time.Sleep(5 * time.Millisecond)
					continue
				}

				for _, job := range leased {
					mu.Lock()
					tokenSet, exists := leasedTokens[job.ID]
					if !exists {
						tokenSet = make(map[int64]bool)
						leasedTokens[job.ID] = tokenSet
					}
					if tokenSet[job.FencingToken] {
						mu.Unlock()
						t.Errorf("JOB %s LEASED MULTIPLE TIMES WITH SAME TOKEN %d", job.ID, job.FencingToken)
						return
					}
					tokenSet[job.FencingToken] = true
					mu.Unlock()

					err := pg.Complete(ctx, job.ID, job.FencingToken, []byte("ok"))
					if err != nil {
						t.Errorf("complete failed for job %s: %v", job.ID, err)
						return
					}
					completedCount.Add(1)
				}
			}
		}(workerID)
	}

	startTime := time.Now()
	close(startSignal)
	wg.Wait()
	duration := time.Since(startTime)

	t.Logf("Completed %d jobs across %d workers in %v (%.1f jobs/s)", completedCount.Load(), numWorkers, duration, float64(completedCount.Load())/duration.Seconds())

	if completedCount.Load() != totalJobs {
		t.Fatalf("expected %d completions, got %d", totalJobs, completedCount.Load())
	}

	// Verify all jobs in DB are in 'succeeded' state
	var succeededCount int64
	err = pg.Pool().QueryRow(ctx, "SELECT count(*) FROM jobs WHERE queue = $1 AND state = 'succeeded'", queueName).Scan(&succeededCount)
	if err != nil {
		t.Fatalf("failed to count succeeded jobs: %v", err)
	}
	if succeededCount != totalJobs {
		t.Fatalf("expected %d succeeded jobs in DB, got %d", totalJobs, succeededCount)
	}
}
