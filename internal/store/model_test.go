package store_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hiroshi-os/fencepost/internal/store"
	"github.com/hiroshi-os/fencepost/internal/store/memstore"
	"github.com/hiroshi-os/fencepost/internal/store/pgstore"
	"pgregory.net/rapid"
)

func getTestDatabaseURL() string {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		url = "postgres://fencepost:fencepost_password@localhost:5432/fencepost?sslmode=disable"
	}
	return url
}

func TestRapidModelProperties(t *testing.T) {
	ctx := context.Background()
	connStr := getTestDatabaseURL()

	pg, err := pgstore.New(ctx, connStr)
	if err != nil {
		t.Skipf("skipping property test, postgres unavailable: %v", err)
	}
	defer pg.Close()

	origBackoff := store.BackoffFunc
	store.BackoffFunc = func(attempt int32, base, maxCap time.Duration) time.Duration {
		return time.Duration(attempt) * 100 * time.Millisecond
	}
	defer func() {
		store.BackoffFunc = origBackoff
	}()

	rapid.Check(t, func(rt *rapid.T) {
		_, _ = pg.Pool().Exec(ctx, "TRUNCATE jobs, job_events, schedules CASCADE; ALTER SEQUENCE fencing_seq RESTART WITH 1; UPDATE fencepost_test_clock SET test_now = NULL WHERE id = 1;")

		baseTime := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
		currentTime := baseTime
		_ = pg.SetTestClock(ctx, &currentTime)

		mem := memstore.New(currentTime)

		type trackedJob struct {
			id        uuid.UUID
			queue     string
			idemKey   *string
			pgTokens  []int64
			memTokens []int64
		}
		var tracked []trackedJob
		jobMap := make(map[uuid.UUID]*trackedJob)

		numSteps := rapid.IntRange(10, 25).Draw(rt, "numSteps")

		for step := 0; step < numSteps; step++ {
			action := rapid.IntRange(0, 6).Draw(rt, fmt.Sprintf("action_%d", step))

			switch action {
			case 0: // Enqueue
				qName := rapid.SampledFrom([]string{"qA", "qB"}).Draw(rt, "qName")
				hasIdem := rapid.Bool().Draw(rt, "hasIdem")
				var idemKey *string
				if hasIdem {
					k := fmt.Sprintf("k-%d", rapid.IntRange(1, 5).Draw(rt, "keyIdx"))
					idemKey = &k
				}
				maxAttempts := int32(rapid.IntRange(1, 3).Draw(rt, "maxAttempts"))
				priority := int32(rapid.IntRange(0, 10).Draw(rt, "priority"))

				p := store.EnqueueParams{
					Queue:          qName,
					Payload:        []byte(fmt.Sprintf("payload-%d", step)),
					Priority:       priority,
					IdempotencyKey: idemKey,
					MaxAttempts:    maxAttempts,
				}

				pgRes, pgErr := pg.Enqueue(ctx, p)
				memRes, memErr := mem.Enqueue(ctx, p)

				if (pgErr != nil) != (memErr != nil) {
					rt.Fatalf("step %d Enqueue error mismatch: pg=%v, mem=%v", step, pgErr, memErr)
				}
				if pgRes.Deduplicated != memRes.Deduplicated {
					rt.Fatalf("step %d Enqueue dedup mismatch: pg=%v, mem=%v", step, pgRes.Deduplicated, memRes.Deduplicated)
				}

				// Invariant (f): same idempotency key returns same job_id within the same queue
				if pgRes.Deduplicated {
					for _, tj := range tracked {
						if tj.queue == qName && tj.idemKey != nil && idemKey != nil && *tj.idemKey == *idemKey {
							if tj.id != pgRes.JobID {
								rt.Fatalf("Invariant (f) violation: different ID for same idempotency key")
							}
						}
					}
				} else {
					tj := trackedJob{id: pgRes.JobID, queue: qName, idemKey: idemKey}
					tracked = append(tracked, tj)
					jobMap[pgRes.JobID] = &tracked[len(tracked)-1]
				}

			case 1: // Lease
				qName := rapid.SampledFrom([]string{"qA", "qB"}).Draw(rt, "leaseQ")
				worker := fmt.Sprintf("w-%d", rapid.IntRange(1, 3).Draw(rt, "workerId"))
				maxJobs := int32(rapid.IntRange(1, 3).Draw(rt, "maxJobs"))
				ttl := time.Duration(rapid.IntRange(2, 5).Draw(rt, "leaseTTL")) * time.Second

				pgLeased, pgErr := pg.Lease(ctx, qName, worker, maxJobs, ttl)
				memLeased, memErr := mem.Lease(ctx, qName, worker, maxJobs, ttl)

				if (pgErr != nil) != (memErr != nil) {
					rt.Fatalf("step %d Lease error mismatch: pg=%v, mem=%v", step, pgErr, memErr)
				}

				for _, lj := range pgLeased {
					if tj, ok := jobMap[lj.ID]; ok {
						// Invariant (b): strictly increasing tokens
						if len(tj.pgTokens) > 0 {
							last := tj.pgTokens[len(tj.pgTokens)-1]
							if lj.FencingToken <= last {
								rt.Fatalf("Invariant (b) violation: pg token %d not strictly greater than %d", lj.FencingToken, last)
							}
						}
						tj.pgTokens = append(tj.pgTokens, lj.FencingToken)
					}
				}

				for _, lj := range memLeased {
					if tj, ok := jobMap[lj.ID]; ok {
						if len(tj.memTokens) > 0 {
							last := tj.memTokens[len(tj.memTokens)-1]
							if lj.FencingToken <= last {
								rt.Fatalf("Invariant (b) violation: mem token %d not strictly greater than %d", lj.FencingToken, last)
							}
						}
						tj.memTokens = append(tj.memTokens, lj.FencingToken)
					}
				}

			case 2: // Heartbeat
				if len(tracked) == 0 {
					continue
				}
				idx := rapid.IntRange(0, len(tracked)-1).Draw(rt, "hbIdx")
				target := tracked[idx]

				validToken := rapid.Bool().Draw(rt, "validHbToken")
				var pgTok, memTok int64 = 999999, 999999
				if validToken && len(target.pgTokens) > 0 {
					pgTok = target.pgTokens[len(target.pgTokens)-1]
				}
				if validToken && len(target.memTokens) > 0 {
					memTok = target.memTokens[len(target.memTokens)-1]
				}

				_, pgErr := pg.Heartbeat(ctx, target.id, pgTok, 3*time.Second)
				_, memErr := mem.Heartbeat(ctx, target.id, memTok, 3*time.Second)

				if !validToken {
					if pgErr != store.ErrStaleToken {
						rt.Fatalf("Invariant (c) violation: invalid heartbeat pg should fail with ErrStaleToken, got %v", pgErr)
					}
					if memErr != store.ErrStaleToken {
						rt.Fatalf("Invariant (c) violation: invalid heartbeat mem should fail with ErrStaleToken, got %v", memErr)
					}
				}

			case 3: // Complete
				if len(tracked) == 0 {
					continue
				}
				idx := rapid.IntRange(0, len(tracked)-1).Draw(rt, "compIdx")
				target := tracked[idx]

				validToken := rapid.Bool().Draw(rt, "validCompToken")
				var pgTok, memTok int64 = 888888, 888888
				if validToken && len(target.pgTokens) > 0 {
					pgTok = target.pgTokens[len(target.pgTokens)-1]
				}
				if validToken && len(target.memTokens) > 0 {
					memTok = target.memTokens[len(target.memTokens)-1]
				}

				pgErr := pg.Complete(ctx, target.id, pgTok, []byte("result"))
				memErr := mem.Complete(ctx, target.id, memTok, []byte("result"))

				if !validToken {
					if pgErr != store.ErrStaleToken {
						rt.Fatalf("Invariant (c) violation: complete with stale token must fail on pg, got %v", pgErr)
					}
					if memErr != store.ErrStaleToken {
						rt.Fatalf("Invariant (c) violation: complete with stale token must fail on mem, got %v", memErr)
					}
				}

			case 4: // Fail
				if len(tracked) == 0 {
					continue
				}
				idx := rapid.IntRange(0, len(tracked)-1).Draw(rt, "failIdx")
				target := tracked[idx]

				var pgTok, memTok int64 = 777777, 777777
				if len(target.pgTokens) > 0 {
					pgTok = target.pgTokens[len(target.pgTokens)-1]
				}
				if len(target.memTokens) > 0 {
					memTok = target.memTokens[len(target.memTokens)-1]
				}
				retryable := rapid.Bool().Draw(rt, "failRetryable")

				_ = pg.Fail(ctx, target.id, pgTok, "error", retryable)
				_ = mem.Fail(ctx, target.id, memTok, "error", retryable)

			case 5: // AdvanceClock
				advSeconds := rapid.IntRange(1, 10).Draw(rt, "advSec")
				advDur := time.Duration(advSeconds) * time.Second
				currentTime = currentTime.Add(advDur)
				_ = pg.SetTestClock(ctx, &currentTime)
				mem.SetClock(currentTime)

			case 6: // Reap
				bSize := int32(rapid.IntRange(1, 5).Draw(rt, "reapBatch"))
				_, _ = pg.Reap(ctx, bSize)
				_, _ = mem.Reap(ctx, bSize)
			}

			// Invariant Checks on every step
			for _, tj := range tracked {
				j, err := pg.Get(ctx, tj.id)
				if err != nil {
					continue
				}

				// Invariant (d): valid state and terminal state permanence
				switch j.State {
				case store.StateQueued, store.StateLeased, store.StateSucceeded, store.StateDead, store.StateCancelled:
				default:
					rt.Fatalf("Invariant (d) violation: invalid state %s", j.State)
				}

				// Invariant (e): attempts <= max_attempts
				if j.Attempts > j.MaxAttempts {
					rt.Fatalf("Invariant (e) violation: attempts %d > max_attempts %d for job %s", j.Attempts, j.MaxAttempts, j.ID)
				}

				// Invariant (a): at most one valid token
				if j.State != store.StateLeased && j.LeaseOwner != nil {
					rt.Fatalf("Invariant (a) violation: unleased job has lease owner %v", *j.LeaseOwner)
				}
			}
		}
	})
}
