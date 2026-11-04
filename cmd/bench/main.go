package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/hiroshi-os/fencepost/internal/store"
	"github.com/hiroshi-os/fencepost/internal/store/pgstore"
	"github.com/jackc/pgx/v5"
)

type LatencyStats struct {
	P50  time.Duration
	P95  time.Duration
	P99  time.Duration
	Rate float64
}

func calcStats(latencies []time.Duration, totalTime time.Duration) LatencyStats {
	if len(latencies) == 0 {
		return LatencyStats{}
	}
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})
	p50 := latencies[len(latencies)*50/100]
	p95 := latencies[len(latencies)*95/100]
	p99 := latencies[len(latencies)*99/100]
	rate := float64(len(latencies)) / totalTime.Seconds()
	return LatencyStats{
		P50:  p50,
		P95:  p95,
		P99:  p99,
		Rate: rate,
	}
}

func parseList(s string) []int {
	parts := strings.Split(s, ",")
	var res []int
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if n, err := strconv.Atoi(p); err == nil && n > 0 {
			res = append(res, n)
		}
	}
	return res
}

func main() {
	var (
		mode       = flag.String("mode", "enqueue", "Benchmark mode: enqueue or e2e")
		clientsStr = flag.String("clients", "1,8,32", "Comma-separated list of concurrent enqueue clients")
		workersStr = flag.String("workers", "1,4,16,64", "Comma-separated list of concurrent worker counts for e2e")
		batchStr   = flag.String("lease-batch", "1,10,50", "Comma-separated lease batch sizes")
		duration   = flag.Duration("d", 10*time.Second, "Duration for throughput tests")
		jobsCount  = flag.Int("jobs", 10000, "Job count for e2e runs")
		dbURL      = flag.String("db", "postgres://fencepost:fencepost_password@localhost:5432/fencepost?sslmode=disable", "DB URL")
	)
	flag.Parse()

	ctx := context.Background()
	pg, err := pgstore.New(ctx, *dbURL)
	if err != nil {
		panic(err)
	}
	defer pg.Close()

	switch *mode {
	case "enqueue":
		runEnqueueBenchmarks(ctx, pg, parseList(*clientsStr), *duration)
	case "e2e":
		runE2EBenchmarks(ctx, pg, parseList(*workersStr), parseList(*batchStr), *jobsCount)
	default:
		fmt.Printf("Unknown mode %s. Use enqueue or e2e\n", *mode)
	}
}

func runEnqueueBenchmarks(ctx context.Context, pg *pgstore.PGStore, clientCounts []int, dur time.Duration) {
	fmt.Println("=== ENQUEUE THROUGHPUT BENCHMARK ===")
	fmt.Println("| Clients | Idempotency Key | Throughput (jobs/s) | Latency p50 | Latency p99 |")
	fmt.Println("|---|---|---|---|---|")

	for _, clients := range clientCounts {
		for _, useIdem := range []bool{false, true} {
			_, _ = pg.Pool().Exec(ctx, "TRUNCATE jobs, job_events, schedules CASCADE; ALTER SEQUENCE fencing_seq RESTART WITH 1;")

			var latenciesMutex sync.Mutex
			var allLatencies []time.Duration
			var wg sync.WaitGroup

			benchCtx, cancel := context.WithTimeout(ctx, dur)
			start := time.Now()

			for c := 0; c < clients; c++ {
				wg.Add(1)
				clientID := c
				go func(cID int) {
					defer wg.Done()
					var localLatencies []time.Duration
					seq := 0
					for {
						select {
						case <-benchCtx.Done():
							latenciesMutex.Lock()
							allLatencies = append(allLatencies, localLatencies...)
							latenciesMutex.Unlock()
							return
						default:
						}

						var idemKey *string
						if useIdem {
							k := fmt.Sprintf("c%d-%d", cID, seq)
							idemKey = &k
						}
						seq++

						opStart := time.Now()
						_, err := pg.Enqueue(ctx, store.EnqueueParams{
							Queue:          "bench-queue",
							Payload:        []byte("bench-payload-32b-data-blob"),
							Priority:       int32(rand.Intn(10)),
							IdempotencyKey: idemKey,
							MaxAttempts:    3,
						})
						opDur := time.Since(opStart)
						if err == nil {
							localLatencies = append(localLatencies, opDur)
						}
					}
				}(clientID)
			}

			wg.Wait()
			cancel()
			totalTime := time.Since(start)

			stats := calcStats(allLatencies, totalTime)
			idemLabel := "without key"
			if useIdem {
				idemLabel = "with key"
			}
			fmt.Printf("| %7d | %15s | %19.1f | %11v | %11v |\n",
				clients, idemLabel, stats.Rate, stats.P50.Round(time.Microsecond), stats.P99.Round(time.Microsecond))
		}
	}
}

func runE2EBenchmarks(ctx context.Context, pg *pgstore.PGStore, workerCounts []int, batchSizes []int, totalJobs int) {
	fmt.Println("\n=== END-TO-END THROUGHPUT & SKIP LOCKED CONTENTION ===")
	fmt.Println("| Workers | Batch Size | Throughput (jobs/s) | Lag p50 | Lag p99 | Total Time |")
	fmt.Println("|---|---|---|---|---|---|")

	for _, workers := range workerCounts {
		for _, batch := range batchSizes {
			_, _ = pg.Pool().Exec(ctx, "TRUNCATE jobs, job_events, schedules CASCADE; ALTER SEQUENCE fencing_seq RESTART WITH 1;")

			// Batch insert jobs
			now := time.Now()
			rows := make([][]any, totalJobs)
			for i := 0; i < totalJobs; i++ {
				rows[i] = []any{
					uuid.New(),
					"e2e-bench-queue",
					[]byte("bench-data"),
					int32(rand.Intn(10)),
					now,
					"queued",
					int32(0),
					int32(3),
					now,
					now,
				}
			}

			_, _ = pg.Pool().CopyFrom(
				ctx,
				pgx.Identifier{"jobs"},
				[]string{"id", "queue", "payload", "priority", "run_at", "state", "attempts", "max_attempts", "created_at", "updated_at"},
				pgx.CopyFromRows(rows),
			)

			var completed atomic.Int64
			var lagMu sync.Mutex
			var allLags []time.Duration
			var wg sync.WaitGroup

			start := time.Now()

			for w := 0; w < workers; w++ {
				wg.Add(1)
				workerID := fmt.Sprintf("bench-worker-%d", w)
				go func(wID string) {
					defer wg.Done()
					var localLags []time.Duration

					for {
						if completed.Load() >= int64(totalJobs) {
							lagMu.Lock()
							allLags = append(allLags, localLags...)
							lagMu.Unlock()
							return
						}

						leased, err := pg.Lease(ctx, "e2e-bench-queue", wID, int32(batch), 30*time.Second)
						if err != nil || len(leased) == 0 {
							if completed.Load() >= int64(totalJobs) {
								lagMu.Lock()
								allLags = append(allLags, localLags...)
								lagMu.Unlock()
								return
							}
							time.Sleep(1 * time.Millisecond)
							continue
						}

						leaseTime := time.Now()
						for _, j := range leased {
							localLags = append(localLags, leaseTime.Sub(now))
							_ = pg.Complete(ctx, j.ID, j.FencingToken, []byte("ok"))
							completed.Add(1)
						}
					}
				}(workerID)
			}

			wg.Wait()
			totalDur := time.Since(start)
			rate := float64(totalJobs) / totalDur.Seconds()

			stats := calcStats(allLags, totalDur)
			fmt.Printf("| %7d | %10d | %19.1f | %7v | %7v | %10v |\n",
				workers, batch, rate, stats.P50.Round(time.Millisecond), stats.P99.Round(time.Millisecond), totalDur.Round(time.Millisecond))
		}
	}
}
