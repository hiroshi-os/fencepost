package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/hiroshi-os/fencepost/pkg/worker"
	fencepostv1 "github.com/hiroshi-os/fencepost/proto/fencepost/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	var (
		serverAddr  = flag.String("server", "localhost:50051", "fencepostd gRPC address")
		sinkURL     = flag.String("sink", "http://localhost:8081", "Fenced sink base URL")
		queue       = flag.String("queue", "chaos-queue", "Queue name")
		workerID    = flag.String("id", "", "Worker process ID")
		concurrency = flag.Int("concurrency", 1, "Concurrency per process")
		leaseTTL    = flag.Duration("ttl", 2*time.Second, "Lease TTL")
	)
	flag.Parse()

	if *workerID == "" {
		*workerID = "demoworker-" + uuid.NewString()[:8]
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	conn, err := grpc.NewClient(*serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		slog.Error("failed to connect to fencepostd", "addr", *serverAddr, "error", err)
		os.Exit(1)
	}
	defer conn.Close()

	client := fencepostv1.NewQueueClient(conn)
	httpClient := &http.Client{Timeout: 5 * time.Second}

	handler := func(jobCtx context.Context, job worker.Job) error {
		// Simulate work: random 5ms to 50ms
		workDuration := time.Duration(5+rand.Intn(46)) * time.Millisecond
		time.Sleep(workDuration)

		// Downstream call to sink with fencing token
		// Use independent timeout context so that a paused worker that wakes up
		// attempts the sink write to test Kleppmann fencing check
		sinkCtx, sinkCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer sinkCancel()

		effectReq := map[string]any{
			"job_id": job.ID,
			"token":  job.FencingToken,
			"value":  fmt.Sprintf("effect-attempt-%d", job.Attempt),
		}
		bodyBytes, _ := json.Marshal(effectReq)

		req, err := http.NewRequestWithContext(sinkCtx, "POST", *sinkURL+"/effects", bytes.NewReader(bodyBytes))
		if err != nil {
			return fmt.Errorf("create sink request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("sink request failed: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusConflict {
			slog.Warn("sink rejected stale token", "job_id", job.ID, "token", job.FencingToken, "status", resp.StatusCode)
			return fmt.Errorf("sink rejected stale token (409)")
		}

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("sink returned unexpected status: %d", resp.StatusCode)
		}

		return nil
	}

	cfg := worker.Config{
		WorkerID: *workerID,
		LeaseTTL: *leaseTTL,
		Wait:     2 * time.Second,
	}

	slog.Info("starting demoworker", "id", *workerID, "queue", *queue, "ttl", *leaseTTL, "sink", *sinkURL)
	if err := worker.RunWithConfig(ctx, client, *queue, *concurrency, cfg, handler); err != nil && ctx.Err() == nil {
		slog.Error("worker exited with error", "error", err)
	}
}
