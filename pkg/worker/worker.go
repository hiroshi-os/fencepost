// Package worker provides the official client SDK for fencepost.
//
// Distributed Safety Invariant:
// When a worker executes a job, it is granted a monotonic FencingToken.
// Any downstream side-effecting operation (such as database mutations, bank transfers,
// or webhook deliveries) MUST pass Job.FencingToken to the downstream system.
// The downstream system validates that the token is >= max_seen_token, rejecting
// stale execution attempts that have lost their lease during process pauses or GC halts.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	fencepostv1 "github.com/hiroshi-os/fencepost/proto/fencepost/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
)

type Job struct {
	ID           string
	Queue        string
	Payload      []byte
	Attempt      int32
	FencingToken int64
}

type Handler func(ctx context.Context, job Job) error

type Config struct {
	WorkerID string
	LeaseTTL time.Duration
	Wait     time.Duration
}

type Worker struct {
	client  fencepostv1.QueueClient
	queue   string
	cfg     Config
	handler Handler
}

func New(client fencepostv1.QueueClient, queue string, cfg Config, handler Handler) *Worker {
	if cfg.WorkerID == "" {
		cfg.WorkerID = "worker-" + uuid.NewString()[:8]
	}
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = 2 * time.Second
	}
	if cfg.Wait <= 0 {
		cfg.Wait = 3 * time.Second
	}
	return &Worker{
		client:  client,
		queue:   queue,
		cfg:     cfg,
		handler: handler,
	}
}

// Run executes the worker loop across the specified concurrency.
func Run(ctx context.Context, client fencepostv1.QueueClient, queue string, concurrency int, handler Handler) error {
	return RunWithConfig(ctx, client, queue, concurrency, Config{}, handler)
}

// RunWithConfig executes the worker loop with customized TTL and wait configurations.
func RunWithConfig(ctx context.Context, client fencepostv1.QueueClient, queue string, concurrency int, cfg Config, handler Handler) error {
	if concurrency <= 0 {
		concurrency = 1
	}

	w := New(client, queue, cfg, handler)
	var wg sync.WaitGroup

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		wID := fmt.Sprintf("%s-%d", w.cfg.WorkerID, i)
		go func(id string) {
			defer wg.Done()
			w.loop(ctx, id)
		}(wID)
	}

	wg.Wait()
	return nil
}

func (w *Worker) loop(ctx context.Context, workerID string) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		resp, err := w.client.Lease(ctx, &fencepostv1.LeaseRequest{
			Queue:    w.queue,
			WorkerId: workerID,
			MaxJobs:  1,
			LeaseTtl: durationpb.New(w.cfg.LeaseTTL),
			Wait:     durationpb.New(w.cfg.Wait),
		})
		if err != nil {
			if errors.Is(ctx.Err(), context.Canceled) {
				return
			}
			slog.Debug("lease request failed, backoff", "error", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if len(resp.Jobs) == 0 {
			continue
		}

		for _, leased := range resp.Jobs {
			j := Job{
				ID:           leased.Id,
				Queue:        w.queue,
				Payload:      leased.Payload,
				Attempt:      leased.Attempt,
				FencingToken: leased.FencingToken,
			}
			w.processJob(ctx, j)
		}
	}
}

func (w *Worker) processJob(parentCtx context.Context, j Job) {
	jobCtx, cancelJob := context.WithCancel(parentCtx)
	defer cancelJob()

	// Heartbeat runs at ttl / 3
	hbInterval := w.cfg.LeaseTTL / 3
	if hbInterval < 50*time.Millisecond {
		hbInterval = 50 * time.Millisecond
	}

	hbStop := make(chan struct{})
	var hbWg sync.WaitGroup
	hbWg.Add(1)

	go func() {
		defer hbWg.Done()
		ticker := time.NewTicker(hbInterval)
		defer ticker.Stop()

		for {
			select {
			case <-hbStop:
				return
			case <-jobCtx.Done():
				return
			case <-ticker.C:
				_, err := w.client.Heartbeat(jobCtx, &fencepostv1.HeartbeatRequest{
					JobId:        j.ID,
					FencingToken: j.FencingToken,
					Extend:       durationpb.New(w.cfg.LeaseTTL),
				})
				if err != nil {
					slog.Warn("heartbeat failed, cancelling job context", "job_id", j.ID, "token", j.FencingToken, "error", err)
					cancelJob()
					return
				}
			}
		}
	}()

	err := w.handler(jobCtx, j)

	close(hbStop)
	hbWg.Wait()

	// Handle completion or failure
	if err == nil {
		_, compErr := w.client.Complete(parentCtx, &fencepostv1.CompleteRequest{
			JobId:        j.ID,
			FencingToken: j.FencingToken,
			Result:       []byte("ok"),
		})
		if compErr != nil {
			slog.Warn("complete call rejected", "job_id", j.ID, "token", j.FencingToken, "error", compErr)
		}
	} else {
		_, failErr := w.client.Fail(parentCtx, &fencepostv1.FailRequest{
			JobId:        j.ID,
			FencingToken: j.FencingToken,
			Error:        err.Error(),
			Retryable:    true,
		})
		if failErr != nil {
			slog.Warn("fail call rejected", "job_id", j.ID, "token", j.FencingToken, "error", failErr)
		}
	}
}

var _ = grpc.ClientConn{}
