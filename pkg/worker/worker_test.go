package worker_test

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hiroshi-os/fencepost/internal/server"
	"github.com/hiroshi-os/fencepost/internal/store"
	"github.com/hiroshi-os/fencepost/internal/store/memstore"
	"github.com/hiroshi-os/fencepost/pkg/worker"
	fencepostv1 "github.com/hiroshi-os/fencepost/proto/fencepost/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func setupTestWorkerServer(t *testing.T) (fencepostv1.QueueClient, *memstore.MemStore, func()) {
	mem := memstore.New(time.Now())
	srv := server.NewQueueServer(mem)

	lis := bufconn.Listen(1024 * 1024)
	s := grpc.NewServer()
	fencepostv1.RegisterQueueServer(s, srv)

	go func() { _ = s.Serve(lis) }()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial bufconn failed: %v", err)
	}

	client := fencepostv1.NewQueueClient(conn)
	cleanup := func() {
		_ = conn.Close()
		s.Stop()
		_ = lis.Close()
	}

	return client, mem, cleanup
}

func TestWorkerExecution(t *testing.T) {
	client, mem, cleanup := setupTestWorkerServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Enqueue 3 jobs
	for i := 0; i < 3; i++ {
		_, _ = mem.Enqueue(ctx, store.EnqueueParams{
			Queue:   "worker-test",
			Payload: []byte("job"),
		})
	}

	var processed atomic.Int32
	handler := func(jobCtx context.Context, job worker.Job) error {
		if job.FencingToken <= 0 {
			t.Errorf("expected positive fencing token")
		}
		processed.Add(1)
		if processed.Load() == 3 {
			cancel()
		}
		return nil
	}

	cfg := worker.Config{
		LeaseTTL: 1 * time.Second,
		Wait:     100 * time.Millisecond,
	}

	_ = worker.RunWithConfig(ctx, client, "worker-test", 2, cfg, handler)

	if processed.Load() != 3 {
		t.Fatalf("expected 3 processed jobs, got %d", processed.Load())
	}
}

func TestWorkerHeartbeatContextCancellation(t *testing.T) {
	client, mem, cleanup := setupTestWorkerServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	enq, _ := mem.Enqueue(ctx, store.EnqueueParams{
		Queue:   "hb-test",
		Payload: []byte("slow job"),
	})

	contextCancelled := make(chan struct{})

	handler := func(jobCtx context.Context, job worker.Job) error {
		// Advance memory store clock past expiry to simulate lease expiration
		time.Sleep(100 * time.Millisecond)
		mem.AdvanceClock(10 * time.Minute)

		// Wait for jobCtx to be cancelled by the failed heartbeat
		select {
		case <-jobCtx.Done():
			close(contextCancelled)
			return jobCtx.Err()
		case <-time.After(2 * time.Second):
			t.Errorf("job context was not cancelled upon heartbeat failure")
			return nil
		}
	}

	cfg := worker.Config{
		LeaseTTL: 300 * time.Millisecond,
		Wait:     100 * time.Millisecond,
	}

	go func() {
		_ = worker.RunWithConfig(ctx, client, "hb-test", 1, cfg, handler)
	}()

	select {
	case <-contextCancelled:
		// Succeeded in detecting context cancellation!
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for context cancellation")
	}

	_ = enq
}
