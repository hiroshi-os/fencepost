package memstore_test

import (
	"context"
	"testing"
	"time"

	"github.com/hiroshi-os/fencepost/internal/store"
	"github.com/hiroshi-os/fencepost/internal/store/memstore"
)

func TestMemStoreBasicLifecycle(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	s := memstore.New(now)

	idem := "key-1"
	r1, err := s.Enqueue(ctx, store.EnqueueParams{
		Queue:          "default",
		Payload:        []byte("job1"),
		IdempotencyKey: &idem,
	})
	if err != nil || r1.Deduplicated {
		t.Fatalf("first enqueue failed: %v", err)
	}

	r2, err := s.Enqueue(ctx, store.EnqueueParams{
		Queue:          "default",
		Payload:        []byte("job1-dup"),
		IdempotencyKey: &idem,
	})
	if err != nil || !r2.Deduplicated || r1.JobID != r2.JobID {
		t.Fatalf("second enqueue dedup failed")
	}

	leased, err := s.Lease(ctx, "default", "w1", 1, 10*time.Second)
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease failed: %v", err)
	}

	token := leased[0].FencingToken
	if err := s.Complete(ctx, r1.JobID, token, []byte("done")); err != nil {
		t.Fatalf("complete failed: %v", err)
	}

	j, err := s.Get(ctx, r1.JobID)
	if err != nil || j.State != store.StateSucceeded {
		t.Fatalf("unexpected state: %v", j)
	}
}
