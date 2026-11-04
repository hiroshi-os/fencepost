package reaper

import (
	"context"
	"log/slog"
	"time"

	"github.com/hiroshi-os/fencepost/internal/store"
)

type Reaper struct {
	store    store.Store
	interval time.Duration
	batch    int32
}

func New(st store.Store, interval time.Duration, batch int32) *Reaper {
	if interval <= 0 {
		interval = 1 * time.Second
	}
	if batch <= 0 {
		batch = 100
	}
	return &Reaper{
		store:    st,
		interval: interval,
		batch:    batch,
	}
}

func (r *Reaper) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			count, err := r.store.Reap(ctx, r.batch)
			if err != nil {
				slog.Error("reaper error", "error", err)
			} else if count > 0 {
				slog.Info("reaper recovered expired leases", "count", count)
			}
		}
	}
}
