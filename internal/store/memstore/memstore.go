package memstore

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/hiroshi-os/fencepost/internal/store"
)

type MemStore struct {
	mu          sync.Mutex
	clock       time.Time
	fencingSeq  int64
	jobs        map[uuid.UUID]*store.Job
	idempotency map[string]uuid.UUID // queue:key -> jobID
	events      map[uuid.UUID][]store.JobEvent
	nextEventID int64
}

func New(initialTime time.Time) *MemStore {
	if initialTime.IsZero() {
		initialTime = time.Now()
	}
	return &MemStore{
		clock:       initialTime,
		fencingSeq:  0,
		jobs:        make(map[uuid.UUID]*store.Job),
		idempotency: make(map[string]uuid.UUID),
		events:      make(map[uuid.UUID][]store.JobEvent),
		nextEventID: 1,
	}
}

func (m *MemStore) Now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.clock
}

func (m *MemStore) SetClock(t time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.clock = t
}

func (m *MemStore) AdvanceClock(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.clock = m.clock.Add(d)
}

func (m *MemStore) Close() error {
	return nil
}

func (m *MemStore) Enqueue(_ context.Context, p store.EnqueueParams) (store.EnqueueResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.clock

	if p.MaxAttempts <= 0 {
		p.MaxAttempts = 3
	}

	if p.IdempotencyKey != nil && *p.IdempotencyKey != "" {
		lookupKey := p.Queue + ":" + *p.IdempotencyKey
		if existingID, ok := m.idempotency[lookupKey]; ok {
			return store.EnqueueResult{JobID: existingID, Deduplicated: true}, nil
		}
	}

	runAt := now
	if p.RunAt != nil {
		runAt = *p.RunAt
	}

	jobID := uuid.New()
	j := &store.Job{
		ID:             jobID,
		Queue:          p.Queue,
		Payload:        p.Payload,
		Priority:       p.Priority,
		RunAt:          runAt,
		State:          store.StateQueued,
		Attempts:       0,
		MaxAttempts:    p.MaxAttempts,
		IdempotencyKey: p.IdempotencyKey,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	m.jobs[jobID] = j

	if p.IdempotencyKey != nil && *p.IdempotencyKey != "" {
		m.idempotency[p.Queue+":"+*p.IdempotencyKey] = jobID
	}

	m.addEventLocked(jobID, "enqueued", nil, nil, now)

	return store.EnqueueResult{JobID: jobID, Deduplicated: false}, nil
}

func (m *MemStore) Lease(_ context.Context, queue string, workerID string, maxJobs int32, ttl time.Duration) ([]store.LeasedJob, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.clock
	if maxJobs <= 0 {
		maxJobs = 1
	}
	if ttl <= 0 {
		ttl = 30 * time.Second
	}

	// Find candidates: state == queued, run_at <= now, matching queue
	type candidate struct {
		job *store.Job
	}
	var candidates []candidate
	for _, j := range m.jobs {
		if j.Queue == queue && j.State == store.StateQueued && !j.RunAt.After(now) {
			candidates = append(candidates, candidate{job: j})
		}
	}

	// Sort candidates by priority desc, run_at asc
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].job.Priority != candidates[j].job.Priority {
			return candidates[i].job.Priority > candidates[j].job.Priority
		}
		return candidates[i].job.RunAt.Before(candidates[j].job.RunAt)
	})

	limit := int(maxJobs)
	if limit > len(candidates) {
		limit = len(candidates)
	}

	var leased []store.LeasedJob
	for i := 0; i < limit; i++ {
		j := candidates[i].job
		m.fencingSeq++
		token := m.fencingSeq
		expiresAt := now.Add(ttl)

		j.State = store.StateLeased
		j.LeaseOwner = &workerID
		j.LeaseExpiresAt = &expiresAt
		j.FencingToken = &token
		j.Attempts++
		j.UpdatedAt = now

		leased = append(leased, store.LeasedJob{
			ID:             j.ID,
			Queue:          j.Queue,
			Payload:        j.Payload,
			Attempt:        j.Attempts,
			FencingToken:   token,
			LeaseExpiresAt: expiresAt,
		})

		m.addEventLocked(j.ID, "leased", &token, &workerID, now)
	}

	return leased, nil
}

func (m *MemStore) Heartbeat(_ context.Context, jobID uuid.UUID, fencingToken int64, extend time.Duration) (time.Time, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.clock
	if extend <= 0 {
		extend = 30 * time.Second
	}

	j, ok := m.jobs[jobID]
	if !ok || j.State != store.StateLeased || j.FencingToken == nil || *j.FencingToken != fencingToken || (j.LeaseExpiresAt != nil && j.LeaseExpiresAt.Before(now)) {
		return time.Time{}, store.ErrStaleToken
	}

	newExpiry := now.Add(extend)
	j.LeaseExpiresAt = &newExpiry
	j.UpdatedAt = now

	m.addEventLocked(j.ID, "heartbeat", &fencingToken, j.LeaseOwner, now)

	return newExpiry, nil
}

func (m *MemStore) Complete(_ context.Context, jobID uuid.UUID, fencingToken int64, result []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.clock
	j, ok := m.jobs[jobID]
	if !ok || j.State != store.StateLeased || j.FencingToken == nil || *j.FencingToken != fencingToken {
		return store.ErrStaleToken
	}

	j.State = store.StateSucceeded
	j.Result = result
	j.LeaseOwner = nil
	j.UpdatedAt = now

	m.addEventLocked(j.ID, "completed", &fencingToken, nil, now)
	return nil
}

func (m *MemStore) Fail(_ context.Context, jobID uuid.UUID, fencingToken int64, errStr string, retryable bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.clock
	j, ok := m.jobs[jobID]
	if !ok || j.State != store.StateLeased || j.FencingToken == nil || *j.FencingToken != fencingToken {
		return store.ErrStaleToken
	}

	owner := j.LeaseOwner
	j.LastError = &errStr
	j.LeaseOwner = nil
	j.UpdatedAt = now

	if retryable && j.Attempts < j.MaxAttempts {
		delay := store.BackoffFunc(j.Attempts, 100*time.Millisecond, 30*time.Second)
		j.State = store.StateQueued
		j.RunAt = now.Add(delay)
		m.addEventLocked(j.ID, "failed_queued", &fencingToken, owner, now)
	} else {
		j.State = store.StateDead
		m.addEventLocked(j.ID, "failed_dead", &fencingToken, owner, now)
	}

	return nil
}

func (m *MemStore) Cancel(_ context.Context, jobID uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.clock
	j, ok := m.jobs[jobID]
	if !ok {
		return nil
	}
	if j.State != store.StateQueued && j.State != store.StateLeased {
		return nil
	}

	token := j.FencingToken
	j.State = store.StateCancelled
	j.LeaseOwner = nil
	j.UpdatedAt = now

	m.addEventLocked(j.ID, "cancelled", token, nil, now)
	return nil
}

func (m *MemStore) Get(_ context.Context, jobID uuid.UUID) (*store.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	j, ok := m.jobs[jobID]
	if !ok {
		return nil, store.ErrJobNotFound
	}
	copied := *j
	return &copied, nil
}

func (m *MemStore) GetEvents(_ context.Context, jobID uuid.UUID) ([]store.JobEvent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	evs, ok := m.events[jobID]
	if !ok {
		return nil, nil
	}
	copied := make([]store.JobEvent, len(evs))
	copy(copied, evs)
	return copied, nil
}

func (m *MemStore) Reap(_ context.Context, batchSize int32) (int32, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.clock
	if batchSize <= 0 {
		batchSize = 100
	}

	type expiredJob struct {
		job *store.Job
	}
	var expired []expiredJob
	for _, j := range m.jobs {
		if j.State == store.StateLeased && j.LeaseExpiresAt != nil && j.LeaseExpiresAt.Before(now) {
			expired = append(expired, expiredJob{job: j})
		}
	}

	sort.Slice(expired, func(i, j int) bool {
		return expired[i].job.LeaseExpiresAt.Before(*expired[j].job.LeaseExpiresAt)
	})

	limit := int(batchSize)
	if limit > len(expired) {
		limit = len(expired)
	}

	for i := 0; i < limit; i++ {
		j := expired[i].job
		owner := j.LeaseOwner
		token := j.FencingToken
		j.LeaseOwner = nil
		j.UpdatedAt = now

		if j.Attempts >= j.MaxAttempts {
			j.State = store.StateDead
			lastErr := "lease expired and attempts exhausted"
			j.LastError = &lastErr
			m.addEventLocked(j.ID, "reaped_dead", token, owner, now)
		} else {
			delay := store.BackoffFunc(j.Attempts, 100*time.Millisecond, 30*time.Second)
			j.State = store.StateQueued
			j.RunAt = now.Add(delay)
			m.addEventLocked(j.ID, "reaped_queued", token, owner, now)
		}
	}

	return int32(limit), nil
}

func (m *MemStore) addEventLocked(jobID uuid.UUID, kind string, token *int64, worker *string, at time.Time) {
	ev := store.JobEvent{
		ID:           m.nextEventID,
		JobID:        jobID,
		Kind:         kind,
		FencingToken: token,
		Worker:       worker,
		At:           at,
	}
	m.nextEventID++
	m.events[jobID] = append(m.events[jobID], ev)
}
