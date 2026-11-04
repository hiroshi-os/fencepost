package pgstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/hiroshi-os/fencepost/internal/metrics"
	"github.com/hiroshi-os/fencepost/internal/store"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PGStore struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context, connStr string) (*PGStore, error) {
	cfg, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("parse conn string: %w", err)
	}
	cfg.MaxConns = 50
	cfg.MinConns = 5

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pgx pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	return &PGStore{pool: pool}, nil
}

func (s *PGStore) Pool() *pgxpool.Pool {
	return s.pool
}

func (s *PGStore) Close() error {
	s.pool.Close()
	return nil
}

func (s *PGStore) Enqueue(ctx context.Context, p store.EnqueueParams) (store.EnqueueResult, error) {
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = 3
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return store.EnqueueResult{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	var runAtVal any = nil
	if p.RunAt != nil {
		runAtVal = *p.RunAt
	}

	const insertSQL = `
		INSERT INTO jobs (
			queue, payload, priority, run_at, state, attempts, max_attempts, idempotency_key, created_at, updated_at
		) VALUES (
			$1, $2, $3, COALESCE($4, fencepost_now()), 'queued', 0, $5, $6, fencepost_now(), fencepost_now()
		)
		ON CONFLICT (queue, idempotency_key) DO NOTHING
		RETURNING id;
	`

	var jobID uuid.UUID
	err = tx.QueryRow(ctx, insertSQL, p.Queue, p.Payload, p.Priority, runAtVal, p.MaxAttempts, p.IdempotencyKey).Scan(&jobID)
	if err == nil {
		// New job inserted
		const eventSQL = `
			INSERT INTO job_events (job_id, kind, fencing_token, worker, at)
			VALUES ($1, 'enqueued', NULL, NULL, fencepost_now());
		`
		if _, err := tx.Exec(ctx, eventSQL, jobID); err != nil {
			return store.EnqueueResult{}, fmt.Errorf("insert event: %w", err)
		}

		// Notify queue listeners
		_, _ = tx.Exec(ctx, "SELECT pg_notify('fencepost_enqueue', $1)", p.Queue)

		if err := tx.Commit(ctx); err != nil {
			return store.EnqueueResult{}, fmt.Errorf("commit tx: %w", err)
		}

		metrics.EnqueueTotal.WithLabelValues("false").Inc()
		return store.EnqueueResult{JobID: jobID, Deduplicated: false}, nil
	}

	if !errors.Is(err, pgx.ErrNoRows) {
		return store.EnqueueResult{}, fmt.Errorf("insert job: %w", err)
	}

	// Conflict occurred on (queue, idempotency_key)
	const selectSQL = `
		SELECT id FROM jobs WHERE queue = $1 AND idempotency_key = $2;
	`
	err = tx.QueryRow(ctx, selectSQL, p.Queue, p.IdempotencyKey).Scan(&jobID)
	if err != nil {
		return store.EnqueueResult{}, fmt.Errorf("select existing job on conflict: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return store.EnqueueResult{}, fmt.Errorf("commit tx: %w", err)
	}

	metrics.EnqueueTotal.WithLabelValues("true").Inc()
	return store.EnqueueResult{JobID: jobID, Deduplicated: true}, nil
}

func (s *PGStore) Lease(ctx context.Context, queue string, workerID string, maxJobs int32, ttl time.Duration) ([]store.LeasedJob, error) {
	start := time.Now()
	defer func() {
		metrics.LeaseLatencySeconds.Observe(time.Since(start).Seconds())
	}()

	if maxJobs <= 0 {
		maxJobs = 1
	}
	if ttl <= 0 {
		ttl = 30 * time.Second
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	const leaseSQL = `
		UPDATE jobs
		SET state = 'leased',
		    lease_owner = $1,
		    lease_expires_at = fencepost_now() + ($2 * interval '1 millisecond'),
		    fencing_token = nextval('fencing_seq'),
		    attempts = attempts + 1,
		    updated_at = fencepost_now()
		WHERE id IN (
			SELECT id FROM jobs
			WHERE queue = $3 AND state = 'queued' AND run_at <= fencepost_now()
			ORDER BY priority DESC, run_at ASC
			LIMIT $4
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id, queue, payload, attempts, fencing_token, lease_expires_at, run_at;
	`

	ttlMs := ttl.Milliseconds()
	rows, err := tx.Query(ctx, leaseSQL, workerID, ttlMs, queue, maxJobs)
	if err != nil {
		return nil, fmt.Errorf("lease query: %w", err)
	}
	defer rows.Close()

	var leased []store.LeasedJob
	var eventArgs [][]any

	for rows.Next() {
		var (
			j        store.LeasedJob
			runAt    time.Time
			tokenVal int64
		)
		if err := rows.Scan(&j.ID, &j.Queue, &j.Payload, &j.Attempt, &tokenVal, &j.LeaseExpiresAt, &runAt); err != nil {
			return nil, fmt.Errorf("scan leased job: %w", err)
		}
		j.FencingToken = tokenVal
		leased = append(leased, j)

		lag := time.Since(runAt).Seconds()
		if lag > 0 {
			metrics.QueueLagSeconds.Observe(lag)
		}

		eventArgs = append(eventArgs, []any{j.ID, "leased", tokenVal, workerID})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows err: %w", err)
	}
	rows.Close()

	for _, arg := range eventArgs {
		const insertEventSQL = `
			INSERT INTO job_events (job_id, kind, fencing_token, worker, at)
			VALUES ($1, $2, $3, $4, fencepost_now());
		`
		if _, err := tx.Exec(ctx, insertEventSQL, arg...); err != nil {
			return nil, fmt.Errorf("insert lease event: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit tx: %w", err)
	}

	return leased, nil
}

func (s *PGStore) Heartbeat(ctx context.Context, jobID uuid.UUID, fencingToken int64, extend time.Duration) (time.Time, error) {
	if extend <= 0 {
		extend = 30 * time.Second
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return time.Time{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	const heartbeatSQL = `
		UPDATE jobs
		SET lease_expires_at = fencepost_now() + ($1 * interval '1 millisecond'),
		    updated_at = fencepost_now()
		WHERE id = $2 AND fencing_token = $3 AND state = 'leased' AND lease_expires_at >= fencepost_now()
		RETURNING lease_expires_at, lease_owner;
	`

	extendMs := extend.Milliseconds()
	var newExpires time.Time
	var owner *string
	err = tx.QueryRow(ctx, heartbeatSQL, extendMs, jobID, fencingToken).Scan(&newExpires, &owner)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			metrics.StaleTokenRejectionsTotal.Inc()
			return time.Time{}, store.ErrStaleToken
		}
		return time.Time{}, fmt.Errorf("heartbeat update: %w", err)
	}

	const eventSQL = `
		INSERT INTO job_events (job_id, kind, fencing_token, worker, at)
		VALUES ($1, 'heartbeat', $2, $3, fencepost_now());
	`
	if _, err := tx.Exec(ctx, eventSQL, jobID, fencingToken, owner); err != nil {
		return time.Time{}, fmt.Errorf("insert heartbeat event: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return time.Time{}, fmt.Errorf("commit tx: %w", err)
	}

	return newExpires, nil
}

func (s *PGStore) Complete(ctx context.Context, jobID uuid.UUID, fencingToken int64, result []byte) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	const completeSQL = `
		UPDATE jobs
		SET state = 'succeeded',
		    result = $1,
		    lease_owner = NULL,
		    updated_at = fencepost_now()
		WHERE id = $2 AND fencing_token = $3 AND state = 'leased'
		RETURNING attempts;
	`

	var attempts int32
	err = tx.QueryRow(ctx, completeSQL, result, jobID, fencingToken).Scan(&attempts)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			metrics.StaleTokenRejectionsTotal.Inc()
			return store.ErrStaleToken
		}
		return fmt.Errorf("complete update: %w", err)
	}

	const eventSQL = `
		INSERT INTO job_events (job_id, kind, fencing_token, worker, at)
		VALUES ($1, 'completed', $2, NULL, fencepost_now());
	`
	if _, err := tx.Exec(ctx, eventSQL, jobID, fencingToken); err != nil {
		return fmt.Errorf("insert complete event: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}

	metrics.JobAttempts.Observe(float64(attempts))
	return nil
}

func (s *PGStore) Fail(ctx context.Context, jobID uuid.UUID, fencingToken int64, errStr string, retryable bool) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	const checkSQL = `
		SELECT attempts, max_attempts, lease_owner
		FROM jobs
		WHERE id = $1 AND fencing_token = $2 AND state = 'leased'
		FOR UPDATE;
	`
	var (
		attempts    int32
		maxAttempts int32
		owner       *string
	)
	err = tx.QueryRow(ctx, checkSQL, jobID, fencingToken).Scan(&attempts, &maxAttempts, &owner)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			metrics.StaleTokenRejectionsTotal.Inc()
			return store.ErrStaleToken
		}
		return fmt.Errorf("fail check: %w", err)
	}

	if retryable && attempts < maxAttempts {
		delay := store.BackoffFunc(attempts, 100*time.Millisecond, 30*time.Second)
		delayMs := delay.Milliseconds()

		const updateRetrySQL = `
			UPDATE jobs
			SET state = 'queued',
			    run_at = fencepost_now() + ($1 * interval '1 millisecond'),
			    last_error = $2,
			    lease_owner = NULL,
			    updated_at = fencepost_now()
			WHERE id = $3;
		`
		if _, err := tx.Exec(ctx, updateRetrySQL, delayMs, errStr, jobID); err != nil {
			return fmt.Errorf("update retry: %w", err)
		}

		const eventSQL = `
			INSERT INTO job_events (job_id, kind, fencing_token, worker, at)
			VALUES ($1, 'failed_queued', $2, $3, fencepost_now());
		`
		if _, err := tx.Exec(ctx, eventSQL, jobID, fencingToken, owner); err != nil {
			return fmt.Errorf("insert fail event: %w", err)
		}
	} else {
		const updateDeadSQL = `
			UPDATE jobs
			SET state = 'dead',
			    last_error = $1,
			    lease_owner = NULL,
			    updated_at = fencepost_now()
			WHERE id = $2;
		`
		if _, err := tx.Exec(ctx, updateDeadSQL, errStr, jobID); err != nil {
			return fmt.Errorf("update dead: %w", err)
		}

		const eventSQL = `
			INSERT INTO job_events (job_id, kind, fencing_token, worker, at)
			VALUES ($1, 'failed_dead', $2, $3, fencepost_now());
		`
		if _, err := tx.Exec(ctx, eventSQL, jobID, fencingToken, owner); err != nil {
			return fmt.Errorf("insert dead event: %w", err)
		}

		metrics.JobAttempts.Observe(float64(attempts))
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}

	return nil
}

func (s *PGStore) Cancel(ctx context.Context, jobID uuid.UUID) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	const cancelSQL = `
		UPDATE jobs
		SET state = 'cancelled',
		    lease_owner = NULL,
		    updated_at = fencepost_now()
		WHERE id = $1 AND state IN ('queued', 'leased')
		RETURNING fencing_token;
	`
	var token *int64
	err = tx.QueryRow(ctx, cancelSQL, jobID).Scan(&token)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return fmt.Errorf("cancel update: %w", err)
	}

	const eventSQL = `
		INSERT INTO job_events (job_id, kind, fencing_token, worker, at)
		VALUES ($1, 'cancelled', $2, NULL, fencepost_now());
	`
	if _, err := tx.Exec(ctx, eventSQL, jobID, token); err != nil {
		return fmt.Errorf("insert cancel event: %w", err)
	}

	return tx.Commit(ctx)
}

func (s *PGStore) Get(ctx context.Context, jobID uuid.UUID) (*store.Job, error) {
	const getSQL = `
		SELECT id, queue, payload, priority, run_at, state, attempts, max_attempts,
		       lease_owner, lease_expires_at, fencing_token, idempotency_key, result,
		       last_error, created_at, updated_at
		FROM jobs
		WHERE id = $1;
	`
	var j store.Job
	var stateStr string
	err := s.pool.QueryRow(ctx, getSQL, jobID).Scan(
		&j.ID, &j.Queue, &j.Payload, &j.Priority, &j.RunAt, &stateStr, &j.Attempts, &j.MaxAttempts,
		&j.LeaseOwner, &j.LeaseExpiresAt, &j.FencingToken, &j.IdempotencyKey, &j.Result,
		&j.LastError, &j.CreatedAt, &j.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, store.ErrJobNotFound
		}
		return nil, fmt.Errorf("get job: %w", err)
	}
	j.State = store.JobState(stateStr)
	return &j, nil
}

func (s *PGStore) GetEvents(ctx context.Context, jobID uuid.UUID) ([]store.JobEvent, error) {
	const eventsSQL = `
		SELECT id, job_id, kind, fencing_token, worker, at
		FROM job_events
		WHERE job_id = $1
		ORDER BY at ASC, id ASC;
	`
	rows, err := s.pool.Query(ctx, eventsSQL, jobID)
	if err != nil {
		return nil, fmt.Errorf("query events: %w", err)
	}
	defer rows.Close()

	var events []store.JobEvent
	for rows.Next() {
		var e store.JobEvent
		if err := rows.Scan(&e.ID, &e.JobID, &e.Kind, &e.FencingToken, &e.Worker, &e.At); err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		events = append(events, e)
	}
	return events, rows.Err()
}

func (s *PGStore) Reap(ctx context.Context, batchSize int32) (int32, error) {
	if batchSize <= 0 {
		batchSize = 100
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	const findExpiredSQL = `
		SELECT id, attempts, max_attempts, fencing_token, lease_owner
		FROM jobs
		WHERE state = 'leased' AND lease_expires_at < fencepost_now()
		ORDER BY lease_expires_at ASC
		LIMIT $1
		FOR UPDATE SKIP LOCKED;
	`

	rows, err := tx.Query(ctx, findExpiredSQL, batchSize)
	if err != nil {
		return 0, fmt.Errorf("find expired leases: %w", err)
	}

	type expiredJob struct {
		id          uuid.UUID
		attempts    int32
		maxAttempts int32
		token       *int64
		owner       *string
	}
	var expired []expiredJob
	for rows.Next() {
		var ej expiredJob
		if err := rows.Scan(&ej.id, &ej.attempts, &ej.maxAttempts, &ej.token, &ej.owner); err != nil {
			rows.Close()
			return 0, fmt.Errorf("scan expired: %w", err)
		}
		expired = append(expired, ej)
	}
	rows.Close()

	if len(expired) == 0 {
		return 0, tx.Commit(ctx)
	}

	for _, ej := range expired {
		if ej.attempts >= ej.maxAttempts {
			const deadSQL = `
				UPDATE jobs
				SET state = 'dead',
				    last_error = 'lease expired and attempts exhausted',
				    lease_owner = NULL,
				    updated_at = fencepost_now()
				WHERE id = $1;
			`
			if _, err := tx.Exec(ctx, deadSQL, ej.id); err != nil {
				return 0, fmt.Errorf("update dead reaped: %w", err)
			}

			const eventSQL = `
				INSERT INTO job_events (job_id, kind, fencing_token, worker, at)
				VALUES ($1, 'reaped_dead', $2, $3, fencepost_now());
			`
			if _, err := tx.Exec(ctx, eventSQL, ej.id, ej.token, ej.owner); err != nil {
				return 0, fmt.Errorf("insert reaped_dead event: %w", err)
			}
			metrics.JobAttempts.Observe(float64(ej.attempts))
		} else {
			delay := store.BackoffFunc(ej.attempts, 100*time.Millisecond, 30*time.Second)
			delayMs := delay.Milliseconds()

			const requeueSQL = `
				UPDATE jobs
				SET state = 'queued',
				    run_at = fencepost_now() + ($1 * interval '1 millisecond'),
				    lease_owner = NULL,
				    updated_at = fencepost_now()
				WHERE id = $2;
			`
			if _, err := tx.Exec(ctx, requeueSQL, delayMs, ej.id); err != nil {
				return 0, fmt.Errorf("update requeue reaped: %w", err)
			}

			const eventSQL = `
				INSERT INTO job_events (job_id, kind, fencing_token, worker, at)
				VALUES ($1, 'reaped_queued', $2, $3, fencepost_now());
			`
			if _, err := tx.Exec(ctx, eventSQL, ej.id, ej.token, ej.owner); err != nil {
				return 0, fmt.Errorf("insert reaped_queued event: %w", err)
			}
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit tx: %w", err)
	}

	reapedCount := int32(len(expired))
	metrics.LeaseExpirationsTotal.Add(float64(reapedCount))
	return reapedCount, nil
}

// Clock manipulation methods for rapid model testing

func (s *PGStore) SetTestClock(ctx context.Context, t *time.Time) error {
	const sql = `
		INSERT INTO fencepost_test_clock (id, test_now)
		VALUES (1, $1)
		ON CONFLICT (id) DO UPDATE SET test_now = EXCLUDED.test_now;
	`
	_, err := s.pool.Exec(ctx, sql, t)
	return err
}

func (s *PGStore) AdvanceTestClock(ctx context.Context, d time.Duration) error {
	const sql = `
		UPDATE fencepost_test_clock
		SET test_now = fencepost_now() + ($1 * interval '1 microsecond')
		WHERE id = 1;
	`
	_, err := s.pool.Exec(ctx, sql, d.Microseconds())
	return err
}

func (s *PGStore) ResetTestClock(ctx context.Context) error {
	const sql = `UPDATE fencepost_test_clock SET test_now = NULL WHERE id = 1;`
	_, err := s.pool.Exec(ctx, sql)
	return err
}
