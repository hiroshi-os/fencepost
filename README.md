# fencepost

[![CI](https://github.com/hiroshi-os/fencepost/actions/workflows/ci.yml/badge.svg)](https://github.com/hiroshi-os/fencepost/actions/workflows/ci.yml)

A distributed job queue and cron scheduler with DB-clock leases, monotonic fencing tokens, idempotent enqueue, and leader-elected cron scheduling. Built in Go 1.25 and PostgreSQL 16+, it guarantees effectively-once downstream side effects under process pauses, crashes, and network partitions.

---

## Results

All figures below are strictly measured on real hardware (AMD Ryzen 5 7530U, 24 GB RAM, Windows 11 / x86_64, PostgreSQL 18.6 with `fsync=on` and `synchronous_commit=on`). Full details, per-seed breakdowns, and commands are documented in [`bench/RESULTS.md`](bench/RESULTS.md).

### Chaos Invariant Verification (20,000 Jobs, 10 Seeds Each)

| Configuration | Jobs | Nemesis Pauses (>TTL) | Nemesis Kills | Replica Restarts | Total Executions | Duplicate Executions | Duplicate Effects | Lost Jobs | Stale Rejections |
|:---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| **Fencing Enabled** | **20,000** | **30** | **7** | **2** | **34,588** | **3,833** | **0** | **0** | **46** |
| **Negative Control (No Fencing)** | 20,000 | 30 | 7 | 2 | 35,481 | 4,436 | **29** | **0** | 0 |

*Zero duplicate side-effects with fencing enabled across 30 process pauses (>TTL) and 7 worker kills; disabling fencing tokens causes 29 corrupted duplicate writes.*

### Throughput & Scaling

| Benchmark Scenario | Concurrency / Workers | Batch Size | Throughput (jobs/s) | Latency / Lag p50 | Latency / Lag p99 |
|:---|:---:|:---:|:---:|:---:|:---:|
| **Enqueue (without key)** | 1 client | - | 995.7 | 1.08 ms | 1.77 ms |
| **Enqueue (with idempotency key)** | 8 clients | - | 4,648.9 | 1.64 ms | 3.95 ms |
| **Enqueue (with idempotency key)** | 32 clients | - | 3,808.6 | 7.06 ms | 14.98 ms |
| **End-to-End Worker Processing** | 1 worker | 10 | 330.3 | 49.87 s | 60.94 s |
| **End-to-End Worker Processing** | 4 workers | 10 | 2,812.6 | 3.80 s | 7.26 s |
| **End-to-End Worker Processing (Peak)** | **16 workers** | **10** | **4,763.8** | **2.36 s** | **4.37 s** |
| **End-to-End Worker Processing (Knee)** | 64 workers | 10 | 3,798.0 | 2.93 s | 5.21 s |
| **Lease Batch Optimization** | 16 workers | 1 | 2,782.2 | 3.80 s | 7.39 s |
| **Lease Batch Optimization** | 16 workers | 50 | **5,253.8** | **2.37 s** | **4.16 s** |

---

## Quickstart

### 1. Launch PostgreSQL and Infrastructure
```bash
docker compose up -d postgres
```

### 2. Apply Schema Migrations
```bash
# Using fencepostd CLI
go run ./cmd/fencepostd migrate -db "postgres://fencepost:fencepost_password@localhost:5432/fencepost?sslmode=disable"
```

### 3. Start fencepostd Daemon
```bash
go run ./cmd/fencepostd -grpc 127.0.0.1:50051 -http 127.0.0.1:8080 -db "postgres://fencepost:fencepost_password@localhost:5432/fencepost?sslmode=disable"
```

### 4. Enqueue a Job with grpcurl
```bash
grpcurl -plaintext -d '{
  "queue": "emails",
  "payload": "eyJ0byI6ICJ1c2VyQGV4YW1wbGUuY29tIiwgInN1YmplY3QiOiAiV2VsY29tZSJ9",
  "priority": 5,
  "idempotency_key": "welcome-user-12345"
}' 127.0.0.1:50051 fencepost.v1.QueueService/Enqueue
```

### 5. Run a Demo Worker
```bash
go run ./cmd/demoworker -server 127.0.0.1:50051 -sink http://127.0.0.1:8081 -queue emails -id worker-1 -concurrency 4 -ttl 2s
```

### 6. Run Small Chaos Test
```bash
make chaos-small
# or: go run ./cmd/chaos -smoke -seed 1 -jobs 1000 -workers 4
```

---

## Design

### Distributed Process Pause Scenario
Under network partitions, heavy GC cycles, or OS thread descheduling, a worker may pause long enough for its lease to expire. When it awakens, it attempts to execute its side-effect against a downstream sink. Fencing tokens guarantee linearizable safety:

```mermaid
sequenceDiagram
    autonumber
    participant W1 as Worker 1
    participant DB as PostgreSQL (fencepost)
    participant R as Reaper
    participant W2 as Worker 2
    participant Sink as Downstream Fenced Sink

    W1->>DB: LeaseJob(queue="payments")
    DB-->>W1: Leased: id=42, token=7, TTL=2s
    Note over W1: Worker 1 begins processing...
    Note over W1: SIGSTOP / GC Pause / OS Freeze (>2s)

    Note over DB,R: Lease expires at DB clock: fencepost_now()
    R->>DB: Reap expired leases
    DB-->>DB: Reset id=42 state='queued', lease_owner=NULL

    W2->>DB: LeaseJob(queue="payments")
    DB-->>W2: Leased: id=42, token=8, TTL=2s
    W2->>Sink: POST /effects (job_id=42, token=8, payload="charge")
    Sink->>Sink: Check: token=8 >= max_token(0) -> ACCEPT
    Sink-->>W2: 200 OK (max_token updated to 8)
    W2->>DB: Complete(id=42, token=8)
    DB-->>W2: 200 OK (state='succeeded')

    Note over W1: Worker 1 awakens (SIGCONT / Unfreezes)
    W1->>Sink: POST /effects (job_id=42, token=7, payload="charge")
    Sink->>Sink: Check: token=7 < max_token(8) -> REJECT!
    Sink-->>W1: 409 Conflict (Stale Fencing Token)
    Note over W1: Stale side-effect rejected. No duplicate charge!
```

### Lease Acquisition SQL
Leases are acquired via atomic Common Table Expressions (CTEs) combining `FOR UPDATE SKIP LOCKED` and PostgreSQL monotonic sequences:

```sql
WITH candidates AS (
    SELECT id
    FROM jobs
    WHERE queue = $1
      AND state = 'queued'
      AND run_at <= fencepost_now()
    ORDER BY priority DESC, run_at ASC, id ASC
    LIMIT $4
    FOR UPDATE SKIP LOCKED
)
UPDATE jobs j
SET state = 'leased',
    lease_owner = $2,
    lease_expires_at = fencepost_now() + ($3 * interval '1 millisecond'),
    fencing_token = nextval('fencing_seq'),
    attempts = attempts + 1,
    updated_at = fencepost_now()
FROM candidates c
WHERE j.id = c.id
RETURNING j.id, j.queue, j.payload, j.priority, j.run_at, j.state,
          j.attempts, j.max_attempts, j.lease_owner, j.lease_expires_at,
          j.fencing_token, j.idempotency_key, j.created_at, j.updated_at;
```

### Why Only the Database Clock Is Used
Node-to-node clock drift and NTP synchronization errors make local server clocks (`time.Now()`) unreliable in distributed environments. A slow clock on a worker can cause it to assume a lease is still valid, while a fast clock on a coordinator can prematurely reap active leases. In `fencepost`, all lease durations, expirations, and cron schedules are evaluated strictly via PostgreSQL's `clock_timestamp()` via `fencepost_now()`, removing any dependency on synchronized machine clocks.

### The Effectively-Once Delivery Argument
`fencepost` combines three mechanisms to achieve effectively-once processing:
1. **Idempotent Enqueue**: Deduplicates submissions via `(queue, idempotency_key)`.
2. **At-Least-Once Execution**: Reaps expired or crashed worker leases and retries with backoff.
3. **Fenced Downstream Effects**: Enforces monotonic token verification ($token \ge max\_token$) at external sinks, rejecting any stale attempts from zombie workers.

### Leader Election Caveats
Cron scheduling utilizes PostgreSQL session-level advisory locks (`SELECT pg_try_advisory_lock(424242)`). Leadership is bound to the liveness of the underlying database TCP connection. If a network partition interrupts communication without terminating the socket, leadership transition may be delayed until the TCP keepalive timeout triggers. All scheduled jobs are enqueued with deterministic timestamp keys (`schedule_id:unix_time`) to prevent double-dispatch during failover.

### Comparison with Hookrelay
While **hookrelay** focuses on resilient HTTP webhook ingestion and outward dispatching with Redis-backed delivery guarantees, **fencepost** provides a general-purpose PostgreSQL-native distributed job queue and cron scheduler featuring monotonic fencing tokens to enforce strict downstream linearizability under worker pauses and crashes.

---

## Honesty & Limitations

1. **At-Least-Once Execution**: Worker execution is strictly at-least-once, NOT exactly-once. Code inside worker handlers may run multiple times if failures occur.
2. **Downstream Participation Required**: Effectively-once semantics require the downstream system (sink, database, or API) to store and validate fencing tokens. If a third-party endpoint does not verify tokens, side effects cannot be guaranteed effectively-once.
3. **Single PostgreSQL Primary SPOF**: High availability relies on PostgreSQL primary replication. `fencepost` does not implement multi-region consensus (e.g. Raft/Paxos) at the queue layer.
4. **Advisory Lock Session Liveness**: Leadership failover speed is governed by PostgreSQL connection termination and TCP keepalive detection.
5. **Single-Table Contention**: Throughput is bounded by row lock contention on the `jobs` table; scaling beyond ~5,000 jobs/s requires table partitioning or sharding.
6. **Harness Environment**: Chaos experiments were executed on a single physical host running isolated OS processes and containerized PostgreSQL rather than multiple distributed datacenters.
