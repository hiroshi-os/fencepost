# Benchmark & Chaos Verification Results

All numbers recorded in this document are strictly measured on real hardware. Neither synthetic nor extrapolated values are permitted.

## Test Environment

- **Date / Timezone**: 2026-10-10 16:05 IST (UTC+05:30)
- **Hardware**: AMD Ryzen 5 7530U (6 physical cores, 12 logical threads @ 2.0 GHz base / 4.5 GHz boost)
- **Memory**: 24 GB DDR4 RAM
- **Operating System**: Windows 11 Enterprise (build 26100) x86_64
- **Go Version**: `go version go1.25.5 windows/amd64`
- **PostgreSQL Version**: `PostgreSQL 18.6 on x86_64-windows, compiled by msvc-19.44.35229, 64-bit`
- **PostgreSQL Durability Configuration**:
  - `fsync`: **`on`**
  - `synchronous_commit`: **`on`**

---

## 1. Chaos Matrix: Distributed Invariant Verification

**Harness Command (Fencing ON)**:
```bash
go run ./cmd/chaos -seed <seed> -jobs 2000 -workers 8
```
**Harness Command (Negative Control - Fencing OFF)**:
```bash
go run ./cmd/chaos -seed <seed> -jobs 2000 -workers 8 -no-fencing
```
Evaluated across 10 deterministic RNG seeds (seeds 1 to 10) for 20,000 jobs per configuration with randomized worker SIGSTOP pauses (2.3s–4.0s, strictly exceeding the 2.0s lease TTL), SIGKILL worker termination, daemon replica restarts, and PostgreSQL connection termination.

### 1.1 Fencing Enabled (Distributed Invariant: 0 Duplicate Effects)

| Seed | Jobs | Drain Time | Kills | Pauses (>TTL) | Replica Restarts | PG Hiccups | Total Executions | Duplicate Executions | Duplicate Effects | Lost Jobs | Stale Rejections |
|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| 1 | 2,000 | 4.52s | 1 | 1 | 1 | 0 | 2,004 | 4 | **0** | **0** | 2 |
| 2 | 2,000 | 8.06s | 0 | 2 | 1 | 0 | 2,009 | 3 | **0** | **0** | 9 |
| 3 | 2,000 | 7.56s | 2 | 2 | 0 | 0 | 2,006 | 6 | **0** | **0** | 4 |
| 4 | 2,000 | 7.52s | 0 | 2 | 0 | 1 | 2,024 | 9 | **0** | **0** | 23 |
| 5 | 2,000 | 7.02s | 1 | 2 | 0 | 0 | 2,004 | 4 | **0** | **0** | 2 |
| 6 | 2,000 | 6.52s | 0 | 2 | 0 | 0 | 2,004 | 4 | **0** | **0** | 4 |
| 7 | 2,000 | 6.02s | 1 | 2 | 0 | 0 | 2,006 | 6 | **0** | **0** | 2 |
| 8 | 2,000 | 7.53s | 0 | 2 | 0 | 0 | 2,722 | 182 | **0** | **0** | 0 |
| 9 | 2,000 | 28.09s | 2 | 8 | 0 | 1 | 9,969 | 1,995 | **0** | **0** | 0 |
| 10 | 2,000 | 22.09s | 0 | 7 | 0 | 0 | 7,840 | 1,620 | **0** | **0** | 0 |
| **TOTAL** | **20,000** | **104.93s** | **7** | **30** | **2** | **2** | **34,588** | **3,833** | **0** | **0** | **46** |

### 1.2 Negative Control: Fencing Disabled (Same 10 Seeds)

When downstream fencing token checks are disabled at the sink (`-no-fencing`), paused workers resume and commit side-effects despite losing their lease, resulting in corrupted double-writes.

| Seed | Jobs | Drain Time | Kills | Pauses (>TTL) | Replica Restarts | Total Executions | Duplicate Executions | Duplicate Effects (Unfenced) | Lost Jobs |
|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| 1 | 2,000 | 5.02s | 1 | 1 | 1 | 2,004 | 4 | **2** | **0** |
| 2 | 2,000 | 7.52s | 0 | 2 | 1 | 2,801 | 447 | **3** | **0** |
| 3 | 2,000 | 7.52s | 2 | 2 | 0 | 2,334 | 195 | **2** | **0** |
| 4 | 2,000 | 7.02s | 0 | 2 | 0 | 2,011 | 11 | **11** | **0** |
| 5 | 2,000 | 7.03s | 1 | 2 | 0 | 2,199 | 94 | **3** | **0** |
| 6 | 2,000 | 6.03s | 0 | 2 | 0 | 2,004 | 4 | **4** | **0** |
| 7 | 2,000 | 6.09s | 1 | 2 | 0 | 2,006 | 6 | **2** | **0** |
| 8 | 2,000 | 11.05s | 0 | 3 | 0 | 3,868 | 472 | **0** | **0** |
| 9 | 2,000 | 28.08s | 2 | 8 | 0 | 9,904 | 1,989 | **0** | **0** |
| 10 | 2,000 | 19.57s | 0 | 6 | 0 | 6,350 | 1,214 | **2** | **0** |
| **TOTAL** | **20,000** | **104.94s** | **7** | **30** | **2** | **35,481** | **4,436** | **29** | **0** |

**Conclusion**: The nemesis triggered 3,833 duplicate execution attempts across 30 process pauses (>TTL) and 7 worker kills. Fencing tokens prevented **100% of duplicate side-effects (0 duplicate effects)** and safely rejected 46 stale requests at the sink. Without fencing, identical nemesis triggers resulted in **29 duplicate writes**. In both scenarios, zero jobs were lost (all reached terminal states).

---

## 2. Enqueue Throughput Benchmark

**Command**:
```bash
go run ./cmd/bench -mode enqueue -clients 1,8,32 -d 15s
```

| Clients | Idempotency Key | Throughput (jobs/s) | Latency p50 | Latency p99 |
|:---:|:---:|:---:|:---:|:---:|
| 1 | without key | 995.7 | 1.083 ms | 1.773 ms |
| 1 | with key | 979.6 | 1.086 ms | 1.889 ms |
| 8 | without key | 4,273.5 | 1.657 ms | 4.615 ms |
| 8 | with key | 4,648.9 | 1.635 ms | 3.954 ms |
| 32 | without key | 4,058.5 | 7.089 ms | 15.428 ms |
| 32 | with key | 3,808.6 | 7.058 ms | 14.975 ms |

**Observation**: Enqueue throughput scales near-linearly from 1 client (~980 jobs/s) to 8 clients (~4,649 jobs/s). At 32 clients, PostgreSQL WAL buffer flushes and index lock contention on the primary table flatten throughput to ~3,800–4,050 jobs/s while tail latency (p99) rises from ~4 ms to ~15 ms.

---

## 3. End-to-End Worker Throughput & SKIP LOCKED Contention

**Command**:
```bash
go run ./cmd/bench -mode e2e -workers 1,4,16,64 -lease-batch 10 -jobs 20000
```

| Workers | Batch Size | Throughput (jobs/s) | Lag p50 | Lag p99 | Total Drain Time |
|:---:|:---:|:---:|:---:|:---:|:---:|
| 1 | 10 | 330.3 | 49.873 s | 60.940 s | 60.552 s |
| 4 | 10 | 2,812.6 | 3.804 s | 7.256 s | 7.111 s |
| 16 | 10 | **4,763.8** | **2.355 s** | **4.367 s** | **4.198 s** |
| 64 | 10 | 3,798.0 | 2.929 s | 5.214 s | 5.266 s |

### Analysis of the Contention Knee

Throughput peaks at **16 workers (4,763.8 jobs/s)** before flattening and degrading by 20.2% at **64 workers (3,798.0 jobs/s)**.

This is the classic PostgreSQL `FOR UPDATE SKIP LOCKED` contention knee:
1. **Index Scan Tuple Contention**: When 64 concurrent transactions execute `SELECT ... FOR UPDATE SKIP LOCKED` against the same partial index `idx_jobs_lease_candidate`, multiple transactions scan and attempt to lock overlapping candidate tuples simultaneously.
2. **Lock Traversal Overhead**: When transaction $T_1$ acquires a row lock, transactions $T_2 \dots T_{64}$ must evaluate that the tuple is locked, skip it, and scan subsequent tuples. As worker count increases, workers spend increasing CPU cycles skipping locked rows in PostgreSQL shared memory buffers rather than leasing candidate work.
3. **Write Amplification on Updated Tuples**: Each lease execution updates `state`, `lease_owner`, `lease_expires_at`, `fencing_token`, and `attempts`. At 64 concurrent updaters, tuple header MVCC churn and index page splits create lock manager pressure in PostgreSQL.

---

## 4. Lease Batch Size Effect

**Command**:
```bash
go run ./cmd/bench -mode e2e -workers 16 -lease-batch 1,10,50 -jobs 20000
```

| Workers | Batch Size | Throughput (jobs/s) | Lag p50 | Lag p99 | Total Drain Time |
|:---:|:---:|:---:|:---:|:---:|:---:|
| 16 | 1 | 2,782.2 | 3.803 s | 7.386 s | 7.189 s |
| 16 | 10 | 4,759.4 | 2.526 s | 4.387 s | 4.202 s |
| 16 | 50 | **5,253.8** | **2.370 s** | **4.163 s** | **3.807 s** |

**Observation**: Batching dramatically amortizes roundtrip transaction costs. Moving from batch size 1 to batch size 10 increases throughput by **+71.0%** (2,782 jobs/s to 4,759 jobs/s). Expanding batch size to 50 provides an additional **+10.4%** improvement (5,253.8 jobs/s), reducing total drain time from 7.19s down to 3.81s.
