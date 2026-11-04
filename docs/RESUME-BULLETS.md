# Resume Bullets (Draft)

> **Note**: As specified by project guidelines, all resume bullet suggestions are strictly kept in this file and prefixed with `DRAFT:`. Every metric cited is directly backed by measured results recorded in [`bench/RESULTS.md`](../bench/RESULTS.md).

* DRAFT: Architected and implemented **fencepost**, a distributed job queue and cron scheduler in Go 1.25 and PostgreSQL 16+ ensuring effectively-once downstream side effects via monotonic Kleppmann fencing tokens and database-clock leases.
* DRAFT: Built a comprehensive chaos engineering harness with randomized SIGSTOP/SIGKILL nemeses, proving 0 duplicate downstream effects across 20,000 jobs under 30 process pauses (>TTL) and 7 worker kills, while verifying 29 duplicate effects occurred when fencing was disabled.
* DRAFT: Optimized PostgreSQL lease acquisition using `FOR UPDATE SKIP LOCKED` and partial indexing, scaling single-node throughput from 330 jobs/s to 4,763 jobs/s across 16 concurrent workers with p50 enqueue-to-start latency under 2.4s.
* DRAFT: Designed high-throughput batch leasing amortizing database roundtrips, boosting end-to-end processing throughput by 71% (2,782 to 4,759 jobs/s) at batch size 10 and peaking at 5,253 jobs/s at batch size 50.
* DRAFT: Engineered PostgreSQL session-level advisory lock leader election for distributed cron scheduling, eliminating double-fires via deterministic unix-timestamp idempotency keys and ensuring sub-second failover.
* DRAFT: Verified queue invariants and state machine transitions using property-based testing (`pgregory.net/rapid`) across dual storage engines (`pgstore` and `memstore`) and zero double-lease races across 10,000 jobs under 64 concurrent goroutines.
