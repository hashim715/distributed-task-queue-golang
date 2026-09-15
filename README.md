# Distributed Task Queue (Go)

A task/job queue written in Go, built as a learning project for exploring queue
mechanics, job lifecycles, and concurrent worker processing. Started as an in-memory
channel-backed queue and is now growing a Redis-backed implementation so jobs can be
shared across multiple worker processes.

## Current Features

- **Job** model with ID, payload, status, priority, attempt count, and creation
  timestamp (`job.go`). `Priority` is an int where lower is more urgent
  (`0` = urgent, `1` = high, `2` = normal, `3` = low)
- **In-memory queue** backed by a Go channel (`chan *Job`) (`queue.go`). **Not currently
  wired into `main.go`** — `main.go` only constructs and runs the Redis-backed queue
  below; `queue.go`/`worker.go` are kept as a reference implementation. Supports:
  - `Enqueue` — send a job onto the channel
  - `Dequeue` — receive the next job (blocks until one is available or the channel is closed)
  - `Process` — mark a job as running and simulate work (randomly fails, for testing retries)
  - `Ack` — mark a job as completed
  - `Nack` — mark a job as failed; retries the job (up to `maxRetries`) or gives up
  - `CloseWhenDone` — safely closes the queue's channel only once every enqueued job has
    reached a terminal state (acked, or retries exhausted), tracked via an internal
    `sync.WaitGroup` so in-flight retries can never race with the channel closing
- **Redis-backed queue** (`redis.go`), so jobs can persist and be shared across
  multiple worker processes instead of living only in one process's memory. Uses a
  reliable-queue pattern with named keys — `jobs-pending`, `jobs-scheduled`,
  `jobs-processing`, and `jobs-deadletter` — so a job is never silently dropped:
  - `jobs-pending` is a **Redis sorted set**, not a list, scored by
    `(priority * SOME_LARGE_OFFSET) + unixTimestamp`. Since the offset is far
    larger than any realistic timestamp spread, this sorts strictly by priority
    first (lower = more urgent) and falls back to arrival time (FIFO) within the
    same priority — the score comparison never has to "trade off" priority
    against age
  - `Enqueue` — JSON-encodes the job, computes its priority score, and `ZADD`s it
    onto `jobs-pending`
  - `Dequeue` — atomically claims the next job via a Lua script (`dequeueScript`):
    `ZPOPMIN` pops the lowest-scored (highest-priority, then oldest) job from
    `jobs-pending`, `LPUSH`es it onto `jobs-processing`, and `HSET`s its claim
    timestamp into `jobs-processing-times` — all in one atomic step, closing a
    previous race where the claim and the timer write were separate, non-atomic
    calls. Since Redis disallows blocking commands inside `EVAL`, `Dequeue` polls
    this script on a 200ms ticker after one eager non-blocking attempt, rather than
    blocking inside Redis the way a plain `BLMOVE` would
  - `Process` — same simulated work/failure behavior as the in-memory queue, but
    also respects `ctx` cancellation so a cancelled context can interrupt the
    simulated work rather than only being checked between jobs
  - `Ack` — atomically removes the job from `jobs-processing` and clears its
    `jobs-processing-times` entry via a single Lua script (keyed by the job's raw
    JSON payload, matching how the job is stored), then marks it completed
  - `Nack` — same atomic cleanup as `Ack`, then re-scores and `ZADD`s the job back
    onto `jobs-pending` (up to `maxRetries`), logging the failure reason on each
    retry, or, once retries are exhausted, marks it `dead` and pushes it onto
    `jobs-deadletter` (also logged with the failure reason) instead of dropping it
  - `scheduleJobs` / the scheduler goroutine (`runScheduler`) — `ZADD`s a job onto
    `jobs-scheduled`, scored by the Unix timestamp it should run at, for
    delayed/scheduled execution. A ticker periodically runs `scheduleReleaseScript`,
    which atomically finds all due jobs (score ≤ now, capped per call), removes
    them from `jobs-scheduled`, and `ZADD`s each onto `jobs-pending` with its
    priority score computed at release time — so a delayed job still gets prioritized
    correctly once it becomes eligible to run
  - **Job metadata** — a `job:<ID>` Redis hash (`status`, `attempts`, `priority`,
    `created_at`, `updated_at`) tracks each job's lifecycle independently of where
    it currently sits in the queue, so a caller can check on a job's status after
    enqueueing it without needing to scan `jobs-pending`/`jobs-processing`. Written
    at `Enqueue` (full seed), then updated by the worker when it starts running the
    job, and by `Ack`/`Nack`/the reaper's reclaim path on every subsequent
    transition. Once a job reaches a terminal state (`completed` or `dead`), its
    hash gets a 24-hour TTL so it doesn't accumulate forever — a still-pending or
    -running job's hash has no expiry. One caveat: if the worker's post-pickup
    metadata write itself fails, the worker abandons that job in `jobs-processing`
    rather than processing it anyway — recovery then depends on the stale-job
    reaper reclaiming it later, same as a crashed worker
- **Stale-job reaper** (`reaper.go`): a background goroutine (ticking every few
  seconds) that scans `jobs-processing-times` for jobs claimed longer than a
  configurable `staleAfter` window — catching jobs left behind by a worker that
  crashed or hung before calling `Ack`/`Nack` — and atomically reclaims them (via a
  Lua script) back onto `jobs-pending`, re-scoring each by its original priority
  (same `(priority * SOME_LARGE_OFFSET) + unixTimestamp` formula used everywhere
  else) so a reclaimed urgent job doesn't lose its place in line
- **Worker pools** for both queues: multiple goroutines concurrently pull jobs and
  process them, each logging its own ID as it picks up and finishes jobs
  (`worker.go` for the in-memory queue, `redis-worker.go` for the Redis queue)
- **Graceful shutdown**: `main.go` listens for `SIGINT`/`SIGTERM` and cancels a shared
  `context.Context`, so workers can stop cleanly mid-run instead of only stopping once
  the queue is drained
- **End-to-end test script** (`test.sh`): exercises the Redis queue against a real
  Redis instance — normal processing, crash + reaper recovery (`kill -9` mid-job),
  dead-letter exhaustion, scheduled/delayed job release, priority ordering, and job
  metadata tracking — inspecting queue state via `redis-cli` between runs

Using a channel instead of a shared slice means concurrent access to the in-memory
queue is safe without a manual mutex — sends/receives are synchronized by the Go
runtime. The Redis queue trades that in-process safety for durability and the ability
to run workers in separate processes (or on separate machines) against the same keys.

## Status / Roadmap

This project is a work in progress. Planned next steps:

- [ ] Thread `context.Context` through `Process` on the in-memory queue too (the
      Redis queue's `Process` already respects cancellation)
- [ ] Bound retry requeues on the in-memory queue (currently each retry spawns its
      own goroutine to send without blocking the worker; fine at small scale, but
      unbounded under heavy load)
- [x] Requeue/expire jobs stuck in `jobs-processing` from workers that crash before
      calling `Ack`/`Nack` — handled by the stale-job reaper
- [x] Close the non-atomic `Dequeue` race (claim + timer write are now one atomic
      Lua script, so a crash between them can no longer orphan a job past the reaper)
- [x] Priority queueing — `jobs-pending` is a priority-scored sorted set instead of a
      plain FIFO list, so urgent jobs jump ahead of lower-priority ones
- [x] Delayed/scheduled job execution via `jobs-scheduled`
- [x] Persistent per-job status metadata (`job:<ID>` hash), queryable independently
      of the job's current position in the queue, expiring 24h after the job reaches
      a terminal state
- [ ] Distributed coordination across multiple worker processes using the Redis queue

## Requirements

- Go 1.24.3+
- A running Redis server (default `localhost:6379`) for the Redis-backed queue

## Running

```bash
go run .
```

This starts the worker pool, Redis-backed queue, scheduler, and stale-job reaper, but
does not enqueue any jobs on its own (the demo `NewJob`/`Enqueue` calls in `main.go`
are currently commented out) — feed it jobs from another terminal, e.g.:

```bash
# jobs-pending is a sorted set — score = (priority * 10_000_000_000) + unix timestamp.
# Priority 2 (normal) example, scored for "right now":
redis-cli ZADD jobs-pending "$(( (2 * 10000000000) + $(date +%s) ))" \
  '{"ID":"demo1","Payload":"hi","Status":"pending","Priority":2,"Attempts":0,"CreatedAt":"today"}'
```

For a scripted run that pushes jobs, exercises crash recovery, and checks dead-letter
behavior end-to-end, see `test.sh` below instead.

## Project Structure

```
.
├── go.mod           # Module definition
├── go.sum           # Dependency checksums
├── job.go           # Job struct and constructor
├── main.go          # Entry point / demo flow
├── queue.go         # In-memory channel-backed queue
├── worker.go         # Worker pool for the in-memory queue
├── redis.go         # Redis-backed queue (priority sorted set + scheduling)
├── redis-worker.go  # Worker pool for the Redis queue
├── reaper.go        # Background stale-job reaper for the Redis queue
└── test.sh          # End-to-end test script against a real Redis instance
```
