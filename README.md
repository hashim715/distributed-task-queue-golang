# Distributed Task Queue (Go)

A task/job queue written in Go, built as a learning project for exploring queue
mechanics, job lifecycles, and concurrent worker processing. Started as an in-memory
channel-backed queue and is now growing a Redis-backed implementation so jobs can be
shared across multiple worker processes.

## Current Features

- **Job** model with ID, payload, status, attempt count, and creation timestamp (`job.go`)
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
- **Redis-backed queue** (`redis.go`) using Redis lists, so jobs can persist and be
  shared across multiple worker processes instead of living only in one process's
  memory. Uses a reliable-queue pattern with three named lists — `jobs-pending`,
  `jobs-processing`, and `jobs-deadletter` — so a job is never silently dropped:
  - `Enqueue` — JSON-encodes the job and `LPUSH`es it onto `jobs-pending`
  - `Dequeue` — `BLMOVE`s the next job from `jobs-pending` into `jobs-processing`,
    blocking until one is available, the timeout elapses, or `ctx` is cancelled — so
    a job stays visible in `jobs-processing` instead of disappearing if a worker
    crashes mid-`Process`. **Known gap:** the `BLMOVE` and the follow-up `HSET` that
    records the claim timestamp in `jobs-processing-times` are two separate,
    non-atomic Redis calls — a crash between them leaves the job in `jobs-processing`
    with no timer entry, permanently invisible to the reaper below. A Lua-script-based
    atomic replacement is drafted (commented out) in `redis.go`; not yet wired in.
  - `Process` — same simulated work/failure behavior as the in-memory queue, but
    also respects `ctx` cancellation so a cancelled context can interrupt the
    simulated work rather than only being checked between jobs
  - `Ack` — atomically removes the job from `jobs-processing` and clears its
    `jobs-processing-times` entry via a single Lua script (keyed by the job's raw
    JSON payload, matching how the job is stored in the lists), then marks it
    completed
  - `Nack` — same atomic cleanup as `Ack`, then re-`LPUSH`es the job onto
    `jobs-pending` (up to `maxRetries`), logging the failure reason on each retry, or,
    once retries are exhausted, marks it `dead` and pushes it onto `jobs-deadletter`
    (also logged with the failure reason) instead of dropping it
- **Stale-job reaper** (`reaper.go`): a background goroutine (ticking every few
  seconds) that scans `jobs-processing-times` for jobs claimed longer than a
  configurable `staleAfter` window — catching jobs left behind by a worker that
  crashed or hung before calling `Ack`/`Nack` — and atomically reclaims them (via a
  Lua script) back onto `jobs-pending`, logging each reclaim. Since it only discovers
  candidates via `jobs-processing-times`, it cannot see jobs orphaned by the
  `Dequeue` race noted above (those never got a timer entry in the first place)
- **Worker pools** for both queues: multiple goroutines concurrently pull jobs and
  process them, each logging its own ID as it picks up and finishes jobs
  (`worker.go` for the in-memory queue, `redis-worker.go` for the Redis queue)
- **Graceful shutdown**: `main.go` listens for `SIGINT`/`SIGTERM` and cancels a shared
  `context.Context`, so workers can stop cleanly mid-run instead of only stopping once
  the queue is drained
- **End-to-end test script** (`test.sh`): exercises the Redis queue against a real
  Redis instance — normal processing, crash + reaper recovery (`kill -9` mid-job), and
  dead-letter exhaustion — inspecting queue state via `redis-cli` between runs

Using a channel instead of a shared slice means concurrent access to the in-memory
queue is safe without a manual mutex — sends/receives are synchronized by the Go
runtime. The Redis queue trades that in-process safety for durability and the ability
to run workers in separate processes (or on separate machines) against the same list.

## Status / Roadmap

This project is a work in progress. Planned next steps:

- [ ] Thread `context.Context` through `Process` on the in-memory queue too (the
      Redis queue's `Process` already respects cancellation)
- [ ] Bound retry requeues on the in-memory queue (currently each retry spawns its
      own goroutine to send without blocking the worker; fine at small scale, but
      unbounded under heavy load)
- [x] Requeue/expire jobs stuck in `jobs-processing` from workers that crash before
      calling `Ack`/`Nack` — handled by the stale-job reaper
- [ ] Close the non-atomic `Dequeue` race (see the `Dequeue` known gap above) so a
      crash between the claim and the timer write can't orphan a job past the reaper
- [ ] Distributed coordination across multiple worker processes using the Redis queue

## Requirements

- Go 1.24.3+
- A running Redis server (default `localhost:6379`) for the Redis-backed queue

## Running

```bash
go run .
```

This starts the worker pool, Redis-backed queue, and stale-job reaper, but does not
enqueue any jobs on its own (the demo `NewJob`/`Enqueue` calls in `main.go` are
currently commented out) — feed it jobs from another terminal, e.g.:

```bash
redis-cli LPUSH jobs-pending '{"ID":"demo1","Payload":"hi","Status":"pending","Attempts":0,"CreatedAt":"today"}'
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
├── redis.go         # Redis list-backed queue
├── redis-worker.go  # Worker pool for the Redis queue
├── reaper.go        # Background stale-job reaper for the Redis queue
└── test.sh          # End-to-end test script against a real Redis instance
```
