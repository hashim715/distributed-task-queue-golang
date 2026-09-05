# Distributed Task Queue (Go)

A task/job queue written in Go, built as a learning project for exploring queue
mechanics, job lifecycles, and concurrent worker processing. Started as an in-memory
channel-backed queue and is now growing a Redis-backed implementation so jobs can be
shared across multiple worker processes.

## Current Features

- **Job** model with ID, payload, status, attempt count, and creation timestamp (`job.go`)
- **In-memory queue** backed by a Go channel (`chan *Job`) (`queue.go`), supporting:
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
  memory:
  - `Enqueue` — JSON-encodes the job and `LPUSH`es it onto a shared list key
  - `Dequeue` — `BRPOP`s the next job, blocking until one is available or `ctx` is cancelled
  - `Process` — same simulated work/failure behavior as the in-memory queue
  - `Ack` — marks a job completed and removes it from the list
  - `Nack` — marks a job failed; re-`LPUSH`es it (up to `maxRetries`) or gives up
- **Worker pools** for both queues: multiple goroutines concurrently pull jobs and
  process them, each logging its own ID as it picks up and finishes jobs
  (`worker.go` for the in-memory queue, `redis-worker.go` for the Redis queue)
- **Graceful shutdown**: `main.go` listens for `SIGINT`/`SIGTERM` and cancels a shared
  `context.Context`, so workers can stop cleanly mid-run instead of only stopping once
  the queue is drained

Using a channel instead of a shared slice means concurrent access to the in-memory
queue is safe without a manual mutex — sends/receives are synchronized by the Go
runtime. The Redis queue trades that in-process safety for durability and the ability
to run workers in separate processes (or on separate machines) against the same list.

## Status / Roadmap

This project is a work in progress. Planned next steps:

- [ ] Thread `context.Context` through `Process` on both queues so a cancelled
      context can interrupt in-flight (simulated) work, not just the dequeue loop
- [ ] Bound retry requeues (currently each retry spawns its own goroutine to send
      without blocking the worker; fine at small scale, but unbounded under heavy load)
- [ ] Distributed coordination across multiple worker processes using the Redis queue
- [ ] Dead-letter handling for jobs that exhaust their retries

## Requirements

- Go 1.24.3+
- A running Redis server (default `localhost:6379`) for the Redis-backed queue

## Running

```bash
go run .
```

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
└── redis-worker.go  # Worker pool for the Redis queue
```
