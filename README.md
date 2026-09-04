# Distributed Task Queue (Go)

A simple in-memory task/job queue written in Go, built as a learning project for exploring queue mechanics, job lifecycles, and (eventually) concurrent worker processing.

## Current Features

- **Job** model with ID, payload, status, attempt count, and creation timestamp (`job.go`)
- **Queue** backed by a Go channel (`chan *Job`), supporting:
  - `Enqueue` — send a job onto the channel
  - `Dequeue` — receive the next job (blocks until one is available or the channel is closed)
  - `Process` — mark a job as running and simulate work (randomly fails, for testing retries)
  - `Ack` — mark a job as completed
  - `Nack` — mark a job as failed; retries the job (up to `maxRetries`) or gives up
  - `CloseWhenDone` — safely closes the queue's channel only once every enqueued job has
    reached a terminal state (acked, or retries exhausted), tracked via an internal
    `sync.WaitGroup` so in-flight retries can never race with the channel closing
  (`queue.go`)
- **Worker pool**: multiple goroutines concurrently pull jobs off the queue and process
  them, each logging its own ID as it picks up and finishes jobs (`worker.go`)
- **Graceful shutdown**: `main.go` listens for `SIGINT`/`SIGTERM` and cancels a shared
  `context.Context`, so workers can stop cleanly mid-run instead of only stopping once
  the queue is drained
- Entry point that registers the initial jobs as in-flight, starts the closer and worker
  goroutines, then enqueues jobs (`main.go`)

Using a channel instead of a shared slice means concurrent access to the queue is safe
without a manual mutex — sends/receives are synchronized by the Go runtime.

## Status / Roadmap

This project is a work in progress. Planned next steps:

- [ ] Bound retry requeues (currently each retry spawns its own goroutine to send
      without blocking the worker; fine at small scale, but unbounded under heavy load)
- [ ] Persistence layer (currently in-memory only)
- [ ] Distributed coordination across multiple worker processes

## Requirements

- Go 1.24.3+

## Running

```bash
go run .
```

## Project Structure

```
.
├── go.mod      # Module definition
├── job.go      # Job struct and constructor
├── main.go     # Entry point / demo flow
├── queue.go    # Queue implementation
└── worker.go   # Worker (in progress)
```
