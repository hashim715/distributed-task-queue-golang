# Distributed Task Queue (Go)

A simple in-memory task/job queue written in Go, built as a learning project for exploring queue mechanics, job lifecycles, and (eventually) concurrent worker processing.

## Current Features

- **Job** model with ID, payload, status, attempt count, and creation timestamp (`job.go`)
- **Queue** backed by a Go channel (`chan *Job`), supporting:
  - `Enqueue` — send a job onto the channel
  - `Dequeue` — receive the next job (blocks until one is available or the channel is closed)
  - `Process` — mark a job as running and simulate work
  - `Ack` — mark a job as completed
  - `Nack` — mark a job as failed (increments attempt count)
  (`queue.go`)
- **Worker pool**: multiple goroutines concurrently pull jobs off the queue and process them until the channel is closed and drained (`worker.go`)
- Entry point that starts the worker pool, then enqueues jobs and closes the channel to signal no more work (`main.go`)

Using a channel instead of a shared slice means concurrent access to the queue is safe without a manual mutex — sends/receives are synchronized by the Go runtime.

## Status / Roadmap

This project is a work in progress. Planned next steps:

- [ ] Wire up `maxRetries` so failed jobs (`Nack`) are retried instead of dropped
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
