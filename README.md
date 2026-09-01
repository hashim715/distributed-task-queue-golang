# Distributed Task Queue (Go)

A simple in-memory task/job queue written in Go, built as a learning project for exploring queue mechanics, job lifecycles, and (eventually) concurrent worker processing.

## Current Features

- **Job** model with ID, payload, status, attempt count, and creation timestamp (`job.go`)
- **Queue** with in-memory job storage supporting:
  - `Add` — enqueue a job
  - `Process` — mark a job as running and simulate work
  - `Ack` — mark a job as completed
  - `Nack` — mark a job as failed
  - `Remove` — remove a job from the queue
  (`queue.go`)
- Entry point demonstrating the basic add → process → ack/nack → remove flow (`main.go`)

## Status / Roadmap

This project is a work in progress. Planned next steps:

- [ ] Concurrent worker pool to process jobs from the queue (`worker.go` currently a stub)
- [ ] Retry logic using job attempt counts
- [ ] Thread-safe queue access (mutex/channels)
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
