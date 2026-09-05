package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

var maxRetries int = 5;

type Queue struct {
	jobs chan *Job
	inFlight sync.WaitGroup
};

func NewQueue() *Queue {
	return &Queue{jobs: make(chan *Job)};
};

func (queue *Queue) Enqueue(job *Job) {
	queue.jobs<-job;
};

func (queue *Queue) CloseWhenDone() {
	queue.inFlight.Wait();
	close(queue.jobs);
};

func (queue *Queue) Dequeue() (*Job,error) {
	job, ok := <-queue.jobs;
	if !ok {
		err := fmt.Errorf("Failed to recieve value from the channel");
		return nil, err;
	};
	return job,nil;
};

func (queue *Queue) Process(job *Job) error {
	fmt.Println("processing.....");
	job.Status = "running";
	time.Sleep(5 * time.Second);
	if rand.Intn(2) == 0 {
		return fmt.Errorf("Hey this process is failed buddy.");
	}
	fmt.Println("Proccess succeeded");
	return nil;
};

func (queue *Queue) Ack(job *Job) {
	job.Status = "completed";
	fmt.Printf("Acknowledged the job with id: %s\n", job.ID);
	queue.inFlight.Done()
};

func (queue *Queue) Nack(ctx context.Context,job *Job) {
	job.Status = "failed";
	job.Attempts++;
	if job.Attempts < maxRetries {
		job.Status = "pending";
		fmt.Printf("job %s: retrying (attempt %d)\n", job.ID, job.Attempts);
		go func() {
			select {
			case queue.jobs <- job:
				// requeued successfully
			case <-ctx.Done():
				// give up, don't requeue, exit cleanly
			}
		}(); // don't block the worker
		// still in flight, so no Done() here
		return;
	};
	fmt.Printf("job %s: exhausted retries, giving up\n", job.ID);
	queue.inFlight.Done();
};