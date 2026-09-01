package main

import (
	"fmt"
	"time"
)

var maxRetries int = 5;

type Queue struct {
	jobs chan *Job
};

func NewQueue() *Queue {
	return &Queue{jobs: make(chan *Job)};
};

func (queue *Queue) Enqueue(job *Job) {
	queue.jobs<-job;
};

func (queue *Queue) Dequeue() (*Job,bool) {
	job, ok := <-queue.jobs;
	if !ok {
		return nil, false;
	};
	return job,true;
};

func (queue *Queue) Process(job *Job) error {
	fmt.Println("processing.....");
	job.Status = "running";
	time.Sleep(5 * time.Second);
	fmt.Println("Proccess succeeded");
	return nil;
};

func (queue *Queue) Ack(job *Job) error {
	job.Status = "completed";
	fmt.Printf("Acknowledged the job with id: %s\n", job.ID);
	return  nil;
};

func (queue *Queue) Nack(job *Job) error {
	job.Status = "failed";
	job.Attempts++;
	fmt.Printf("Not Acknowledged the job with id: %s\n", job.ID);
	return  nil;
};