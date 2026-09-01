package main

import (
	"fmt"
	"time"
)

type Queue struct {
	jobs []Job
	current int
};

func NewQueue() *Queue {
	return &Queue{jobs: []Job{}, current: 0};
};

func (queue *Queue) Add(job *Job) {
	queue.jobs = append(queue.jobs, *job);
};

func (queue *Queue) Remove(id string) error {
	index := -1;
	for i := range queue.jobs {
		if queue.jobs[i].ID == id {
			index = i;
			break;
		};
	};
	if index == -1 {
		return fmt.Errorf("Job not found baby");
	};
	queue.jobs = append(queue.jobs[:index], queue.jobs[index+1:]...);
	fmt.Printf("removed the job with id: %s from jobs in the queue\n", id);
	return nil;
};

func (queue *Queue) Dequeue() (*Job,bool) {
	if queue.current > len(queue.jobs) {
		return nil, false
	};
	job := queue.jobs[queue.current];
	queue.current++;
	return &job,true;
};

func (queue *Queue) Process(id string) error {
	index := -1;
	for i := range queue.jobs {
		if queue.jobs[i].ID == id {
			index = i;
			break;
		};
	};
	if index == -1 {
		return fmt.Errorf("Job not found baby");
	};
	fmt.Println("processing.....");
	queue.jobs[index].Status = "running";
	time.Sleep(5 * time.Second);
	fmt.Println("Proccess succeeded");
	return nil;
};

func (queue *Queue) Ack(id string) error {
	index := -1;
	for i := range queue.jobs {
		if queue.jobs[i].ID == id {
			index = i;
			break;
		};
	};
	if index == -1 {
		return fmt.Errorf("Job not found baby");
	};
	queue.jobs[index].Status = "completed";
	queue.Remove(id);
	fmt.Printf("Acknowledged the job with id: %s\n", id);
	return  nil;
};

func (queue *Queue) Nack(id string) error {
	index := -1;
	for i := range queue.jobs {
		if queue.jobs[i].ID == id {
			index = i;
			break;
		};
	};
	if index == -1 {
		return fmt.Errorf("Job not found baby");
	};
	queue.jobs[index].Status = "failed";
	fmt.Printf("Not Acknowledged the job with id: %s\n", id);
	return  nil;
};