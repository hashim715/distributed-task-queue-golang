package main

import "fmt"

func main() {
	queue := NewQueue();
	job := NewJob("NewId", "Hey body..", "nil" , "today");
	queue.Add(job);
	fmt.Println(queue.jobs[0]);
	err := queue.Process(job.ID);
	if err != nil {
		queue.Nack(job.ID);
	} else {
		queue.Ack(job.ID);
		queue.Remove(job.ID);
	};
};