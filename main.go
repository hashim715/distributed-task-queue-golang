package main

import (
	"sync"
);

func main() {
	queue := NewQueue();

	var wg sync.WaitGroup = sync.WaitGroup{};
	for i := 0; i < 3; i++ {
		wg.Add(1);
		go worker(queue, &wg);
	};

	job := NewJob("Id1", "Hey body..", "pending" , "today");
	job2 := NewJob("Id2", "Hey body..", "pending" , "today");
	job3 := NewJob("Id3", "Hey body..", "pending" , "today");

	queue.Enqueue(job);
	queue.Enqueue(job2);
	queue.Enqueue(job3);
	close(queue.jobs);

	wg.Wait();
};