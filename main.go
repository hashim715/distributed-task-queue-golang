package main

import (
	"sync"
);

func main() {
	queue := NewQueue();
	job := NewJob("Id1", "Hey body..", "nil" , "today");
	queue.Add(job);
	job2 := NewJob("Id2", "Hey body..", "nil" , "today");
	queue.Add(job2);
	job3 := NewJob("Id3", "Hey body..", "nil" , "today");
	queue.Add(job3);
	var wg sync.WaitGroup = sync.WaitGroup{};
	for i := 0; i < 3; i++ {
		wg.Add(1);
		go worker(queue, &wg);
	};
	wg.Wait();
};