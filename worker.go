package main

import "sync"

func worker(id int, queue *Queue, wg *sync.WaitGroup) {
	defer wg.Done();

	for {
		
	}
}