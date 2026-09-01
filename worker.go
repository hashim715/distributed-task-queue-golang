package main

import "sync"

func worker(queue *Queue, wg *sync.WaitGroup) {
	defer wg.Done();

	for {
		job, ok := queue.Dequeue(); // pull next job, if any
		if !ok {
			return; // no more jobs
		};

		err := queue.Process(job.ID);

		if err != nil {
			queue.Nack(job.ID);
		} else {
			queue.Ack(job.ID)
		};
	};
};