package main

import (
	"context"
	"fmt"
	"sync"
)

func worker(ctx context.Context,id int, queue *Queue, wg *sync.WaitGroup) {
	defer wg.Done();

	for {
		select {
		case <-ctx.Done():
			fmt.Printf("worker %d: shutdown signal received\n", id);
			return;
		case job, ok := <-queue.jobs: // pull next job, if any
			if !ok {
				fmt.Printf("worker %d: shutting down, queue closed\n", id)
				return; // no more jobs
			};
	
			fmt.Printf("worker %d: picked up job %s\n", id, job.ID)
			err := queue.Process(job);
	
			if err != nil {
				queue.Nack(ctx,job);
			} else {
				queue.Ack(job)
			};
		}	
	};
};