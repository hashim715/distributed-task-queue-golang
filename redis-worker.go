package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/redis/go-redis/v9"
)

func redisWorker(ctx context.Context,id int, q *RedisQueue, wg *sync.WaitGroup) {
	defer wg.Done();

	for {
		select {
		case <-ctx.Done():
			fmt.Printf("worker %d: shutdown signal received\n", id);
			return;
		default:	
		}
		
		jobStr, err := q.Dequeue(ctx);
		if err != nil {
			if ctx.Err() != nil {
				fmt.Printf("worker %d: shutting down (%v)\n", id, ctx.Err())
				return
			}
			if err == redis.Nil {
				continue // idle timeout, totally normal, not an error
			}
			fmt.Printf("worker %d: dequeue error: %v\n", id, err) // now only real errors land here
			continue
		};

		var job Job;
		if err := json.Unmarshal([]byte(jobStr), &job); err != nil {
			fmt.Printf("worker %d: bad job payload: %v\n", id, err)
			continue
		};

		fmt.Printf("worker %d: picked up job %s\n", id, job.ID)

		if err := q.Process(ctx,&job); err != nil {
			err := q.Nack(ctx, &job);
			if err != nil {
				fmt.Println(err);
			};
		} else {
			err := q.Ack(ctx,&job);
			if err != nil {
				fmt.Println(err);
			};
		};
	};
};