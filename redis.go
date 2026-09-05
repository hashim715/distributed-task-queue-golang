package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisQueue struct {
	client *redis.Client
	key string
};

func NewRedisQueue(client *redis.Client, key string) *RedisQueue {
	return &RedisQueue{client: client, key: key};
};

func (q *RedisQueue) Enqueue(ctx context.Context, job *Job) error {
	data, err := json.Marshal(job);
	if err != nil {
		return err;
	};
	return q.client.LPush(ctx, q.key, data).Err();
};

func (q *RedisQueue) Dequeue(ctx context.Context) (string, error) {
	// BRPop blocks until something is available (or ctx is cancelled)
	result, err := q.client.BRPop(ctx, 2*time.Second, q.key).Result();
	if err != nil {
		return "", err;
	};
	// BRPop returns [key, value] — we want the value
	return result[1], nil;
};

func (q *RedisQueue) Process(ctx context.Context,job *Job) error {
	fmt.Println("processing.....");
	job.Status = "running";
	select {
	case <-time.After(5 * time.Second):
	case <-ctx.Done():
		return ctx.Err();
	};
	if rand.Intn(2) == 0 {
		return fmt.Errorf("Hey this process is failed buddy.");
	};
	fmt.Println("Proccess succeeded");
	return nil;
};

func (q *RedisQueue) Ack(ctx context.Context, job *Job) error {
	job.Status = "completed";
	fmt.Printf("Acknowledged the job with id: %s\n", job.ID);
	return nil;
};

func (q *RedisQueue) Nack(ctx context.Context,job *Job) error {
	job.Status = "failed";
	job.Attempts++;
	if job.Attempts < maxRetries {
		job.Status = "pending";
		data, err := json.Marshal(job);
		if err != nil {
			return err;
		};
		if err := q.client.LPush(ctx, q.key, data).Err(); err != nil {
			return err;
		};
		fmt.Printf("job %s: retrying (attempt %d)\n", job.ID, job.Attempts);
		return nil;
	};
	fmt.Printf("job %s: exhausted retries, giving up\n", job.ID);
	return nil;
};