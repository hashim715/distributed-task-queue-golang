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

const removeFromProcessingScript = `
redis.call('LREM', KEYS[1], 1, ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[2])
return "ok"
`;

// const dequeueScript = `
// local job = redis.call('LMOVE', KEYS[1], KEYS[2], 'RIGHT', 'LEFT')
// if job == false then
//     return nil
// end
// redis.call('HSET', KEYS[3], job, ARGV[1])
// return job
// `;

func NewRedisQueue(client *redis.Client, key string) *RedisQueue {
	return &RedisQueue{client: client, key: key};
};

func (q *RedisQueue) removeFromProcessing(ctx context.Context, originalData string) error {
	return q.client.Eval(ctx, removeFromProcessingScript,
		[]string{"jobs-processing", "jobs-processing-times"},
		originalData, originalData,
	).Err();
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
	result, err := q.client.BLMove(ctx, q.key, "jobs-processing", "right", "left", 15*time.Second).Result();

	if err != nil {
		return "", err;
	};

	var job Job;
	if err := json.Unmarshal([]byte(result), &job); err == nil {
		err := q.client.HSet(ctx, "jobs-processing-times", job.ID, time.Now().Unix()).Err();

		if err != nil {
			return "",err;
		};
	};

	// BRPop returns [key, value] — we want the value
	return result, nil;
};

// func (q *RedisQueue) Dequeue(ctx context.Context) (string, error) {
// 	ticker := time.NewTicker(200 * time.Millisecond)

// 	defer ticker.Stop()

// 	for {
// 		select {
// 		case <-ctx.Done():
// 			return "", ctx.Err()
// 		case <-ticker.C:
// 			result, err := q.client.Eval(ctx, dequeueScript,
// 				[]string{q.key, "jobs-processing", "jobs-processing-times"},
// 				time.Now().Unix(),
// 			).Result()
// 			if err != nil {
// 				return "", err
// 			}
// 			if result == nil {
// 				continue // nothing available, poll again
// 			}
// 			return result.(string), nil
// 		};
// 	};
// };

func (q *RedisQueue) Process(ctx context.Context,job *Job) error {
	fmt.Println("processing.....");
	job.Status = "running";
	
	select {
	case <-time.After(2 * time.Second):
	case <-ctx.Done():
		return ctx.Err();
	};

	if rand.Intn(2) == 0 {
		return fmt.Errorf("Hey this process is failed buddy.");
	};

	fmt.Println("Proccess succeeded");
	return nil;
};

func (q *RedisQueue) Ack(ctx context.Context, job *Job, originalData string) error {
	if err := q.removeFromProcessing(ctx, originalData); err != nil {
		return err;
	};

	job.Status = "completed";
	fmt.Printf("Acknowledged the job with id: %s\n", job.ID);

	return nil;
};

func (q *RedisQueue) Nack(ctx context.Context,job *Job, originalData string, cause error) error {
	if err := q.removeFromProcessing(ctx, originalData); err != nil {
		return err
	};

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

		fmt.Printf("job %s: failed (%v), retrying (attempt %d/%d)\n", job.ID, cause, job.Attempts, maxRetries);

		return nil;
	};

	job.Status = "dead";

	data, err := json.Marshal(job);

	if err != nil {
		return err;
	};

	if err := q.client.LPush(ctx, "jobs-deadletter", data).Err(); err != nil {
		return err;
	};

	fmt.Printf("job %s: exhausted retries after %d attempts (%v), moved to dead-letter\n", job.ID, job.Attempts, cause);

	return nil;
};