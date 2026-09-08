package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
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
	result, err := q.client.BLMove(ctx, q.key, "jobs-processing", "right", "left", 10*time.Second).Result();

	if err != nil {
		return "", err;
	};

	// BRPop returns [key, value] — we want the value
	return result, nil;
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

func (q *RedisQueue) Ack(ctx context.Context, job *Job, originalData string) error {
	if err := q.client.LRem(ctx, "jobs-processing", 1, originalData).Err(); err != nil {
		return err
	};

	job.Status = "completed";
	fmt.Printf("Acknowledged the job with id: %s\n", job.ID);

	if err := q.client.HDel(ctx, "jobs-processing-times", job.ID).Err(); err != nil {
		return err;
	};

	return nil;
};

func (q *RedisQueue) ReapStale(ctx context.Context, staleAfter time.Duration) {
	ticker := time.NewTicker(10 * time.Second);
	defer ticker.Stop();

	for {
		select {
		case <-ctx.Done():
			return;
		case <-ticker.C:
			q.reapOnce(ctx, staleAfter);
		};
	};
};

func (q *RedisQueue) reapOnce(ctx context.Context, staleAfter time.Duration) {
	times, err := q.client.HGetAll(ctx, "jobs-processing-times").Result();

	if err != nil {
		return;
	};

	type entry struct {
		raw string
		job Job
	};

	now := time.Now().Unix();

	jobStr,err := q.client.LRange(ctx, "jobs-processing", 0, -1).Result();

	if err != nil {
		return;
	};

	jobs := []entry{};

	for _,job := range jobStr {
		var parsedJob Job;

		if err := json.Unmarshal([]byte(job), &parsedJob); err != nil {
			continue
		};

		jobs = append(jobs, entry{raw:job, job:parsedJob});
	};

	for jobID, claimedStr := range times {
		claimedAt, _ := strconv.ParseInt(claimedStr, 10, 64);

		if now - claimedAt > int64(staleAfter.Seconds()) {
			fmt.Printf("reaper: job %s looks stale, requeuing\n", jobID);
			for _,job := range jobs {
				if job.job.ID == jobID {
					if err := q.client.LRem(ctx, "jobs-processing", 1, job.raw).Err(); err != nil {
						continue;
					};

					if err := q.client.HDel(ctx, "jobs-processing-times", jobID).Err(); err != nil {
						continue;
					};
					
					if err := q.client.LPush(ctx, q.key, job.raw).Err(); err != nil {
						continue;
					};
				};
			};
		};
	};
};

func (q *RedisQueue) Nack(ctx context.Context,job *Job, originalData string) error {
	if err := q.client.LRem(ctx, "jobs-processing", 1, originalData).Err(); err != nil {
		return err;
	};

	if err := q.client.HDel(ctx, "jobs-processing-times", job.ID).Err(); err != nil {
		return err;
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

		fmt.Printf("job %s: retrying (attempt %d)\n", job.ID, job.Attempts);
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

	fmt.Printf("job %s: exhausted retries, giving up\n", job.ID);

	return nil;
};