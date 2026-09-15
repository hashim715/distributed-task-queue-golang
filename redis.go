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

const SOME_LARGE_OFFSET uint64 = 10_000_000_000;

const removeFromProcessingScript = `
-- KEYS[1] = jobs-processing
-- KEYS[2] = jobs-processing-times
-- ARGV[1] = rawJob (string)

redis.call('LREM', KEYS[1], 1, ARGV[1])

redis.call('HDEL', KEYS[2], ARGV[1])

return "ok"
`;

const dequeueScript = `
-- priorityDequeueScript
-- KEYS[1] = jobs-pending (sorted set now, not a list)
-- KEYS[2] = jobs-processing
-- KEYS[3] = jobs-processing-times
-- ARGV[1] = current unix timestamp

local popped = redis.call('ZPOPMIN', KEYS[1])
if #popped == 0 then
    return nil
end

local jobData = popped[1] -- ZPOPMIN returns [member, score]

redis.call('LPUSH', KEYS[2], jobData)

redis.call('HSET', KEYS[3], jobData, ARGV[1])

return jobData
`;

const scheduleReleaseScript = `
-- scheduleReleaseScript
-- KEYS[1] = jobs-scheduled (sorted set, scored by run-at time)
-- KEYS[2] = jobs-pending (sorted set, scored by priority)
-- ARGV[1] = current unix timestamp
-- ARGV[2] = max number of due jobs to release per call
-- ARGV[3] = SOME_LARGE_OFFSET (passed from Go, keeps the formula in one place)

local due = redis.call('ZRANGE', KEYS[1], '-inf', ARGV[1], 'BYSCORE', 'LIMIT', 0, tonumber(ARGV[2]))

if #due == 0 then
    return 0
end

for i, jobData in ipairs(due) do
    redis.call('ZREM', KEYS[1], jobData)

    local decoded = cjson.decode(jobData)
    local score = (decoded.priority * tonumber(ARGV[3])) + tonumber(ARGV[1])

    redis.call('ZADD', KEYS[2], score, jobData)
end

return #due
`;

const addMetadataScript = `
-- KEYS[1] = job:ID

-- ARGV[1] = status
-- ARGV[2] = attempts
-- ARGV[3] = unix timestamp

redis.call('HSET', KEYS[1], 'status', ARGV[1], 'attempts', ARGV[2], 'updated_at', ARGV[3])

return "ok"
`;

func NewRedisQueue(client *redis.Client, key string) *RedisQueue {
	return &RedisQueue{client: client, key: key};
};

func (q *RedisQueue) removeFromProcessing(ctx context.Context, originalData string) error {
	return q.client.Eval(ctx, removeFromProcessingScript,
		[]string{"jobs-processing", "jobs-processing-times"},
		originalData,
	).Err();
};

func (q *RedisQueue) setJobMetadata(ctx context.Context, job *Job) error {
	key := "job:" + job.ID;

	if err := q.client.Eval(ctx, addMetadataScript, []string{key},job.Status,job.Attempts,time.Now().Unix()).Err(); err != nil {
		return err;
	};

	// Only terminal states get a TTL - a job still pending/running should stay
	// queryable indefinitely, since it isn't done yet.
	if job.Status == "completed" || job.Status == "dead" {
		return q.client.Expire(ctx, key, 24*time.Hour).Err()
	};

	return nil;
};

func (q *RedisQueue) Enqueue(ctx context.Context, job *Job) error {
	data, err := json.Marshal(job);

	if err != nil {
		return err;
	};

	unix_timestamp := time.Now().Unix();

	key := "job:" + job.ID;

	// Seeds the full metadata hash (including fields setJobMetadata never touches,
	// like priority/created_at) since this is the job's first write; later
	// lifecycle transitions go through setJobMetadata instead.
	if err := q.client.HSet(ctx, key, map[string]interface{}{
		"status":     job.Status,
		"attempts":   job.Attempts,
		"priority":   job.Priority,
		"created_at": job.CreatedAt,
	}).Err(); err != nil {
		return err;
	};

	score := (uint64(job.Priority) * SOME_LARGE_OFFSET) + uint64(unix_timestamp);

	return q.client.ZAdd(ctx, q.key, redis.Z{Score: float64(score), Member: data}).Err();
};

func (q *RedisQueue) scheduleJobs(ctx context.Context, job *Job, runtime time.Time) error {
	data, err := json.Marshal(job);

	if err != nil {
		return err;
	};

	return q.client.ZAdd(ctx, "jobs-scheduled", redis.Z{Score: float64(runtime.Unix()),Member: data}).Err();
};

func (q *RedisQueue) runScheduler(ctx context.Context)  {
	ticker := time.NewTicker(time.Second * 2);

	defer ticker.Stop();

	for {
		select {
		case <-ctx.Done():
			return;
		case <-ticker.C:
			now := time.Now().Unix();

			scheduledJobs, err := q.client.Eval(ctx, scheduleReleaseScript, []string{"jobs-scheduled","jobs-pending"},now,100,SOME_LARGE_OFFSET,).Result();

			if err != nil {
				continue;
			};

			n, ok := scheduledJobs.(int64);
			
			if ok {
				if n > 0 {
					fmt.Printf("scheduler: released %d due job(s)\n", n);
				};
			} else {
				// fmt.Printf("scheduler: released 0 due job(s)\n");
			};
		};
	};
};

func (q *RedisQueue) Dequeue(ctx context.Context) (string, error) {
	tryDequeue := func() (string, bool, error) {
		result, err := q.client.Eval(ctx, dequeueScript,
			[]string{q.key, "jobs-processing", "jobs-processing-times"},
			time.Now().Unix(),
		).Result();

		if err != nil {
			return "", false, err
		};

		if result == nil {
			return "", false, nil
		};

		return result.(string), true, nil
	};

	if s, ok, err := tryDequeue(); err != nil {
		return "", err;
	} else if ok {
		return s, nil;
	};

	ticker := time.NewTicker(200 * time.Millisecond);
	defer ticker.Stop();

	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-ticker.C:
			s, ok, err := tryDequeue()
			if err != nil {
				return "", err
			};
			if ok {
				return s, nil
			};
		};
	};
};

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

	if err := q.setJobMetadata(ctx, job); err != nil {
		return err;
	};

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

		unix_timestamp := time.Now().Unix();

		score := (uint64(job.Priority) * SOME_LARGE_OFFSET) + uint64(unix_timestamp);

	    if err := q.client.ZAdd(ctx, q.key, redis.Z{Score: float64(score), Member: data}).Err(); err != nil {
			return err;
		};

		fmt.Printf("job %s: failed (%v), retrying (attempt %d/%d)\n", job.ID, cause, job.Attempts, maxRetries);

		if err := q.setJobMetadata(ctx, job); err != nil {
			return err;
		};

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

	if err := q.setJobMetadata(ctx, job); err != nil {
		return err;
	};

	return nil;
};