package main

import (
	"context"
	"fmt"
	"strconv"
	"time"
);

const reapScript = `
local claimedAt = redis.call('HGET', KEYS[1], ARGV[1])

if claimedAt == false then
    return "no-timer"
end

if (tonumber(ARGV[2]) - tonumber(claimedAt)) <= tonumber(ARGV[3]) then
    return "not-stale"
end

redis.call('LREM', KEYS[2], 1, ARGV[4])
redis.call('HDEL', KEYS[1], ARGV[1])
redis.call('LPUSH', KEYS[3], ARGV[4])
return "reclaimed"
`;

func (q *RedisQueue) ReapStale(ctx context.Context, staleAfter time.Duration) {
	ticker := time.NewTicker(3 * time.Second);
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
		fmt.Printf("reaper: HgetAll failed:%v\n", err);
		return;
	};

	now := time.Now().Unix();

	for rawJob, claimedStr := range times {
		claimedAt, err := strconv.ParseInt(claimedStr, 10, 64);

		if err != nil {
			continue;
		};

		if now - claimedAt < int64(staleAfter.Seconds()) {
			continue; // not stale yet, skip (avoid an unnecessary Eval call)
		};

		result, err := q.client.Eval(ctx, reapScript, []string{"jobs-processing-times", "jobs-processing", q.key}, rawJob, now, int64(staleAfter.Seconds()), rawJob,).Result();

		if err != nil {
			fmt.Printf("reaper: eval failed, %v\n", err);
			continue;
		};

		switch result {
		case "no-timer":
			// already acked/cleared by the real worker — fine, skip
		case "not-stale":
			// became fresh between our Go-side check and the script running — fine, skip
		case "reclaimed":
			fmt.Printf("reaper: job %s reclaimed, moved back to pending\n",rawJob);
		};
	};
};