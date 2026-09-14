package main

import (
	"context"
	"fmt"
	"strconv"
	"time"
);

const reapScript = `
-- KEYS[1] = jobs-processing-times
-- KEYS[2] = jobs-processing
-- KEYS[3] = jobs-pending (sorted set, scored by priority)
-- ARGV[1] = rawjob (string)
-- ARGV[2] = unit timestamp
-- ARGV[3] = staleAfter (seconds)
-- ARGV[4] = SOME_LARGE_OFFSET (passed from Go, keeps the formula in one place)

local claimedAt = redis.call('HGET', KEYS[1], ARGV[1])

if claimedAt == false then
    return "no-timer"
end

if (tonumber(ARGV[2]) - tonumber(claimedAt)) <= tonumber(ARGV[3]) then
    return "not-stale"
end

redis.call('LREM', KEYS[2], 1, ARGV[1])
redis.call('HDEL', KEYS[1], ARGV[1])

local decoded = cjson.decode(ARGV[1])
local score = (decoded.priority * tonumber(ARGV[4])) + tonumber(ARGV[2])

redis.call('ZADD', KEYS[3], score, ARGV[1])

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

		result, err := q.client.Eval(ctx, reapScript, []string{"jobs-processing-times", "jobs-processing", q.key}, rawJob, now, int64(staleAfter.Seconds()),SOME_LARGE_OFFSET).Result();

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