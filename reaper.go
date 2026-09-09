package main

import (
	"context"
	"encoding/json"
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

	rawJobs,err := q.client.LRange(ctx, "jobs-processing", 0, -1).Result();

	if err != nil {
		return;
	};

	type entry struct {
		raw string
		jobId string
	};

	entries := []entry{};

	for _,raw := range rawJobs {
		var parsedJob Job;

		if err := json.Unmarshal([]byte(raw), &parsedJob); err != nil {
			continue
		};

		entries = append(entries, entry{raw:raw, jobId:parsedJob.ID});
	};

	now := time.Now().Unix();

	for jobID, claimedStr := range times {
		claimedAt, err := strconv.ParseInt(claimedStr, 10, 64);

		if err != nil {
			continue;
		};

		if now - claimedAt < int64(staleAfter.Seconds()) {
			continue; // not stale yet, skip (avoid an unnecessary Eval call)
		};

		var raw string;
		var found bool = false;

		for _, e := range entries {
			if e.jobId == jobID {
				raw = e.raw;
				found = true;
				break;
			};
		};

		if (!found) {
			continue; // job already gone from processing, nothing to reclaim
		};

		result, err := q.client.Eval(ctx, reapScript, []string{"jobs-processing-times", "jobs-processing", q.key}, jobID, now, int64(staleAfter.Seconds()), raw,).Result();

		if err != nil {
			fmt.Printf("reaper: eval failed for %s: %v\n", jobID, err);
			continue;
		};

		switch result {
		case "no-timer":
			// already acked/cleared by the real worker — fine, skip
		case "not-stale":
			// became fresh between our Go-side check and the script running — fine, skip
		case "reclaimed":
			fmt.Printf("reaper: job %s reclaimed, moved back to pending\n", jobID);
		};
	};
};