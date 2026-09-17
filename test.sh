#!/usr/bin/env bash
#
# End-to-end test script for the Redis-backed distributed task queue.
#
# Covers:
#   1. Normal processing (enqueue -> pending -> processing -> acked, list empties out)
#   2. Crash + reaper recovery (kill -9 mid-job, confirm it's stuck, confirm reaper reclaims it)
#   3. Dead-letter exhaustion (force max retries, confirm it lands in jobs-deadletter)
#   4. Scheduled/delayed jobs (push into jobs-scheduled with a future score, confirm it
#      stays put until due, then gets released into jobs-pending and processed)
#   5. Priority ordering (jobs-pending is a sorted set scored by priority; push jobs
#      out of order and confirm ZRANGE returns them lowest-score-first, i.e. urgent
#      before high before normal before low)
#   6. Job metadata (job:<ID> hash tracks status/attempts across the job's lifecycle;
#      confirm it goes pending -> running -> completed, and that attempts increments
#      on a retry)
#   7. HTTP server (POST /jobs enqueues or schedules a job depending on whether runAt
#      is set, GET /jobs/{id} returns its metadata, GET /health checks Redis
#      connectivity)
#
# Requires: redis-cli and curl reachable, a built binary named `taskqueue` in the
#           current dir (build it first: go build -o taskqueue .)
#
# Usage: ./test.sh [1|2|3|4|5|6|7|all]

set -uo pipefail

BINARY="./taskqueue"
REDIS="redis-cli"
HTTP_ADDR="http://localhost:8080"

# ---- helpers ----------------------------------------------------------

flush() {
    echo "==> Flushing Redis (clean slate)"
    $REDIS FLUSHALL > /dev/null
}

# priority_score <priority>
SOME_LARGE_OFFSET=10000000000

priority_score() {
    local priority="$1"
    echo $(( (priority * SOME_LARGE_OFFSET) + $(date +%s) ))
}

# push_job <id> [priority]
push_job() {
    local id="$1"

    local priority="${2:-2}"

    local score

    score=$(priority_score "$priority")

    $REDIS ZADD jobs-pending "$score" \
        "{\"ID\":\"$id\",\"Payload\":\"payload for $id\",\"Status\":\"pending\",\"Priority\":$priority,\"Attempts\":0,\"CreatedAt\":\"today\"}" \
        > /dev/null
}

# schedule_job <id> <run_in_seconds> [priority]
schedule_job() {
    local id="$1"

    local run_in="$2"

    local priority="${3:-2}"

    local score=$(($(date +%s) + run_in))

    $REDIS ZADD jobs-scheduled "$score" \
        "{\"ID\":\"$id\",\"Payload\":\"payload for $id\",\"Status\":\"pending\",\"Priority\":$priority,\"Attempts\":0,\"CreatedAt\":\"today\"}" \
        > /dev/null
}

wait_for_pid_and_kill() {
    # kills the taskqueue process forcefully to simulate a real crash
    local pid
    pid=$(pgrep -f "$BINARY" | head -n1)
    if [[ -z "$pid" ]]; then
        echo "!! Could not find running $BINARY process to kill"
        return 1
    fi
    echo "==> Killing PID $pid with SIGKILL (simulated crash)"
    kill -9 "$pid"
}

section() {
    echo
    echo "############################################################"
    echo "# $1"
    echo "############################################################"
}

# wait_for_http <timeout_seconds>
# Polls GET /health until it responds or the timeout elapses.
wait_for_http() {
    local timeout="$1"
    for ((i = 0; i < timeout * 10; i++)); do
        if curl -s -o /dev/null "$HTTP_ADDR/health"; then
            return 0
        fi
        sleep 0.1
    done
    echo "!! HTTP server did not become reachable within ${timeout}s"
    return 1
}

# ---- test 1: normal processing ----------------------------------------

test_normal() {
    section "TEST 1: Normal processing"
    flush

    echo "==> Pushing 5 jobs directly via redis-cli"
    for i in 1 2 3 4 5; do
        push_job "NormalJob$i"
    done
    echo "==> jobs-pending length before run:"
    $REDIS ZCARD jobs-pending

    echo "==> Starting program for 15s to let jobs process"
    timeout 15 "$BINARY" || true

    echo "==> jobs-pending length after run (expect 0):"
    $REDIS ZCARD jobs-pending
    echo "==> jobs-processing length after run (expect 0 if nothing crashed):"
    $REDIS LLEN jobs-processing
    echo "==> jobs-processing-times entries (expect 0 if all cleanly acked/nacked):"
    $REDIS LLEN jobs-processing-times
}

# ---- test 2: crash + reaper recovery -----------------------------------

test_reaper() {
    section "TEST 2: Crash + reaper recovery"
    flush

    echo "==> Pushing 1 job"
    push_job "CrashJob1"

    echo "==> Starting program in background"
    "$BINARY" &
    PROG_PID=$!

    echo "==> Waiting for job to be fully claimed (in processing AND timed)..."
    sleep 1
    
    # for i in $(seq 1 20); do
    # in_processing=$($REDIS LLEN jobs-processing)
    # has_timer=$($REDIS HLEN jobs-processing-times)
    # if [[ "$in_processing" -gt 0 && "$has_timer" -gt 0 ]]; then
    #     echo "==> Job fully claimed after ${i} checks"
    #     break
    # fi
    # sleep 0.2
    # done

    echo "==> jobs-processing BEFORE kill (should contain CrashJob1):"
    $REDIS LRANGE jobs-processing 0 -1

    echo "==> jobs-processing-times BEFORE kill (should contain CrashJob1's timer):"
    $REDIS HGETALL jobs-processing-times

    wait_for_pid_and_kill
    sleep 1

    echo "==> jobs-processing AFTER kill -9 (job should still be stuck here, no owner):"
    $REDIS LRANGE jobs-processing 0 -1
    echo "==> jobs-processing-times AFTER kill -9 (timer entry should still exist):"
    $REDIS HGETALL jobs-processing-times

    echo "==> Starting a FRESH instance so a live reaper is running"
    echo "==> (make sure staleAfter / ticker in your code are short for this test, e.g. 10s/3s)"
    timeout 25 "$BINARY" &
    PROG_PID2=$!
    wait "$PROG_PID2" 2>/dev/null

    echo "==> jobs-processing AFTER reaper cycle (expect empty - job reclaimed):"
    $REDIS LRANGE jobs-processing 0 -1
    echo "==> jobs-pending AFTER reaper cycle (job may be here if reclaimed but not yet reprocessed):"
    $REDIS ZRANGE jobs-pending 0 -1 WITHSCORES
}

# ---- test 3: dead-letter exhaustion -------------------------------------

test_deadletter() {
    section "TEST 3: Dead-letter exhaustion"
    echo "!! NOTE: for this test to reliably trigger, temporarily set maxRetries = 1"
    echo "!!       in your Go code, rebuild, then run this test."
    flush

    echo "==> Pushing 3 jobs (each has ~50% fail chance per attempt)"
    for i in 1 2 3; do
        push_job "DeadJob$i"
    done

    echo "==> Running program for 15s"
    timeout 15 "$BINARY" || true

    echo "==> jobs-deadletter contents:"
    $REDIS LRANGE jobs-deadletter 0 -1
    echo "==> jobs-pending (should be empty - everything either acked or dead-lettered):"
    $REDIS ZRANGE jobs-pending 0 -1 WITHSCORES
    echo "==> jobs-processing (should be empty):"
    $REDIS LRANGE jobs-processing 0 -1
}

# ---- test 4: scheduled/delayed jobs -------------------------------------

test_scheduled() {
    section "TEST 4: Scheduled/delayed jobs"
    flush

    echo "==> Scheduling ScheduledJob1 to run 10s from now, ScheduledJob2 to run 60s from now"
    schedule_job "ScheduledJob1" 10
    schedule_job "ScheduledJob2" 60

    echo "==> jobs-scheduled BEFORE run (expect both jobs, with future scores):"
    $REDIS ZRANGE jobs-scheduled 0 -1 WITHSCORES

    echo "==> jobs-pending BEFORE run (expect empty - nothing due yet):"
    $REDIS ZRANGE jobs-pending 0 -1 WITHSCORES

    echo "==> Starting program for 20s (enough for the 10s job to become due,"
    echo "==> get released by the scheduler, and get processed)"
    "$BINARY" &
    PROG_PID=$!

    echo "==> Waiting 5s (still before ScheduledJob1 is due - should NOT be released yet)"
    sleep 5
    echo "==> jobs-scheduled at +5s (expect both jobs still present):"
    $REDIS ZRANGE jobs-scheduled 0 -1 WITHSCORES
    echo "==> jobs-pending at +5s (expect empty):"
    $REDIS ZRANGE jobs-pending 0 -1 WITHSCORES

    echo "==> Waiting 15s more (ScheduledJob1 should now be due, released, and processed)"
    sleep 15

    kill "$PROG_PID" 2>/dev/null
    wait "$PROG_PID" 2>/dev/null

    echo "==> jobs-scheduled AFTER run (expect only ScheduledJob2 left - not due for 60s):"
    $REDIS ZRANGE jobs-scheduled 0 -1 WITHSCORES
    echo "==> jobs-pending AFTER run (expect empty - ScheduledJob1 released and consumed):"
    $REDIS ZRANGE jobs-pending 0 -1 WITHSCORES
    echo "==> jobs-processing AFTER run (expect empty if ScheduledJob1 finished cleanly):"
    $REDIS LRANGE jobs-processing 0 -1
}

# ---- test 5: priority ordering -------------------------------------------

test_priority() {
    section "TEST 5: Priority ordering"
    flush

    echo "==> Pushing jobs out of priority order: low(3), normal(2), urgent(0), high(1)"
    push_job "LowJob" 3
    push_job "NormalJob" 2
    push_job "UrgentJob" 0
    push_job "HighJob" 1

    echo "==> jobs-pending BEFORE run, ordered by score (ZPOPMIN drains lowest first)."
    echo "==> Expect: UrgentJob, HighJob, NormalJob, LowJob:"
    $REDIS ZRANGE jobs-pending 0 -1 WITHSCORES

    echo "==> Starting program and capturing worker pickup order for 15s"
    echo "==> NOTE: main.go runs 3 concurrent workers, so this isn't a strict"
    echo "==> single-file-line race — with multiple idle workers, more than one"
    echo "==> can claim a job in the same tick. Process() also has a ~50% random"
    echo "==> failure rate, so a job may be Nacked and reappear (re-scored at"
    echo "==> current time, same priority) more than once in this log. The"
    echo "==> reliable assertion is the ZRANGE score ordering above, not the"
    echo "==> picked-up log order/count below."
    timeout 15 "$BINARY" 2>&1 | grep "picked up job" || true

    echo "==> jobs-pending AFTER run (expect empty - all jobs drained):"
    $REDIS ZRANGE jobs-pending 0 -1 WITHSCORES
}

# ---- test 6: job metadata -------------------------------------------------

test_metadata() {
    section "TEST 6: Job metadata"
    flush

    echo "==> Pushing MetaJob1 via redis-cli (bypasses Go's Enqueue, so no job:MetaJob1"
    echo "==> hash exists yet - only Enqueue itself writes the initial metadata):"
    push_job "MetaJob1"
    $REDIS HGETALL job:MetaJob1

    echo "==> Starting program for 15s to let the job run to a terminal state"
    timeout 15 "$BINARY" || true

    echo "==> job:MetaJob1 AFTER run (status should be completed or dead, attempts > 0"
    echo "==> if it was ever retried):"
    $REDIS HGETALL job:MetaJob1

    echo
    echo "==> Re-running with maxRetries effectively forced low isn't done here (see"
    echo "==> TEST 3's note) - instead just confirm attempts tracks retries on a fresh job."
    echo "==> NOT flushing here (would also wipe job:MetaJob1, which we still check below):"
    push_job "MetaJob2"
    echo "==> job:MetaJob2 attempts BEFORE run (expect empty - same redis-cli caveat as above):"
    $REDIS HGET job:MetaJob2 attempts

    timeout 15 "$BINARY" || true

    echo "==> job:MetaJob2 AFTER run (attempts should be >= 1 if it failed at least"
    echo "==> once before reaching a terminal state, per Process()'s ~50% fail rate):"
    $REDIS HGETALL job:MetaJob2

    echo
    echo "==> job:MetaJob1/MetaJob2 TTL in seconds (expect ~86400 / 24h - set once a"
    echo "==> job reaches a terminal state; -1 would mean no expiry, -2 means gone):"
    $REDIS TTL job:MetaJob1
    $REDIS TTL job:MetaJob2
}

# ---- test 7: HTTP server ---------------------------------------------------

test_http() {
    section "TEST 7: HTTP server"
    flush

    echo "==> Starting program in background, waiting for /health"
    "$BINARY" > /tmp/taskqueue_http_test.log 2>&1 &
    PROG_PID=$!
    wait_for_http 10 || { kill "$PROG_PID" 2>/dev/null; return 1; }

    echo "==> GET /health:"
    curl -s "$HTTP_ADDR/health"; echo

    echo
    echo "==> POST /jobs without runAt (regression check: this used to return 201"
    echo "==> but never actually enqueue the job - see git history):"
    RESP=$(curl -s -X POST "$HTTP_ADDR/jobs" -d '{"payload":"http job","priority":2}')
    echo "$RESP"
    JOB_ID=$(echo "$RESP" | grep -o '"id":"[^"]*"' | cut -d'"' -f4)

    sleep 1
    echo "==> job:$JOB_ID exists in Redis (expect 1):"
    $REDIS EXISTS "job:$JOB_ID"
    echo "==> GET /jobs/$JOB_ID (expect it to be found, not 404):"
    curl -s "$HTTP_ADDR/jobs/$JOB_ID"; echo

    echo
    echo "==> POST /jobs with runAt 1h out (regression check: this used to ALSO call"
    echo "==> Enqueue, so a \"delayed\" job ran immediately instead of waiting):"
    RUN_AT=$(date -u -v+1H +%Y-%m-%dT%H:%M:%SZ)
    RESP=$(curl -s -X POST "$HTTP_ADDR/jobs" -d "{\"payload\":\"delayed http job\",\"priority\":1,\"runAt\":\"$RUN_AT\"}")
    echo "$RESP"
    SCHEDULED_ID=$(echo "$RESP" | grep -o '"id":"[^"]*"' | cut -d'"' -f4)

    sleep 1
    echo "==> jobs-pending count (expect 0 - must NOT have run yet):"
    $REDIS ZCARD jobs-pending
    echo "==> GET /jobs/$SCHEDULED_ID (expect status \"scheduled\", not 404):"
    curl -s "$HTTP_ADDR/jobs/$SCHEDULED_ID"; echo

    echo
    echo "==> GET /jobs/does-not-exist (expect 404):"
    curl -s -w " [HTTP %{http_code}]\n" "$HTTP_ADDR/jobs/does-not-exist"

    kill "$PROG_PID" 2>/dev/null
    wait "$PROG_PID" 2>/dev/null
}

# ---- runner -------------------------------------------------------------

if [[ ! -x "$BINARY" ]]; then
    echo "!! $BINARY not found or not executable."
    echo "!! Build it first: go build -o taskqueue ."
    exit 1
fi

case "${1:-all}" in
    1) test_normal ;;
    2) test_reaper ;;
    3) test_deadletter ;;
    4) test_scheduled ;;
    5) test_priority ;;
    6) test_metadata ;;
    7) test_http ;;
    all)
        test_normal
        test_reaper
        test_deadletter
        test_scheduled
        test_priority
        test_metadata
        test_http
        ;;
    *)
        echo "Usage: $0 [1|2|3|4|5|6|7|all]"
        exit 1
        ;;
esac

echo
echo "==> Done. Review the LLEN/LRANGE/HGETALL output above against the expectations in each section."