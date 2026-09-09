#!/usr/bin/env bash
#
# End-to-end test script for the Redis-backed distributed task queue.
#
# Covers:
#   1. Normal processing (enqueue -> pending -> processing -> acked, list empties out)
#   2. Crash + reaper recovery (kill -9 mid-job, confirm it's stuck, confirm reaper reclaims it)
#   3. Dead-letter exhaustion (force max retries, confirm it lands in jobs-deadletter)
#
# Requires: redis-cli reachable, a built binary named `taskqueue` in the current dir
#           (build it first: go build -o taskqueue .)
#
# Usage: ./test_queue.sh [1|2|3|all]

set -uo pipefail

BINARY="./taskqueue"
REDIS="redis-cli"

# ---- helpers ----------------------------------------------------------

flush() {
    echo "==> Flushing Redis (clean slate)"
    $REDIS FLUSHALL > /dev/null
}

push_job() {
    local id="$1"
    $REDIS LPUSH jobs-pending \
        "{\"ID\":\"$id\",\"Payload\":\"payload for $id\",\"Status\":\"pending\",\"Attempts\":0,\"CreatedAt\":\"today\"}" \
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

# ---- test 1: normal processing ----------------------------------------

test_normal() {
    section "TEST 1: Normal processing"
    flush

    echo "==> Pushing 5 jobs directly via redis-cli"
    for i in 1 2 3 4 5; do
        push_job "NormalJob$i"
    done
    echo "==> jobs-pending length before run:"
    $REDIS LLEN jobs-pending

    echo "==> Starting program for 15s to let jobs process"
    timeout 15 "$BINARY" || true

    echo "==> jobs-pending length after run (expect 0):"
    $REDIS LLEN jobs-pending
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
    $REDIS LRANGE jobs-pending 0 -1
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
    $REDIS LRANGE jobs-pending 0 -1
    echo "==> jobs-processing (should be empty):"
    $REDIS LRANGE jobs-processing 0 -1
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
    all)
        test_normal
        test_reaper
        test_deadletter
        ;;
    *)
        echo "Usage: $0 [1|2|3|all]"
        exit 1
        ;;
esac

echo
echo "==> Done. Review the LLEN/LRANGE/HGETALL output above against the expectations in each section."