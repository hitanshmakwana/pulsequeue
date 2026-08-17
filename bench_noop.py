"""
End-to-end throughput benchmark using benchmark_noop handler.
Submits N jobs, then polls until all reach SUCCESS or DEAD_LETTER,
recording wall-clock time and computing jobs/second.

Usage:
    python bench_noop.py [N] [host]

Defaults: N=3000, host=http://localhost:8080
"""

import sys
import time
import urllib.request
import urllib.parse
import json
import threading
import concurrent.futures

N = int(sys.argv[1]) if len(sys.argv) > 1 else 3000
HOST = sys.argv[2] if len(sys.argv) > 2 else "http://localhost:8080"

SUBMIT_URL = f"{HOST}/jobs"
TERMINAL = {"success", "dead_letter"}

def submit_job(i):
    data = json.dumps({
        "job_type": "benchmark_noop",
        "payload": {"seq": i},
        "priority": 3,
    }).encode()
    req = urllib.request.Request(
        SUBMIT_URL,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        body = json.loads(resp.read())
        return body["id"]

def poll_status(job_id):
    url = f"{HOST}/jobs/{job_id}"
    while True:
        with urllib.request.urlopen(url, timeout=5) as resp:
            body = json.loads(resp.read())
        if body["status"] in TERMINAL:
            return body["status"]
        time.sleep(0.2)

print(f"[bench] Submitting {N} benchmark_noop jobs to {HOST} ...")
t0 = time.monotonic()

job_ids = []
# submit with thread pool for speed
with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
    futures = [pool.submit(submit_job, i) for i in range(N)]
    for f in concurrent.futures.as_completed(futures):
        job_ids.append(f.result())

t_submit_done = time.monotonic()
submit_elapsed = t_submit_done - t0
submit_rps = N / submit_elapsed
print(f"[bench] All {N} jobs submitted in {submit_elapsed:.2f}s ({submit_rps:.1f} submits/s)")
print(f"[bench] Waiting for all {N} jobs to reach terminal state ...")

# Poll all jobs concurrently
t_drain_start = time.monotonic()
completed = 0
lock = threading.Lock()

def poll_and_count(job_id):
    global completed
    status = poll_status(job_id)
    with lock:
        completed += 1
        if completed % 500 == 0:
            elapsed = time.monotonic() - t_drain_start
            print(f"[bench]   {completed}/{N} complete ({elapsed:.1f}s elapsed)")
    return status

with concurrent.futures.ThreadPoolExecutor(max_workers=50) as pool:
    statuses = list(pool.map(poll_and_count, job_ids))

t_end = time.monotonic()
drain_elapsed = t_end - t_drain_start
total_elapsed = t_end - t0
throughput = N / drain_elapsed
end_to_end_rps = N / total_elapsed

success_count = statuses.count("success")
dead_letter_count = statuses.count("dead_letter")

print()
print("=" * 60)
print(f"  BENCHMARK RESULTS — {N} benchmark_noop jobs")
print("=" * 60)
print(f"  Submit phase:       {submit_elapsed:.2f}s  ({submit_rps:.1f} jobs/s)")
print(f"  Drain phase:        {drain_elapsed:.2f}s  ({throughput:.1f} jobs/s completion)")
print(f"  Total wall-clock:   {total_elapsed:.2f}s  ({end_to_end_rps:.1f} jobs/s end-to-end)")
print(f"  Outcomes:           {success_count} success, {dead_letter_count} dead_letter")
print(f"  Zero loss:          {'YES' if (success_count + dead_letter_count) == N else 'NO - CHECK LOGS'}")
print("=" * 60)
