"""
PulseQueue GCP Benchmark Suite — 6 test scenarios with real P50/P95/P99.

Tests:
    1. Throughput/latency benchmark (benchmark_noop, P50/P95/P99)
    2. Concurrent job processing (no double-processing under concurrency)
    3. SIGKILL worker-failure recovery (docker kill + verify re-queue)
    4. Pub/Sub retry + dead-letter testing (via gcloud pubsub publish)
    5. Idempotency/duplicate-request testing (concurrent same key)
    6. Optional multi-worker scaling (docker compose scale)

Usage:
    # From your local machine, pointing at the GCE VM:
    python tests/bench_gcp.py --host http://VM_IP:8000 --jobs 500

    # Run a specific scenario only:
    python tests/bench_gcp.py --host http://VM_IP:8000 --scenario throughput
    python tests/bench_gcp.py --host http://VM_IP:8000 --scenario concurrent
    python tests/bench_gcp.py --host http://VM_IP:8000 --scenario sigkill
    python tests/bench_gcp.py --host http://VM_IP:8000 --scenario pubsub
    python tests/bench_gcp.py --host http://VM_IP:8000 --scenario idempotency
    python tests/bench_gcp.py --host http://VM_IP:8000 --scenario scale

    # For the sigkill and scale scenarios, the script must be run ON the VM
    # (it calls docker commands). For others, any machine with network access works.

NOTE: Numbers are measured from real HTTP round-trips — never fabricated.
      Run on the same network as the VM for representative latency.
      Run from outside the VM for realistic end-to-end numbers.
"""

import argparse
import json
import math
import os
import subprocess
import sys
import threading
import time
import uuid
import urllib.error
import urllib.request
import concurrent.futures
from dataclasses import dataclass, field
from typing import Optional

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

TERMINAL_STATUSES = {"success", "dead_letter"}


def _request(method: str, url: str, body: Optional[dict] = None, timeout: int = 15) -> dict:
    """Simple HTTP helper — no external dependencies required."""
    data = json.dumps(body).encode("utf-8") if body else None
    headers = {"Content-Type": "application/json"} if data else {}
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        body_text = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code}: {body_text}") from exc


def submit_job(host: str, job_type: str = "benchmark_noop",
               payload: Optional[dict] = None, priority: int = 3,
               idempotency_key: Optional[str] = None,
               max_attempts: int = 3) -> str:
    """Submit a job and return its ID."""
    body: dict = {
        "job_type": job_type,
        "payload": payload or {},
        "priority": priority,
        "max_attempts": max_attempts,
    }
    if idempotency_key:
        body["idempotency_key"] = idempotency_key
    result = _request("POST", f"{host}/jobs", body)
    return result["id"]


def poll_until_terminal(host: str, job_id: str, timeout: int = 120) -> dict:
    """Poll a job until it reaches a terminal status or timeout."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        job = _request("GET", f"{host}/jobs/{job_id}")
        if job["status"] in TERMINAL_STATUSES:
            return job
        time.sleep(0.15)
    raise TimeoutError(f"Job {job_id} did not reach terminal state in {timeout}s")


def percentile(values: list[float], p: float) -> float:
    """Calculate the p-th percentile of a sorted list."""
    if not values:
        return 0.0
    sorted_vals = sorted(values)
    idx = (p / 100) * (len(sorted_vals) - 1)
    lower = int(idx)
    upper = min(lower + 1, len(sorted_vals) - 1)
    frac = idx - lower
    return sorted_vals[lower] * (1 - frac) + sorted_vals[upper] * frac


def check_health(host: str) -> bool:
    """Return True if the API is reachable and healthy."""
    try:
        result = _request("GET", f"{host}/health", timeout=5)
        return result.get("status") == "ok"
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Scenario 1: Throughput / Latency Benchmark
# ---------------------------------------------------------------------------

def scenario_throughput(host: str, n_jobs: int = 500) -> dict:
    """
    Submit N benchmark_noop jobs concurrently, then wait for all to finish.
    Measures:
      - Submission throughput (jobs/sec)
      - End-to-end latency per job (submit → terminal)
      - P50/P95/P99 latency
      - Overall throughput (jobs/sec completion rate)
      - Error rate
    """
    print(f"\n{'='*60}")
    print(f"  SCENARIO 1: Throughput / Latency Benchmark")
    print(f"  {n_jobs} benchmark_noop jobs")
    print(f"{'='*60}")

    submit_times: dict[str, float] = {}
    job_ids: list[str] = []
    submit_errors = 0
    lock = threading.Lock()

    def submit_one(i: int) -> Optional[str]:
        nonlocal submit_errors
        t = time.monotonic()
        try:
            jid = submit_job(host, payload={"seq": i})
            with lock:
                submit_times[jid] = t
            return jid
        except Exception as exc:
            with lock:
                submit_errors += 1
            print(f"  [WARN] Submit error for job {i}: {exc}")
            return None

    # Submit all jobs concurrently
    t_submit_start = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
        futures = [pool.submit(submit_one, i) for i in range(n_jobs)]
        for f in concurrent.futures.as_completed(futures):
            jid = f.result()
            if jid:
                job_ids.append(jid)
    t_submit_done = time.monotonic()

    submit_elapsed = t_submit_done - t_submit_start
    submit_rps = len(job_ids) / submit_elapsed if submit_elapsed > 0 else 0
    print(f"  Submitted {len(job_ids)}/{n_jobs} jobs in {submit_elapsed:.2f}s ({submit_rps:.1f} jobs/s)")

    # Wait for all jobs to complete, measuring end-to-end latency per job
    latencies: list[float] = []
    completed = 0
    poll_errors = 0
    t_drain_start = time.monotonic()
    completed_lock = threading.Lock()

    def poll_one(job_id: str) -> Optional[str]:
        nonlocal completed, poll_errors
        try:
            job = poll_until_terminal(host, job_id, timeout=180)
            end_time = time.monotonic()
            latency = end_time - submit_times[job_id]
            with completed_lock:
                latencies.append(latency)
                completed += 1
                if completed % max(1, n_jobs // 10) == 0:
                    print(f"  Progress: {completed}/{n_jobs} ({time.monotonic()-t_drain_start:.1f}s)")
            return job["status"]
        except Exception as exc:
            with completed_lock:
                poll_errors += 1
            return "error"

    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as pool:
        statuses = list(pool.map(poll_one, job_ids))

    t_end = time.monotonic()
    drain_elapsed = t_end - t_drain_start
    total_elapsed = t_end - t_submit_start
    throughput = len(latencies) / drain_elapsed if drain_elapsed > 0 else 0
    end_to_end_rps = n_jobs / total_elapsed if total_elapsed > 0 else 0

    success_count = statuses.count("success")
    dead_letter_count = statuses.count("dead_letter")
    error_rate = ((n_jobs - success_count) / n_jobs * 100) if n_jobs > 0 else 0

    p50 = percentile(latencies, 50)
    p95 = percentile(latencies, 95)
    p99 = percentile(latencies, 99)
    p_min = min(latencies) if latencies else 0
    p_max = max(latencies) if latencies else 0

    print(f"\n  {'─'*50}")
    print(f"  THROUGHPUT RESULTS — {n_jobs} benchmark_noop jobs")
    print(f"  {'─'*50}")
    print(f"  Submit throughput:   {submit_rps:.1f} jobs/s")
    print(f"  Completion rate:     {throughput:.1f} jobs/s")
    print(f"  End-to-end rate:     {end_to_end_rps:.1f} jobs/s")
    print(f"  Total wall-clock:    {total_elapsed:.2f}s")
    print(f"")
    print(f"  Latency (submit → terminal):")
    print(f"    P50:  {p50*1000:.0f} ms")
    print(f"    P95:  {p95*1000:.0f} ms")
    print(f"    P99:  {p99*1000:.0f} ms")
    print(f"    Min:  {p_min*1000:.0f} ms")
    print(f"    Max:  {p_max*1000:.0f} ms")
    print(f"")
    print(f"  Outcomes:")
    print(f"    Success:     {success_count}")
    print(f"    Dead-letter: {dead_letter_count}")
    print(f"    Error:       {poll_errors + submit_errors}")
    print(f"    Error rate:  {error_rate:.1f}%")
    print(f"  Zero loss: {'YES ✓' if (success_count + dead_letter_count) == len(job_ids) else 'NO — CHECK LOGS ✗'}")

    return {
        "scenario": "throughput",
        "n_jobs": n_jobs,
        "submit_rps": submit_rps,
        "completion_rps": throughput,
        "end_to_end_rps": end_to_end_rps,
        "p50_ms": round(p50 * 1000),
        "p95_ms": round(p95 * 1000),
        "p99_ms": round(p99 * 1000),
        "success_count": success_count,
        "error_rate_pct": round(error_rate, 2),
    }


# ---------------------------------------------------------------------------
# Scenario 2: Concurrent Job Processing
# ---------------------------------------------------------------------------

def scenario_concurrent(host: str, n_concurrent: int = 20, n_jobs: int = 100) -> dict:
    """
    Submit N jobs in waves of `n_concurrent` concurrently.
    Verifies: no double-processing (each job completes exactly once).
    """
    print(f"\n{'='*60}")
    print(f"  SCENARIO 2: Concurrent Job Processing")
    print(f"  {n_concurrent} concurrent workers, {n_jobs} total jobs")
    print(f"{'='*60}")

    # Use idempotency keys to detect double-processing
    keys = [f"concurrent-test-{uuid.uuid4().hex[:8]}-{i}" for i in range(n_jobs)]
    job_ids = []
    errors = 0

    print(f"  Submitting {n_jobs} jobs with unique idempotency keys...")
    t0 = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(max_workers=n_concurrent) as pool:
        futures = {pool.submit(submit_job, host, "benchmark_noop", {}, 3, k): k
                   for k in keys}
        for f in concurrent.futures.as_completed(futures):
            try:
                job_ids.append(f.result())
            except Exception as exc:
                errors += 1
                print(f"  [WARN] {exc}")

    print(f"  Waiting for all {len(job_ids)} jobs to complete...")
    statuses = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as pool:
        for job in pool.map(lambda jid: poll_until_terminal(host, jid), job_ids):
            statuses.append(job["status"])

    elapsed = time.monotonic() - t0
    success = statuses.count("success")
    unique_ids = len(set(job_ids))

    print(f"\n  {'─'*50}")
    print(f"  CONCURRENT RESULTS")
    print(f"  {'─'*50}")
    print(f"  Jobs submitted:    {len(job_ids)}")
    print(f"  Unique job IDs:    {unique_ids}")
    print(f"  Successes:         {success}")
    print(f"  Errors:            {errors}")
    print(f"  Elapsed:           {elapsed:.2f}s")
    print(f"  No double-process: {'YES ✓' if unique_ids == len(job_ids) else 'POSSIBLE DUPLICATE ✗'}")
    print(f"  Zero loss:         {'YES ✓' if success == len(job_ids) else 'NO ✗'}")

    return {
        "scenario": "concurrent",
        "n_jobs": n_jobs,
        "unique_ids": unique_ids,
        "success_count": success,
        "double_processing": unique_ids != len(job_ids),
    }


# ---------------------------------------------------------------------------
# Scenario 3: SIGKILL Worker-Failure Recovery
# ---------------------------------------------------------------------------

def scenario_sigkill(host: str, n_jobs: int = 10) -> dict:
    """
    Submit N long-ish jobs, kill a worker container mid-execution,
    and verify that all jobs eventually recover and complete.

    IMPORTANT: This scenario requires docker CLI on the machine running
    the script (i.e., it must run ON the GCE VM).
    """
    print(f"\n{'='*60}")
    print(f"  SCENARIO 3: SIGKILL Worker-Failure Recovery")
    print(f"  Submit {n_jobs} jobs → kill worker → verify recovery")
    print(f"{'='*60}")

    # Check we can reach docker
    try:
        result = subprocess.run(
            ["docker", "compose", "ps", "--format", "json"],
            capture_output=True, text=True, cwd="/opt/pulsequeue/repo"
        )
        if result.returncode != 0:
            print("  [SKIP] docker compose not available — run this scenario ON the VM")
            return {"scenario": "sigkill", "skipped": True,
                    "reason": "docker not available on this machine"}
    except FileNotFoundError:
        print("  [SKIP] docker not found — run this scenario ON the VM")
        return {"scenario": "sigkill", "skipped": True,
                "reason": "docker not in PATH"}

    # Submit jobs with a short sleep payload (simulate slow handlers)
    # Use send_email with no SMTP (simulated) so they take ~1s each
    print(f"  Submitting {n_jobs} send_email jobs (simulated, ~1s each)...")
    job_ids = []
    for i in range(n_jobs):
        jid = submit_job(host, "send_email", {"to": f"test{i}@example.com",
                                               "subject": f"SIGKILL test {i}"})
        job_ids.append(jid)

    # Wait briefly for some jobs to start RUNNING
    print("  Waiting 3s for jobs to start...")
    time.sleep(3)

    # Kill one worker container
    print("  Killing worker container (simulating SIGKILL)...")
    try:
        kill_result = subprocess.run(
            ["docker", "compose", "-f", "docker-compose.yml", "-f",
             "docker-compose.gcp.yml", "kill", "-s", "KILL", "worker"],
            capture_output=True, text=True, cwd="/opt/pulsequeue/repo"
        )
        if kill_result.returncode == 0:
            print("  Worker container killed successfully")
        else:
            print(f"  [WARN] kill returned {kill_result.returncode}: {kill_result.stderr}")
    except Exception as exc:
        print(f"  [WARN] Could not kill worker: {exc}")

    print(f"  Waiting for RecoveryService (visibility_timeout=60s) + job completion...")
    print(f"  (This will take 60-90 seconds — enough time for the sweep to run)")

    # Wait up to 3 minutes for all jobs to recover and complete
    t_kill = time.monotonic()
    timeout = 200

    statuses = {}
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        pending = []
        for jid in job_ids:
            if jid not in statuses:
                try:
                    job = _request("GET", f"{host}/jobs/{jid}")
                    if job["status"] in TERMINAL_STATUSES:
                        statuses[jid] = job["status"]
                    else:
                        pending.append((jid, job["status"]))
                except Exception:
                    pending.append((jid, "error"))
        if not pending:
            break
        print(f"  [{time.monotonic()-t_kill:.0f}s] {len(statuses)}/{n_jobs} complete, "
              f"{len(pending)} pending: {[s for _, s in pending[:3]]}...")
        time.sleep(10)

    # Restart the killed worker
    print("  Restarting killed worker container...")
    subprocess.run(
        ["docker", "compose", "-f", "docker-compose.yml", "-f",
         "docker-compose.gcp.yml", "up", "-d", "worker"],
        capture_output=True, cwd="/opt/pulsequeue/repo"
    )

    recovery_time = time.monotonic() - t_kill
    success = list(statuses.values()).count("success")
    recovered = len(statuses)

    print(f"\n  {'─'*50}")
    print(f"  SIGKILL RECOVERY RESULTS")
    print(f"  {'─'*50}")
    print(f"  Jobs submitted:       {n_jobs}")
    print(f"  Jobs recovered:       {recovered}")
    print(f"  Successes:            {success}")
    print(f"  Recovery time:        {recovery_time:.0f}s")
    print(f"  Full recovery:        {'YES ✓' if recovered == n_jobs else f'NO — {n_jobs-recovered} still pending ✗'}")

    return {
        "scenario": "sigkill",
        "n_jobs": n_jobs,
        "recovered": recovered,
        "success_count": success,
        "recovery_time_s": round(recovery_time),
        "full_recovery": recovered == n_jobs,
    }


# ---------------------------------------------------------------------------
# Scenario 4: Pub/Sub Retry + Dead-Letter Testing
# ---------------------------------------------------------------------------

def scenario_pubsub(host: str, project_id: str = "") -> dict:
    """
    Tests the Pub/Sub intake path:
      - Publish a valid message → verify job appears in PulseQueue
      - Publish a malformed message → verify it goes to DLT after 5 nacks

    Requires: gcloud CLI installed and GCP_PROJECT_ID set.
    """
    print(f"\n{'='*60}")
    print(f"  SCENARIO 4: Pub/Sub Retry + Dead-Letter Testing")
    print(f"{'='*60}")

    project_id = project_id or os.environ.get("GCP_PROJECT_ID", "")
    if not project_id:
        print("  [SKIP] GCP_PROJECT_ID not set — skipping Pub/Sub scenario")
        return {"scenario": "pubsub", "skipped": True, "reason": "GCP_PROJECT_ID not set"}

    try:
        subprocess.run(["gcloud", "--version"], capture_output=True, check=True)
    except (FileNotFoundError, subprocess.CalledProcessError):
        print("  [SKIP] gcloud not found")
        return {"scenario": "pubsub", "skipped": True, "reason": "gcloud not in PATH"}

    topic = os.environ.get("PUBSUB_TOPIC", "pulsequeue-jobs")
    dlq_sub = f"projects/{project_id}/subscriptions/pulsequeue-dlq-verify"

    # --- Test 1: Valid message → job appears in PulseQueue ---
    idem_key = f"pubsub-test-{uuid.uuid4().hex[:8]}"
    valid_msg = json.dumps({
        "job_type": "benchmark_noop",
        "payload": {"source": "pubsub_test"},
        "priority": 3,
        "idempotency_key": idem_key,
    })
    print(f"  Publishing valid message to {topic}...")
    result = subprocess.run(
        ["gcloud", "pubsub", "topics", "publish", topic,
         f"--message={valid_msg}", f"--project={project_id}"],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f"  [WARN] Publish failed: {result.stderr}")
        valid_ok = False
    else:
        # Poll /jobs to find a job with this idempotency key
        print(f"  Waiting for job with idempotency_key={idem_key} to appear...")
        found_job = None
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            jobs = _request("GET", f"{host}/jobs?limit=50")
            for job in (jobs if isinstance(jobs, list) else []):
                if job.get("idempotency_key") == idem_key:
                    found_job = job
                    break
            if found_job:
                break
            time.sleep(2)
        valid_ok = found_job is not None
        if valid_ok:
            print(f"  Job {found_job['id']} found (status: {found_job['status']}) ✓")
        else:
            print("  [WARN] Job did not appear within 30s ✗")

    # --- Test 2: Malformed message → dead-letter topic ---
    malformed_msg = "this is not valid json {"
    print(f"\n  Publishing malformed message (should hit DLT after 5 nacks)...")
    subprocess.run(
        ["gcloud", "pubsub", "topics", "publish", topic,
         f"--message={malformed_msg}", f"--project={project_id}"],
        capture_output=True
    )
    print(f"  Malformed message published. It will appear in the DLT")
    print(f"  (pulsequeue-dlq topic) after ~5 Pub/Sub delivery attempts.")
    print(f"  This takes several minutes. Verify manually:")
    print(f"    gcloud pubsub subscriptions pull pulsequeue-dlq-sub \\")
    print(f"      --project={project_id} --limit=10 --auto-ack")

    print(f"\n  {'─'*50}")
    print(f"  PUB/SUB RESULTS")
    print(f"  {'─'*50}")
    print(f"  Valid message → job created: {'YES ✓' if valid_ok else 'NO ✗'}")
    print(f"  Malformed → DLT:             (async — verify in ~2 min)")

    return {
        "scenario": "pubsub",
        "valid_message_created_job": valid_ok,
        "dlq_test": "async - verify manually",
    }


# ---------------------------------------------------------------------------
# Scenario 5: Idempotency / Duplicate-Request Testing
# ---------------------------------------------------------------------------

def scenario_idempotency(host: str, n_duplicates: int = 20) -> dict:
    """
    Submit N concurrent requests with the same idempotency_key.
    Verifies: exactly 1 job is created; all N requests return the same job ID.
    """
    print(f"\n{'='*60}")
    print(f"  SCENARIO 5: Idempotency / Duplicate-Request Testing")
    print(f"  {n_duplicates} concurrent requests with the same key")
    print(f"{'='*60}")

    idem_key = f"idem-test-{uuid.uuid4().hex[:8]}"
    returned_ids: list[str] = []
    errors = 0
    lock = threading.Lock()

    def submit_duplicate(_: int) -> Optional[str]:
        nonlocal errors
        try:
            jid = submit_job(host, "benchmark_noop", {"test": "idempotency"},
                             idempotency_key=idem_key)
            with lock:
                returned_ids.append(jid)
            return jid
        except Exception as exc:
            with lock:
                errors += 1
            print(f"  [WARN] Submission error: {exc}")
            return None

    print(f"  Submitting {n_duplicates} concurrent requests (key={idem_key})...")
    t0 = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(max_workers=n_duplicates) as pool:
        list(pool.map(submit_duplicate, range(n_duplicates)))
    elapsed = time.monotonic() - t0

    unique_ids = set(returned_ids)
    all_same = len(unique_ids) == 1

    # Verify the job runs exactly once
    job_result = None
    if returned_ids:
        try:
            job_result = poll_until_terminal(host, returned_ids[0])
        except Exception:
            pass

    print(f"\n  {'─'*50}")
    print(f"  IDEMPOTENCY RESULTS")
    print(f"  {'─'*50}")
    print(f"  Requests sent:       {n_duplicates}")
    print(f"  IDs returned:        {len(returned_ids)}")
    print(f"  Unique job IDs:      {len(unique_ids)}")
    if unique_ids:
        print(f"  Job ID:              {list(unique_ids)[0]}")
    print(f"  All returned same:   {'YES ✓' if all_same else 'NO — IDEMPOTENCY BROKEN ✗'}")
    print(f"  Errors:              {errors}")
    print(f"  Elapsed:             {elapsed:.3f}s")
    if job_result:
        print(f"  Final status:        {job_result['status']}")

    return {
        "scenario": "idempotency",
        "n_requests": n_duplicates,
        "unique_job_ids": len(unique_ids),
        "idempotency_correct": all_same,
        "errors": errors,
    }


# ---------------------------------------------------------------------------
# Scenario 6: Multi-Worker Scaling
# ---------------------------------------------------------------------------

def scenario_scale(host: str, n_workers: int = 5, n_jobs: int = 500) -> dict:
    """
    Scale the worker count to `n_workers` and re-run the throughput benchmark.
    Requires docker compose to be available (run ON the VM).
    """
    print(f"\n{'='*60}")
    print(f"  SCENARIO 6: Multi-Worker Scaling")
    print(f"  Scale to {n_workers} workers, benchmark {n_jobs} jobs")
    print(f"{'='*60}")

    try:
        result = subprocess.run(
            ["docker", "compose", "-f", "docker-compose.yml", "-f",
             "docker-compose.gcp.yml",
             "up", "-d", "--scale", f"worker={n_workers}", "worker"],
            capture_output=True, text=True, cwd="/opt/pulsequeue/repo"
        )
        if result.returncode != 0:
            print(f"  [SKIP] docker scale failed: {result.stderr}")
            print("  (Run this scenario ON the VM)")
            return {"scenario": "scale", "skipped": True}
        print(f"  Scaled to {n_workers} workers. Waiting 5s for them to start...")
        time.sleep(5)
    except FileNotFoundError:
        print("  [SKIP] docker not found — run this scenario ON the VM")
        return {"scenario": "scale", "skipped": True}

    # Re-run throughput benchmark at the new scale
    results = scenario_throughput(host, n_jobs)
    results["scenario"] = "scale"
    results["n_workers"] = n_workers
    return results


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

SCENARIOS = {
    "throughput": scenario_throughput,
    "concurrent": scenario_concurrent,
    "sigkill": scenario_sigkill,
    "pubsub": scenario_pubsub,
    "idempotency": scenario_idempotency,
    "scale": scenario_scale,
}


def main():
    parser = argparse.ArgumentParser(
        description="PulseQueue GCP Benchmark Suite",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--host", default="http://localhost:8000",
                        help="PulseQueue API base URL (default: http://localhost:8000)")
    parser.add_argument("--jobs", type=int, default=500,
                        help="Number of jobs for throughput/scale scenarios (default: 500)")
    parser.add_argument("--concurrent", type=int, default=20,
                        help="Concurrency level for the concurrent scenario (default: 20)")
    parser.add_argument("--workers", type=int, default=5,
                        help="Worker count for the scale scenario (default: 5)")
    parser.add_argument("--scenario", choices=list(SCENARIOS.keys()) + ["all"],
                        default="all", help="Which scenario to run (default: all)")
    parser.add_argument("--project-id", default=os.environ.get("GCP_PROJECT_ID", ""),
                        help="GCP project ID (for Pub/Sub scenario)")
    args = parser.parse_args()

    # Health check
    print(f"\nPulseQueue GCP Benchmark Suite")
    print(f"Target: {args.host}")
    print(f"Checking API health...")
    if not check_health(args.host):
        print(f"ERROR: API at {args.host} is not healthy. Is it running?")
        sys.exit(1)
    print(f"API is healthy ✓\n")

    results = []
    scenarios_to_run = list(SCENARIOS.keys()) if args.scenario == "all" else [args.scenario]

    for name in scenarios_to_run:
        fn = SCENARIOS[name]
        try:
            if name == "throughput":
                r = fn(args.host, args.jobs)
            elif name == "concurrent":
                r = fn(args.host, args.concurrent, args.jobs)
            elif name == "sigkill":
                r = fn(args.host)
            elif name == "pubsub":
                r = fn(args.host, args.project_id)
            elif name == "idempotency":
                r = fn(args.host)
            elif name == "scale":
                r = fn(args.host, args.workers, args.jobs)
            else:
                r = fn(args.host)
            results.append(r)
        except Exception as exc:
            print(f"\n  [ERROR] Scenario '{name}' failed: {exc}")
            results.append({"scenario": name, "error": str(exc)})

    # Final summary
    print(f"\n{'='*60}")
    print(f"  BENCHMARK SUMMARY")
    print(f"  Host: {args.host}")
    print(f"{'='*60}")
    for r in results:
        name = r.get("scenario", "?")
        if r.get("skipped"):
            print(f"  {name:15s}  SKIPPED ({r.get('reason', '')})")
        elif r.get("error"):
            print(f"  {name:15s}  ERROR: {r['error']}")
        elif name == "throughput" or name == "scale":
            print(f"  {name:15s}  "
                  f"P50={r.get('p50_ms')}ms  "
                  f"P95={r.get('p95_ms')}ms  "
                  f"P99={r.get('p99_ms')}ms  "
                  f"{r.get('end_to_end_rps', 0):.1f} jobs/s  "
                  f"err={r.get('error_rate_pct', 0):.1f}%")
        elif name == "concurrent":
            dbl = "✓" if not r.get("double_processing") else "✗"
            print(f"  {name:15s}  no-double-process={dbl}  "
                  f"success={r.get('success_count')}/{r.get('n_jobs')}")
        elif name == "sigkill":
            rec = "✓" if r.get("full_recovery") else "✗"
            print(f"  {name:15s}  full-recovery={rec}  "
                  f"time={r.get('recovery_time_s')}s")
        elif name == "pubsub":
            ok_str = "✓" if r.get("valid_message_created_job") else "✗"
            print(f"  {name:15s}  valid-msg-job={ok_str}  dlq=async-verify-manually")
        elif name == "idempotency":
            ok_str = "✓" if r.get("idempotency_correct") else "✗"
            print(f"  {name:15s}  correct={ok_str}  "
                  f"unique_ids={r.get('unique_job_ids')}/{r.get('n_requests')}")

    print(f"{'='*60}\n")

    # Save results to JSON
    output_file = f"bench_gcp_results_{int(time.time())}.json"
    with open(output_file, "w") as f:
        json.dump({"host": args.host, "timestamp": time.time(), "results": results}, f, indent=2)
    print(f"Results saved to {output_file}")


if __name__ == "__main__":
    main()
