# PulseQueue — Runbook

How to run it, watch it work, inspect every layer, deliberately break it, and
change code. Written for the person who owns this system.

> **Port note for this machine.** Other containers already hold 5432, 6379 and
> 8000, so the local `.env` remaps the host side to **5433 / 6380 / 8080**.
> Every command below uses those. On a clean machine (and in the committed
> defaults) it is 5432 / 6379 / **8000** — substitute accordingly.

---

## Table of contents

1. [Prerequisites](#1-prerequisites)
2. [Three ways to run it](#2-three-ways-to-run-it)
3. [Seeing it work](#3-seeing-it-work)
4. [Inspecting PostgreSQL](#4-inspecting-postgresql)
5. [Inspecting Redis](#5-inspecting-redis)
6. [Reading the logs](#6-reading-the-logs)
7. [Deliberately triggering every behaviour](#7-deliberately-triggering-every-behaviour)
8. [Changing code and seeing the change](#8-changing-code-and-seeing-the-change)
9. [Running the tests](#9-running-the-tests)
10. [Running the load test](#10-running-the-load-test)
11. [Where things happen — a lookup table](#11-where-things-happen--a-lookup-table)
12. [Troubleshooting](#12-troubleshooting)
13. [Teardown and reset](#13-teardown-and-reset)

---

## 1. Prerequisites

| Tool | Version here | Check |
|---|---|---|
| Docker Desktop | 29.6.1 | `docker --version` |
| Docker Compose | v5.2.0 | `docker compose version` |
| Python | 3.11.9 (in `venv/`) | `.\venv\Scripts\python.exe --version` |
| Git | 2.54.0 | `git --version` |

Everything runs from the repo root:

```powershell
cd c:\Users\hitansh\pulsequeue
```

The venv already exists. To rebuild it from scratch:

```powershell
Remove-Item -Recurse -Force venv
& "$env:LOCALAPPDATA\Python\bin\python3.11.exe" -m venv venv
.\venv\Scripts\python.exe -m pip install -r requirements.txt
```

---

## 2. Three ways to run it

### Mode A — Everything in Docker (the demo mode)

This is what a recruiter or interviewer should see.

```powershell
docker compose up --build
```

Starts five containers: Postgres, Redis, the API, and **three** worker replicas.
Drop `--build` on subsequent runs when no code changed.

Detached, and wait until genuinely ready:

```powershell
docker compose up -d --build --wait
docker compose ps
```

Expect:

```
pulsequeue-postgres-1   Up (healthy)   0.0.0.0:5433->5432/tcp
pulsequeue-redis-1      Up (healthy)   0.0.0.0:6380->6379/tcp
pulsequeue-api-1        Up (healthy)   0.0.0.0:8080->8000/tcp
pulsequeue-worker-1     Up
pulsequeue-worker-2     Up
pulsequeue-worker-3     Up
```

Scale workers up or down live:

```powershell
docker compose up -d --scale worker=5
docker compose up -d --scale worker=1
```

### Mode B — Infrastructure in Docker, app on the host (the development mode)

Use this when editing code. You get uvicorn auto-reload and can attach a
debugger.

```powershell
# Terminal 1 — just the datastores
docker compose up -d postgres redis

# Terminal 2 — the API, with auto-reload
.\venv\Scripts\python.exe -m uvicorn app.main:app --reload --port 8080

# Terminal 3 — one worker
.\venv\Scripts\python.exe -m app.workers.worker

# Terminal 4 — a second worker, to watch concurrency
.\venv\Scripts\python.exe -m app.workers.worker
```

The API reloads on save. **The worker does not** — stop it with Ctrl+C and
restart after changing worker, service, or handler code.

### Mode C — Hybrid

Run the API and Postgres/Redis in Docker, and a single worker on the host so you
can breakpoint inside a handler:

```powershell
docker compose up -d postgres redis api
.\venv\Scripts\python.exe -m app.workers.worker
```

The host worker competes for the same Redis queue as any containerised ones —
that is the whole point of the design.

---

## 3. Seeing it work

### The dashboard — the thing to show people

<http://localhost:8080/dashboard/>

Five live counters and a scrolling event feed. Leave it open and submit jobs;
transitions appear within milliseconds. The header shows `● Connected` when the
WebSocket is live, and reconnects on its own if the API restarts.

### Interactive API docs

<http://localhost:8080/docs>

FastAPI generates this from the type hints and Pydantic schemas. Every endpoint
is executable from the browser — "Try it out" → Execute. Good for demonstrating
that validation is declarative.

<http://localhost:8080/redoc> is the same content in a different layout.

### Submit a job

```powershell
$job = Invoke-RestMethod -Uri "http://localhost:8080/jobs" -Method Post `
       -ContentType "application/json" `
       -Body '{"job_type":"send_email","payload":{"to":"you@example.com"}}'
$job | ConvertTo-Json
```

Watch it change:

```powershell
Invoke-RestMethod -Uri "http://localhost:8080/jobs/$($job.id)" | ConvertTo-Json
```

Poll it continuously:

```powershell
while ($true) {
  $s = Invoke-RestMethod -Uri "http://localhost:8080/jobs/$($job.id)"
  Write-Host ("{0}  status={1}  attempts={2}" -f (Get-Date -Format "HH:mm:ss"), $s.status, $s.attempts)
  if ($s.status -in @("success","dead_letter")) { break }
  Start-Sleep -Milliseconds 500
}
```

### Every endpoint

```powershell
$B = "http://localhost:8080"

Invoke-RestMethod "$B/health"
Invoke-RestMethod "$B/jobs/stats"
Invoke-RestMethod "$B/jobs?limit=10"
Invoke-RestMethod "$B/jobs?status=dead_letter"
Invoke-RestMethod "$B/jobs/$($job.id)"
Invoke-RestMethod "$B/jobs/$($job.id)/retry" -Method Post
```

### Watch the live stream from the terminal

```powershell
.\venv\Scripts\python.exe -c @"
import asyncio, websockets
async def main():
    async with websockets.connect('ws://localhost:8080/jobs/stream') as ws:
        print('connected — waiting for events')
        while True:
            print(await ws.recv())
asyncio.run(main())
"@
```

Every message is exactly what a worker published: `{"job_id": ..., "status": ...}`.

---

## 4. Inspecting PostgreSQL

Postgres is the **source of truth**. When the API and Redis disagree, Postgres
is right.

### Open a shell

```powershell
docker compose exec postgres psql -U pulse -d pulsequeue
```

Useful meta-commands inside `psql`:

```
\dt              list tables
\d jobs          describe the jobs table (columns, indexes, constraints)
\dT+ job_status  show the enum type and its values
\x               toggle expanded output (much nicer for single rows)
\q               quit
```

### One-off queries without entering the shell

```powershell
docker compose exec -T postgres psql -U pulse -d pulsequeue -c "SELECT count(*) FROM jobs;"
```

### The queries you will actually use

**Everything, most recent first:**
```sql
SELECT id, job_type, status, priority, attempts, max_attempts, updated_at
FROM jobs ORDER BY created_at DESC LIMIT 20;
```

**Counts by status — what `/jobs/stats` runs:**
```sql
SELECT status, count(*) FROM jobs GROUP BY status ORDER BY 2 DESC;
```

**Jobs that needed a retry:**
```sql
SELECT id, job_type, status, attempts, result->>'error' AS error
FROM jobs WHERE attempts > 1 ORDER BY updated_at DESC;
```

**The dead-letter queue with reasons:**
```sql
SELECT id, job_type, attempts, result->>'reason' AS reason, result->>'error' AS error
FROM jobs WHERE status = 'dead_letter';
```

**Jobs currently executing, and for how long:**
```sql
SELECT id, job_type, attempts, now() - updated_at AS running_for
FROM jobs WHERE status = 'running' ORDER BY updated_at;
```

**Exactly what the recovery sweep looks for** (60s timeout):
```sql
SELECT id, job_type, attempts, now() - updated_at AS stuck_for
FROM jobs
WHERE status = 'running' AND updated_at < now() - interval '60 seconds';
```

**End-to-end latency distribution:**
```sql
SELECT job_type,
       count(*),
       round(avg(extract(epoch FROM updated_at - created_at))::numeric, 3) AS avg_s,
       round(max(extract(epoch FROM updated_at - created_at))::numeric, 3) AS max_s
FROM jobs WHERE status = 'success' GROUP BY job_type;
```

**Confirm idempotency actually held:**
```sql
SELECT idempotency_key, count(*)
FROM jobs WHERE idempotency_key IS NOT NULL
GROUP BY idempotency_key HAVING count(*) > 1;
```
Any row returned is a bug. It should always be empty — the unique constraint
makes it structurally impossible.

**Prove an index is being used:**
```sql
EXPLAIN ANALYZE
SELECT * FROM jobs WHERE status = 'running' AND updated_at < now() - interval '60 seconds';
```
Look for `Index Scan using ix_jobs_status_updated_at`. A `Seq Scan` on a large
table means the index is not being picked.

### A GUI instead

Any Postgres client works. Connection details:

```
Host: localhost   Port: 5433
Database: pulsequeue
User: pulse       Password: pulse
```

DBeaver, pgAdmin, TablePlus, or the VS Code PostgreSQL extension.

---

## 5. Inspecting Redis

Redis is the **work queue**, not the record. It holds job *ids*, never job data.

### Open a shell

```powershell
docker compose exec redis redis-cli
```

Or one-off:

```powershell
docker compose exec -T redis redis-cli ZCARD pq:queue
```

### The four structures

```redis
KEYS pq:*                      # everything PulseQueue owns
```

**`pq:queue` — ready jobs (sorted set, lowest score first)**
```redis
ZCARD pq:queue                       # how many are waiting
ZRANGE pq:queue 0 9 WITHSCORES       # next 10 to be picked up, with scores
ZSCORE pq:queue <job-id>             # is this specific job queued?
```
Reading a score: `3000001757...` → the leading digit(s) are the priority band
(`priority × 10¹³`), the remainder is the enqueue time in epoch milliseconds.

**`pq:delayed` — jobs waiting out a retry backoff**
```redis
ZCARD pq:delayed
ZRANGE pq:delayed 0 -1 WITHSCORES    # members are "<priority>:<job-id>"
```
The score is the epoch-millisecond deadline. Convert:
```powershell
[DateTimeOffset]::FromUnixTimeMilliseconds(1757000000000).ToLocalTime()
```

**`pq:dlq` — permanently failed ids (list, newest first)**
```redis
LLEN pq:dlq
LRANGE pq:dlq 0 -1
```

**`pq:lock:recovery` — which worker is sweeping right now**
```redis
GET pq:lock:recovery       # the holder's worker id, or nil
TTL pq:lock:recovery       # seconds until it expires
```

### Watch the pub/sub channel live

```powershell
docker compose exec redis redis-cli SUBSCRIBE pq:updates
```

This is the raw feed the dashboard consumes. Submit a job in another terminal
and watch `queued` → `running` → `success` arrive.

### Watch every command the app issues

```powershell
docker compose exec redis redis-cli MONITOR
```

Extremely useful for understanding the system, and **never** for production —
it prints every command from every client. Submit one job and you will see
`ZADD`, `PUBLISH`, `BZPOPMIN`, `EVALSHA`, `SET` in order.

### Check the Lua script is cached

```redis
SCRIPT EXISTS <sha>
INFO memory
```

---

## 6. Reading the logs

```powershell
docker compose logs -f                    # everything, following
docker compose logs -f worker             # all three workers, interleaved
docker compose logs -f api
docker compose logs worker-1 --tail 50    # one specific replica
```

Every line is tagged with the component, which is what makes interleaved worker
output readable:

```
2026-08-02T21:36:22 INFO    [worker] __main__ | Processing resize_image job 06d67e58… (attempt 1/3)
2026-08-02T21:36:23 INFO    [worker] __main__ | Job 06d67e58… succeeded in 1.000s
```

### Filtering for specific events

```powershell
# retries
docker compose logs worker --no-color | Select-String "retrying in"

# dead letters
docker compose logs worker --no-color | Select-String "dead-lettered"

# crash recovery
docker compose logs worker --no-color | Select-String "Recover|reclaim|absent from Redis"

# follow one job through the whole system
docker compose logs --no-color | Select-String "06d67e58"

# which worker did what
docker compose logs worker --no-color | Select-String "Worker .* started"
```

### Turn up the volume

Set `LOG_LEVEL=DEBUG` in the compose environment and restart. To also see every
SQL statement, temporarily comment out this line in
[`app/core/logging.py`](../app/core/logging.py):

```python
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
```

Then every `SELECT`/`UPDATE` appears in the worker log. Excellent for learning
the flow, far too noisy to leave on.

---

## 7. Deliberately triggering every behaviour

This section is the one to rehearse before an interview. Each subsection makes
one guarantee visible.

### 7.1 Priority ordering

Stop the workers so nothing drains while you look:

```powershell
docker compose stop worker

# submit lowest priority first
5,4,3,2,1 | ForEach-Object {
  Invoke-RestMethod -Uri "http://localhost:8080/jobs" -Method Post `
    -ContentType "application/json" `
    -Body "{`"job_type`":`"send_email`",`"payload`":{`"p`":$_},`"priority`":$_}" | Out-Null
}

# the queue is ordered by priority, NOT by submission order
docker compose exec -T redis redis-cli ZRANGE pq:queue 0 -1 WITHSCORES

docker compose start worker
docker compose logs -f worker
```

The priority-1 job is processed first despite being submitted last.

### 7.2 FIFO within a priority band

```powershell
docker compose stop worker
1..5 | ForEach-Object {
  Invoke-RestMethod -Uri "http://localhost:8080/jobs" -Method Post `
    -ContentType "application/json" `
    -Body "{`"job_type`":`"send_email`",`"payload`":{`"n`":$_},`"priority`":3}" | Out-Null
  Start-Sleep -Milliseconds 200
}
docker compose exec -T redis redis-cli ZRANGE pq:queue 0 -1 WITHSCORES
docker compose start worker
```

Same priority → scores increase with submission time → oldest first.

### 7.3 Retry with exponential backoff

`send_email` fails ~20% of the time, so submit enough to guarantee failures:

```powershell
1..20 | ForEach-Object {
  Invoke-RestMethod -Uri "http://localhost:8080/jobs" -Method Post `
    -ContentType "application/json" `
    -Body "{`"job_type`":`"send_email`",`"payload`":{`"to`":`"u$_@t.com`"}}" | Out-Null
}
```

Watch the delayed set fill and drain — **this is the proof the worker is not
sleeping:**

```powershell
1..20 | ForEach-Object {
  $d = (docker compose exec -T redis redis-cli ZCARD pq:delayed) -join ""
  $s = Invoke-RestMethod "http://localhost:8080/jobs/stats"
  Write-Host ("delayed={0}  running={1}  failed={2}  success={3}" -f $d,$s.running,$s.failed,$s.success)
  Start-Sleep -Milliseconds 1500
}
```

`delayed > 0` while `success` keeps climbing means jobs are backing off *and*
workers are still working. See the actual delays:

```sql
SELECT id, attempts, result->>'retry_in' AS next_retry_s, result->>'error' AS error
FROM jobs WHERE status = 'failed';
```

### 7.4 Dead-letter after exhausting attempts

Force it deterministically with `max_attempts: 1`:

```powershell
1..10 | ForEach-Object {
  Invoke-RestMethod -Uri "http://localhost:8080/jobs" -Method Post `
    -ContentType "application/json" `
    -Body '{"job_type":"send_email","payload":{"to":"x@y.com"},"max_attempts":1}' | Out-Null
}
Start-Sleep -Seconds 5
Invoke-RestMethod "http://localhost:8080/jobs?status=dead_letter" | Select-Object id, attempts
docker compose exec -T redis redis-cli LLEN pq:dlq
```

Roughly two of ten fail on their single attempt and dead-letter immediately.

### 7.5 Permanent failure — unregistered job type

```powershell
$bad = Invoke-RestMethod -Uri "http://localhost:8080/jobs" -Method Post `
       -ContentType "application/json" -Body '{"job_type":"does_not_exist","payload":{}}'
Start-Sleep -Seconds 3
Invoke-RestMethod "http://localhost:8080/jobs/$($bad.id)" | ConvertTo-Json
```

`status=dead_letter`, `attempts=1`, `reason="unretryable error"`, and the error
lists the handlers that *are* registered. **One attempt, not three** — retrying a
missing handler is pointless.

### 7.6 Manual retry out of the DLQ

```powershell
$dead = (Invoke-RestMethod "http://localhost:8080/jobs?status=dead_letter")[0]
docker compose exec -T redis redis-cli LLEN pq:dlq          # before
Invoke-RestMethod "http://localhost:8080/jobs/$($dead.id)/retry" -Method Post | ConvertTo-Json
docker compose exec -T redis redis-cli LLEN pq:dlq          # after — one lower
```

`status` back to `queued`, `attempts` reset to **0**, and removed from the DLQ
list.

Now confirm the state machine is guarded:

```powershell
$ok = (Invoke-RestMethod "http://localhost:8080/jobs?status=success")[0]
try { Invoke-RestMethod "http://localhost:8080/jobs/$($ok.id)/retry" -Method Post }
catch { $_.Exception.Response.StatusCode.value__; $_.ErrorDetails.Message }
```

→ **409 Conflict**, "Only dead-lettered jobs can be manually retried".

### 7.7 Idempotency under concurrency

```powershell
$key = "demo-" + [guid]::NewGuid()
$body = "{`"job_type`":`"send_email`",`"payload`":{},`"idempotency_key`":`"$key`"}"

$jobs = 1..10 | ForEach-Object {
  Start-ThreadJob -ScriptBlock {
    param($u,$b)
    (Invoke-RestMethod -Uri $u -Method Post -ContentType "application/json" -Body $b).id
  } -ArgumentList "http://localhost:8080/jobs", $body
}
$ids = $jobs | Wait-Job | Receive-Job; $jobs | Remove-Job
$ids | Group-Object | Format-Table Count, Name
```

Ten parallel requests → **one** id, ten times, zero errors. Confirm only one row
exists:

```sql
SELECT count(*) FROM jobs WHERE idempotency_key = '<the key>';
```

### 7.8 No double-processing across workers

```powershell
docker compose up -d --scale worker=3
1..20 | ForEach-Object {
  Invoke-RestMethod -Uri "http://localhost:8080/jobs" -Method Post `
    -ContentType "application/json" `
    -Body "{`"job_type`":`"send_email`",`"payload`":{`"n`":$_}}" | Out-Null
}
Start-Sleep -Seconds 25
docker compose logs worker --no-color | Select-String "Processing" | Measure-Object
```

Then verify no job was executed twice — `attempts` should equal the number of
log lines mentioning it:

```powershell
docker compose logs worker --no-color | Select-String "Processing" |
  ForEach-Object { ($_ -split "job ")[1].Split(" ")[0] } |
  Group-Object | Where-Object { $_.Count -gt 1 }
```

Any group here must correspond to a job whose `attempts` matches — a genuine
retry, not a duplicate. Also check which worker did what:

```powershell
docker compose logs worker --no-color | Select-String "Processing" |
  ForEach-Object { ($_ -split "\|")[0] } | Group-Object | Format-Table Count, Name
```

All three should have handled roughly a third.

### 7.9 Graceful shutdown (SIGTERM)

```powershell
Invoke-RestMethod -Uri "http://localhost:8080/jobs" -Method Post `
  -ContentType "application/json" -Body '{"job_type":"generate_report","payload":{"report_id":"g"}}'
Start-Sleep -Milliseconds 500
docker compose stop worker            # sends SIGTERM
docker compose logs worker --no-color | Select-String "shut down|Signal"
```

You will see `Signal SIGTERM received — finishing current job` followed by
`shut down cleanly`. The job reaches `success`, **not** `running`. This is the
contrast with the next test.

### 7.10 Crash recovery (SIGKILL) — the important one

The compose worker config uses `VISIBILITY_TIMEOUT=60` and
`RECOVERY_INTERVAL=15`, so this takes ~75s.

```powershell
docker compose up -d worker
$j = Invoke-RestMethod -Uri "http://localhost:8080/jobs" -Method Post `
     -ContentType "application/json" -Body '{"job_type":"generate_report","payload":{"report_id":"crash"}}'

Start-Sleep -Milliseconds 700
Invoke-RestMethod "http://localhost:8080/jobs/$($j.id)" | Select-Object status, attempts
# -> running, 1

docker compose kill -s SIGKILL worker      # no graceful path at all

Start-Sleep -Seconds 3
Invoke-RestMethod "http://localhost:8080/jobs/$($j.id)" | Select-Object status, attempts
# -> STILL running. The job is orphaned.
docker compose exec -T redis redis-cli ZCARD pq:queue
# -> 0. It is not in Redis either. Nothing would ever pick it up.

docker compose up -d worker                # bring the fleet back
# wait ~75s
1..18 | ForEach-Object {
  Start-Sleep -Seconds 5
  $s = Invoke-RestMethod "http://localhost:8080/jobs/$($j.id)"
  Write-Host ("status={0} attempts={1}" -f $s.status, $s.attempts)
  if ($s.status -eq "success") { break }
}
docker compose logs worker --no-color | Select-String "Recover|reclaim"
```

Expected ending: `status=success attempts=2`, and a log line
`Recovering job … stuck in RUNNING since … — re-queueing`.

**Speed this up for a demo** by lowering `VISIBILITY_TIMEOUT` to `15` and
`RECOVERY_INTERVAL` to `5` in `docker-compose.yml`, then
`docker compose up -d worker`.

### 7.11 The orphaned-enqueue path

Simulate the API dying between `COMMIT` and `ZADD` by removing the id from Redis
by hand:

```powershell
$j = Invoke-RestMethod -Uri "http://localhost:8080/jobs" -Method Post `
     -ContentType "application/json" -Body '{"job_type":"resize_image","payload":{}}'
docker compose stop worker
docker compose exec -T redis redis-cli ZREM pq:queue $($j.id)

# Row says queued; Redis has nothing.
Invoke-RestMethod "http://localhost:8080/jobs/$($j.id)" | Select-Object status
docker compose exec -T redis redis-cli ZSCORE pq:queue $($j.id)   # nil

docker compose up -d worker
# after VISIBILITY_TIMEOUT + RECOVERY_INTERVAL:
docker compose logs worker --no-color | Select-String "absent from Redis"
```

### 7.12 Backlog absorption

```powershell
docker compose stop worker
1..500 | ForEach-Object {
  Invoke-RestMethod -Uri "http://localhost:8080/jobs" -Method Post `
    -ContentType "application/json" -Body '{"job_type":"benchmark_noop","payload":{}}' | Out-Null
}
docker compose exec -T redis redis-cli ZCARD pq:queue    # 500
Invoke-RestMethod "http://localhost:8080/jobs/stats"     # queued: 500

docker compose up -d worker
# watch it drain
1..30 | ForEach-Object {
  $q = (docker compose exec -T redis redis-cli ZCARD pq:queue) -join ""
  Write-Host "queue=$q"
  if ($q -eq "0") { break }
  Start-Sleep -Seconds 1
}
```

The API stayed responsive throughout. That is the point of a queue.

---

## 8. Changing code and seeing the change

### The rebuild matrix

| What you changed | What to do |
|---|---|
| API code, Mode B | Nothing — uvicorn `--reload` handles it |
| API code, Mode A | `docker compose up -d --build api` |
| Worker/service/handler code, Mode B | Ctrl+C the worker, restart it |
| Worker/service/handler code, Mode A | `docker compose up -d --build worker` |
| `dashboard/index.html` | Hard-refresh the browser (Ctrl+Shift+R) |
| `requirements.txt` | `docker compose build --no-cache` + reinstall in the venv |
| `docker-compose.yml` | `docker compose up -d` |
| `.env` port values | `docker compose down` then `up -d` |
| Anything in `app/models/job.py` | See below — **this is the sharp edge** |

### Schema changes are the one thing that bites

`create_all()` creates **missing tables**. It does **not** alter existing ones.
Add a column to the model and nothing happens; the app then errors because the
ORM expects a column the table lacks.

For local development, drop and recreate:

```powershell
docker compose exec -T postgres psql -U pulse -d pulsequeue -c "DROP TABLE jobs; DROP TYPE job_status;"
docker compose restart api          # init_db() recreates on startup
```

Nuclear option (also wipes the volume):

```powershell
docker compose down -v
docker compose up -d --build
```

This limitation is exactly what Alembic would solve, and is why it is the top
item in Future Improvements.

### Adding a new job type — the whole workflow

This demonstrates the plugin registry. Append to
[`handlers/builtin.py`](../handlers/builtin.py):

```python
@job_handler("process_payment")
def process_payment(payload: dict[str, Any]) -> dict:
    amount = payload.get("amount", 0)
    if amount <= 0:
        raise ValueError(f"Invalid amount: {amount}")
    time.sleep(0.3)
    return {"charged": amount, "currency": payload.get("currency", "INR")}
```

Then:

```powershell
docker compose up -d --build worker
docker compose logs worker --tail 5 --no-color | Select-String "handlers:"
# -> handlers: ['benchmark_noop', 'generate_report', 'process_payment', 'resize_image', 'send_email']

Invoke-RestMethod -Uri "http://localhost:8080/jobs" -Method Post `
  -ContentType "application/json" `
  -Body '{"job_type":"process_payment","payload":{"amount":499,"currency":"INR"}}'
```

**Files changed: one.** No worker change, no registry change, no router change,
no schema change. That is the Open/Closed Principle, demonstrable in 60 seconds.

### Attaching a debugger

In Mode B, run the worker under your IDE's debugger, or add:

```python
import pdb; pdb.set_trace()
```

inside a handler. The worker is an ordinary synchronous Python process, so
stepping through `process_job` works normally — one reason the worker was kept
synchronous rather than async.

---

## 9. Running the tests

```powershell
# everything (needs Postgres)
.\venv\Scripts\python.exe -m pytest tests/ -v

# only the 71 that need no infrastructure
.\venv\Scripts\python.exe -m pytest tests/ -m "not integration" -v

# one file
.\venv\Scripts\python.exe -m pytest tests/test_retry_service.py -v

# one test
.\venv\Scripts\python.exe -m pytest tests/test_retry_service.py::test_compute_delay_exponential -v

# stop at the first failure, and drop into a debugger
.\venv\Scripts\python.exe -m pytest tests/ -x --pdb

# see print output
.\venv\Scripts\python.exe -m pytest tests/ -s

# what CI runs — a skip becomes a failure
$env:REQUIRE_INTEGRATION_TESTS="1"
.\venv\Scripts\python.exe -m pytest tests/ -v
Remove-Item Env:\REQUIRE_INTEGRATION_TESTS
```

Prove the infrastructure independence claim — point at a database that does not
exist and watch 71 tests still pass:

```powershell
$env:DATABASE_URL="postgresql://pulse:pulse@localhost:1/nope"
.\venv\Scripts\python.exe -m pytest tests/ -q
Remove-Item Env:\DATABASE_URL
```

---

## 10. Running the load test

```powershell
docker compose up -d --build

# clean slate so the numbers mean something
docker compose exec -T postgres psql -U pulse -d pulsequeue -c "TRUNCATE jobs;"
docker compose exec -T redis redis-cli FLUSHDB

.\venv\Scripts\python.exe -m locust -f locustfile.py `
  --host=http://localhost:8080 `
  --users=50 --spawn-rate=5 --run-time=120s `
  --headless --csv=load_test_results
```

Reads: `load_test_results_stats.csv` (throughput and percentiles),
`load_test_results_failures.csv`.

**With the web UI** (live charts — better for a demo):

```powershell
.\venv\Scripts\python.exe -m locust -f locustfile.py --host=http://localhost:8080
# then open http://localhost:8089
```

**Measuring true system throughput** — no simulated I/O:

```powershell
docker compose exec -T redis redis-cli FLUSHDB
docker compose exec -T postgres psql -U pulse -d pulsequeue -c "TRUNCATE jobs;"
docker compose stop worker

# enqueue 3000 jobs with nothing draining
$jobs = 1..12 | ForEach-Object {
  Start-ThreadJob -ScriptBlock {
    param($u)
    for ($i=0; $i -lt 250; $i++) {
      Invoke-RestMethod -Uri $u -Method Post -ContentType "application/json" `
        -Body '{"job_type":"benchmark_noop","payload":{}}' | Out-Null
    }
  } -ArgumentList "http://localhost:8080/jobs"
}
$jobs | Wait-Job | Out-Null; $jobs | Remove-Job

# start the fleet and time the drain
$sw = [Diagnostics.Stopwatch]::StartNew()
docker compose up -d worker
while ($true) {
  $q = [int]((docker compose exec -T redis redis-cli ZCARD pq:queue) -join "")
  if ($q -eq 0) { break }
  Start-Sleep -Seconds 2
}
$sw.Stop()
$done = (Invoke-RestMethod "http://localhost:8080/jobs/stats").success
"$done jobs in $($sw.Elapsed.TotalSeconds)s = $([math]::Round($done/$sw.Elapsed.TotalSeconds,1)) jobs/sec"
```

**Killing a worker mid-load** — the reliability test under real conditions:

```powershell
# start locust in one terminal, then:
docker compose kill -s SIGKILL worker-1
Start-Sleep -Seconds 30
docker compose up -d worker
# afterwards confirm nothing was lost:
Invoke-RestMethod "http://localhost:8080/jobs/stats"
```

`queued + running` should reach 0 and `success + dead_letter` should equal the
total submitted.

---

## 11. Where things happen — a lookup table

When you need to answer "where does X happen?", start here.

| Question | File |
|---|---|
| Where is a setting read? | [`app/core/config.py`](../app/core/config.py) |
| Where is the Redis connection made? | [`app/core/redis.py`](../app/core/redis.py) |
| Where is the DB pool configured? | [`app/core/database.py`](../app/core/database.py) |
| Where is the table created? | `init_db()` in `core/database.py`, called from `main.py` lifespan |
| Where is the table defined? | [`app/models/job.py`](../app/models/job.py) |
| Where is the API contract? | [`app/schemas/job.py`](../app/schemas/job.py) |
| Where is **all** the SQL? | [`app/repositories/job_repository.py`](../app/repositories/job_repository.py) |
| Where is **all** the Redis? | [`app/services/queue_service.py`](../app/services/queue_service.py) |
| Where is a job enqueued? | `QueueService.enqueue`, called by `JobService.submit` |
| Where is a job dequeued? | `QueueService.dequeue` (`BZPOPMIN`), called by the worker loop |
| Where is priority computed? | `QueueService._job_score` |
| Where is backoff computed? | `RetryService.compute_delay` |
| Where is retry-vs-dead-letter decided? | `RetryService.handle_failure` |
| Where is a delayed job promoted? | `QueueService.promote_due` (Lua) |
| Where is idempotency enforced? | `JobService.submit` **and** the DB unique constraint |
| Where is a crashed job reclaimed? | `RecoveryService.requeue_stuck_jobs` |
| Where is an orphaned enqueue fixed? | `RecoveryService.requeue_orphaned_jobs` |
| Where is a handler looked up? | `job_registry.get_handler`, called by `worker.process_job` |
| Where are handlers defined? | [`handlers/builtin.py`](../handlers/builtin.py) |
| Where does a job actually execute? | `Worker.process_job` → `handler(job.payload)` |
| Where is SIGTERM handled? | `Worker._install_signal_handlers` |
| Where are stats computed? | `JobRepository.count_by_status` → `MetricsService.get_stats` |
| Where is an update published? | `QueueService.publish_update` |
| Where does the WebSocket subscribe? | [`app/websocket/stream.py`](../app/websocket/stream.py) |
| Where is a domain error mapped to HTTP? | [`app/api/routers/jobs.py`](../app/api/routers/jobs.py) |
| Where are services constructed? | [`app/api/dependencies.py`](../app/api/dependencies.py) |
| Where is everything wired together? | [`app/main.py`](../app/main.py) |

### What happens when — the timeline

| Time | Event | Where |
|---|---|---|
| t=0 | Request validated | Pydantic, before your code |
| t≈5ms | Row committed | `JobRepository.create` |
| t≈6ms | `ZADD pq:queue` | `QueueService.enqueue` |
| t≈7ms | 201 returned | Router |
| t≈30ms | A worker's `BZPOPMIN` returns | `Worker.run` |
| t≈35ms | `attempts=1`, status RUNNING | `process_job` |
| t≈35ms→ | Handler executes | `handlers/builtin.py` |
| on return | status SUCCESS + result | `process_job` |
| on raise | `RetryService.handle_failure` | delayed set or DLQ |
| +delay | Promotion back to the ready queue | `promote_due`, any worker |
| every 15–30s | Recovery sweep, one worker only | `_maybe_run_recovery` |
| on SIGTERM | Finish current job, exit | signal handler |
| after visibility timeout | Orphans reclaimed | `RecoveryService` |

---

## 12. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `Bind for 0.0.0.0:6379 failed: port is already allocated` | Another container holds the port | Set `REDIS_PORT` in `.env`, or `docker ps --filter publish=6379` to find the culprit |
| Jobs stay `queued` forever | No worker running | `docker compose ps`; `docker compose up -d worker` |
| Jobs stay `queued`, workers *are* running | Worker pointed at a different Redis | Compare `REDIS_URL`; check `ZCARD pq:queue` is non-zero |
| `status=dead_letter`, `reason="unretryable error"` | Unregistered `job_type` | Check the error's `Available:` list; rebuild the worker after adding a handler |
| API 500 on submit | Postgres unreachable | `docker compose logs api`; `docker compose ps postgres` |
| `pytest` fails, `connection refused` | Postgres not up | `docker compose up -d postgres redis` |
| `pytest` skips 26 tests | Same, and it is *intentional* | Only a problem if `REQUIRE_INTEGRATION_TESTS=1` |
| Dashboard shows `○ Disconnected` | API restarted, or WS blocked | It self-reconnects; check `docker compose logs api` |
| Dashboard counters frozen but feed moves | `/jobs/stats` failing | Open <http://localhost:8080/jobs/stats> directly |
| Jobs stuck in `running` | Worker died; visibility timeout not yet elapsed | Wait `VISIBILITY_TIMEOUT + RECOVERY_INTERVAL`, then check logs for `Recovering` |
| Jobs stuck in `running` **and** no recovery log | Recovery lock held by a dead worker | `TTL pq:lock:recovery`; it self-expires |
| Worker restart loop | Bad `DATABASE_URL`, or Postgres not healthy | `docker compose logs worker` |
| `ModuleNotFoundError: app` | Wrong working directory | Run from the repo root |
| Column-does-not-exist error | Model changed; `create_all` does not ALTER | Drop the table (see §8) |
| Redis has jobs, DB has none | Database was truncated but Redis was not | `FLUSHDB` too — keep them in sync |
| Two workers seem to run the same job | Almost certainly a retry, not a duplicate | Compare `attempts` against the log-line count |

---

## 13. Teardown and reset

```powershell
docker compose stop                # pause, keep data
docker compose down                # remove containers, KEEP the volume
docker compose down -v             # remove containers AND wipe the database
```

Reset data without touching containers:

```powershell
docker compose exec -T postgres psql -U pulse -d pulsequeue -c "TRUNCATE jobs;"
docker compose exec -T redis redis-cli FLUSHDB
```

**Always do both together.** Truncating Postgres while leaving Redis full leaves
job ids on the queue with no rows behind them — the worker logs
`Job … not found in the database — skipping` and moves on, which is correct
behaviour but confusing to watch.

Full clean rebuild:

```powershell
docker compose down -v
docker system prune -f
docker compose up -d --build --wait
```
