"""Locust load test.

Two user classes, measuring two different things:

    JobSubmitter      — API throughput. How many jobs/sec can be accepted, and
                        at what submit latency? This is the number that answers
                        "can the front door keep up".

    JobLifecycleUser  — end-to-end completion latency. Submit a job, then poll
                        until it reaches a terminal state, and record the total.
                        This is the number that answers "how long until the work
                        is actually done", which is what a caller cares about
                        and what the project spec asks to be reported.

Reporting only the first would be misleading: accepting 500 requests/sec means
nothing if the workers are hours behind.

Run against a local stack::

    locust -f locustfile.py --host=http://localhost:8000 \
           --users=50 --spawn-rate=5 --run-time=120s --headless \
           --csv=load_test_results

Against the deployed instance, swap --host for the public URL.
"""

import random
import time

from locust import HttpUser, between, events, task

TERMINAL_STATUSES = {"success", "dead_letter"}


class JobSubmitter(HttpUser):
    """Measures submit throughput and API latency."""

    wait_time = between(0.1, 0.5)
    weight = 4

    @task(3)
    def submit_email_job(self):
        """Submit a high-frequency email job."""
        self.client.post(
            "/jobs",
            json={
                "job_type": "send_email",
                "payload": {"to": f"user{random.randint(1, 9999)}@test.com"},
                "priority": random.randint(1, 5),
            },
            name="POST /jobs",
        )

    @task(1)
    def check_stats(self):
        """Periodically check system stats — the dashboard's read path."""
        self.client.get("/jobs/stats", name="GET /jobs/stats")


class JobLifecycleUser(HttpUser):
    """Measures submit-to-completion latency, reported as 'job completion'.

    Uses resize_image rather than send_email: send_email fails ~20% of the time
    by design, and a job that spends 2-8s in retry backoff would swamp the
    latency distribution with the failure-injection rate rather than the
    system's actual processing time. Failure behaviour is measured separately by
    killing a worker mid-run.
    """

    wait_time = between(1, 3)
    weight = 1

    # Give up waiting after this long and record a failure, rather than letting
    # one wedged job hold a user open for the whole run.
    completion_timeout = 60.0
    poll_interval = 0.5

    @task
    def submit_and_await_completion(self):
        started = time.monotonic()

        with self.client.post(
            "/jobs",
            json={"job_type": "resize_image", "payload": {"width": 320}},
            name="POST /jobs (lifecycle)",
            catch_response=True,
        ) as response:
            if response.status_code != 201:
                response.failure(f"submit returned {response.status_code}")
                return
            job_id = response.json()["id"]

        while time.monotonic() - started < self.completion_timeout:
            time.sleep(self.poll_interval)
            poll = self.client.get(
                f"/jobs/{job_id}",
                name="GET /jobs/{id} (poll)",
                catch_response=True,
            )
            with poll:
                if poll.status_code != 200:
                    poll.failure(f"poll returned {poll.status_code}")
                    return
                status = poll.json()["status"]

            if status in TERMINAL_STATUSES:
                elapsed_ms = (time.monotonic() - started) * 1000
                events.request.fire(
                    request_type="JOB",
                    name=f"completion ({status})",
                    response_time=elapsed_ms,
                    response_length=0,
                    exception=None,
                    context={},
                )
                return

        events.request.fire(
            request_type="JOB",
            name="completion (timeout)",
            response_time=self.completion_timeout * 1000,
            response_length=0,
            exception=TimeoutError(
                f"job {job_id} did not finish within {self.completion_timeout}s"
            ),
            context={},
        )
