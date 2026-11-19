# Person 3 — Distributed Job Queue and Worker Management

A small runnable Python prototype for the Person 3 responsibilities:
- Concurrent TCP client connections
- Job submission and acknowledgment
- Worker registration and heartbeat monitoring
- FIFO queue and least-loaded eligible-worker selection
- Job status/result queries
- Bounded retry when a worker fails
- An integration point for the SDN controller

## Requirements
- Python 3.10 or newer
- No third-party packages

## Run locally

Open three or more terminals in this folder.

### Terminal 1 — start manager
```bash
python job_manager.py
```

### Terminal 2 — start worker 1
```bash
python worker.py worker-1 9001
```

### Terminal 3 — start worker 2
```bash
python worker.py worker-2 9002
```

### Terminal 4 — submit a job
```bash
python client.py
```

Choose option 1 and submit a sum job, e.g. start 1, end 100. Copy the returned job_id.
Choose option 4 to check the status/result using that ID. A job may initially be QUEUED or RUNNING; query again to see the final result.

You can run multiple clients in separate terminals to demonstrate concurrent connections.

## Supported messages
Messages are JSON objects terminated by newline (`\\n`).

Client -> manager:
```json
{"type":"submit_job","job_type":"sum","payload":{"start":1,"end":100}}
{"type":"get_status","job_id":"JOB_ID"}
{"type":"get_result","job_id":"JOB_ID"}
{"type":"list_workers"}
```

Worker -> manager:
```json
{"type":"register_worker","worker_id":"worker-1","host":"127.0.0.1","port":9001}
{"type":"heartbeat","worker_id":"worker-1","busy":false}
```

## Current limitations (be clear in the report/demo)
1. This is a prototype, not production-grade distributed execution.
2. Jobs and worker records are stored in memory and are lost if the manager stops.
3. A worker heartbeat timeout triggers retry, but the manager cannot know for certain whether the old worker completed just before losing contact. Therefore retry is at-least-once, not exactly-once. Prefer idempotent tasks.
4. The sample worker binds to `0.0.0.0` but registers `127.0.0.1`; that is correct only when all components run on the same host. For multiple hosts/Mininet, change the registered host to the worker's reachable IP address.
5. SDN controller integration is not implemented because the controller's API/event format depends on your teammate's controller. Add an adapter once that interface is agreed.
6. The worker executes only a whitelist of simple job types; it does not execute arbitrary code or shell commands.

## SDN integration contract to agree with teammate
Ask the SDN owner to expose one of:
- an event/callback when a link or path fails; or
- an HTTP/API endpoint to request a route update; or
- a small documented function your manager can call.

Agree on worker IDs/IPs, controller address, failure-event format, and how to confirm a new path is active. The job manager owns job re-queuing; the SDN controller owns network path changes.

## Suggested demo sequence
1. Start manager and two workers.
2. Submit jobs from two clients.
3. Query status and show results.
4. Stop one worker and wait longer than the heartbeat timeout.
5. Explain that running jobs are re-queued up to the retry limit.
6. If Mininet/controller integration is available, break a link and demonstrate the SDN teammate's route recovery.
