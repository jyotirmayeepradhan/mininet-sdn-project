
"""
Jackfruit SDN-Socket Job Manager
Features:
- TCP JSON-lines communication
- Multiple client hosts with source IP identification
- Complete job history and timestamps
- Worker registration and heartbeat monitoring
- Automatic retry after worker failure
- SDN flow installation through sdn_adapter
"""

import json
import socket
import threading
import time
import uuid
import sys
from pathlib import Path
from queue import Queue, Empty
from datetime import datetime

# Locate the nih folder relative to this file.
PROJECT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_DIR / "nih"))

try:
    from sdn_adapter import install_worker_flow
except ImportError:
    install_worker_flow = None

HOST = "10.0.0.1"
PORT = 5000

HEARTBEAT_TIMEOUT = 8.0
MONITOR_INTERVAL = 2.0
MAX_ATTEMPTS = 3
MAX_EVENTS = 500

job_queue = Queue()
jobs = {}
workers = {}
events = []

lock = threading.RLock()
stopping = threading.Event()


# -------------------- COMMON UTILITIES --------------------

def now():
    return time.time()


def timestamp(value=None):
    value = value or now()
    return datetime.fromtimestamp(value).strftime("%Y-%m-%d %H:%M:%S")


def log_event(event_type, message, **details):
    event = {
        "time": now(),
        "timestamp": timestamp(),
        "type": event_type,
        "message": message,
        "details": details
    }

    with lock:
        events.append(event)
        if len(events) > MAX_EVENTS:
            del events[:-MAX_EVENTS]

    print(f"[{event['timestamp']}] [{event_type}] {message}", flush=True)


def send_json_line(sock, obj):
    data = (json.dumps(obj, separators=(",", ":")) + "\n").encode()
    sock.sendall(data)


def recv_json_lines(sock):
    buffer = b""

    while True:
        chunk = sock.recv(4096)
        if not chunk:
            break

        buffer += chunk

        while b"\n" in buffer:
            line, buffer = buffer.split(b"\n", 1)
            if line.strip():
                yield json.loads(line.decode("utf-8"))


# -------------------- PUBLIC DATA --------------------

def public_job(job):
    return {
        "job_id": job["job_id"],
        "job_type": job["job_type"],
        "payload": job["payload"],
        "status": job["status"],
        "worker_id": job.get("worker_id"),
        "attempts": job["attempts"],
        "result": job.get("result"),
        "error": job.get("error"),
        "client_ip": job.get("client_ip"),
        "client_host": job.get("client_host"),
        "created_at": job["created_at"],
        "created_time": timestamp(job["created_at"]),
        "updated_at": job["updated_at"],
        "updated_time": timestamp(job["updated_at"]),
        "completed_at": job.get("completed_at"),
        "history": list(job.get("history", []))
    }


def public_workers():
    current = now()
    result = {}

    for worker_id, worker in workers.items():
        result[worker_id] = {
            "host": worker["host"],
            "port": worker["port"],
            "alive": worker["alive"],
            "busy": worker["busy"],
            "active_jobs": worker["active_jobs"],
            "last_heartbeat": worker["last_heartbeat"],
            "last_heartbeat_age": round(
                current - worker["last_heartbeat"], 2
            )
        }

    return result


# -------------------- JOB VALIDATION --------------------

def validate_job(job_type, payload):
    if job_type not in {"sum", "factorial", "max"}:
        raise ValueError("Use sum, factorial, or max.")

    if not isinstance(payload, dict):
        raise ValueError("Payload must be a JSON object.")

    if job_type == "sum":
        start = payload.get("start")
        end = payload.get("end")

        if (type(start) is not int or type(end) is not int
                or start > end):
            raise ValueError("sum requires integer start/end with start <= end.")

    elif job_type == "factorial":
        n = payload.get("n")

        if type(n) is not int or n < 0 or n > 500:
            raise ValueError("factorial requires integer n from 0 to 500.")

    elif job_type == "max":
        values = payload.get("values")

        if (not isinstance(values, list) or not values
                or not all(type(x) in (int, float) for x in values)):
            raise ValueError("max requires a non-empty numeric list.")


def add_job(message, client_ip, client_host=None):
    job_type = message.get("job_type")
    payload = message.get("payload")

    validate_job(job_type, payload)

    job_id = str(uuid.uuid4())
    created = now()

    record = {
        "job_id": job_id,
        "job_type": job_type,
        "payload": payload,
        "status": "QUEUED",
        "worker_id": None,
        "attempts": 0,
        "result": None,
        "error": None,
        "client_ip": client_ip,
        "client_host": client_host or message.get("client_host"),
        "created_at": created,
        "updated_at": created,
        "completed_at": None,
        "history": [{
            "time": created,
            "event": "SUBMITTED",
            "worker_id": None,
            "message": f"Submitted by {client_ip}"
        }]
    }

    with lock:
        jobs[job_id] = record

    job_queue.put(job_id)

    log_event(
        "JOB_SUBMITTED",
        f"{job_type} job submitted by {client_ip}",
        job_id=job_id,
        client_ip=client_ip,
        job_type=job_type
    )

    return job_id


def add_job_history(job, event, message, worker_id=None):
    entry = {
        "time": now(),
        "event": event,
        "worker_id": worker_id,
        "message": message
    }

    job["history"].append(entry)
    job["updated_at"] = entry["time"]


# -------------------- WORKER SCHEDULING --------------------

def choose_idle_worker():
    candidates = [
        (worker_id, worker)
        for worker_id, worker in workers.items()
        if worker["alive"]
        and not worker["busy"]
        and worker.get("host")
        and worker.get("port")
    ]

    if not candidates:
        return None

    candidates.sort(
        key=lambda item: (
            item[1].get("active_jobs", 0),
            item[0]
        )
    )

    return candidates[0]


def dispatch_loop():
    while not stopping.is_set():
        try:
            job_id = job_queue.get(timeout=0.5)
        except Empty:
            continue

        try:
            with lock:
                job = jobs.get(job_id)

                if not job or job["status"] in {"COMPLETED", "FAILED"}:
                    continue

                selected = choose_idle_worker()

                if selected is None:
                    job_queue.put(job_id)
                    time.sleep(0.5)
                    continue

                worker_id, worker = selected

                job["status"] = "RUNNING"
                job["worker_id"] = worker_id
                job["attempts"] += 1

                attempt = job["attempts"]
                host = worker["host"]
                port = worker["port"]

                worker["busy"] = True
                worker["active_jobs"] += 1

                add_job_history(
                    job,
                    "ASSIGNED",
                    f"Assigned to {worker_id} ({host}:{port}), attempt {attempt}",
                    worker_id
                )

                payload = {
                    "type": "execute_job",
                    "job_id": job_id,
                    "attempt": attempt,
                    "job_type": job["job_type"],
                    "payload": job["payload"]
                }

            log_event(
                "JOB_ASSIGNED",
                f"Job assigned to {worker_id}",
                job_id=job_id,
                worker_id=worker_id,
                worker_ip=host,
                attempt=attempt
            )

            # Install the actual OpenFlow forwarding rule.
            if install_worker_flow:
                try:
                    switch_port = {
                        "10.0.0.2": 2,
                        "10.0.0.3": 3
                    }.get(str(host))

                    if switch_port is not None:
                        install_worker_flow(str(host), switch_port)

                        log_event(
                            "SDN_FLOW",
                            f"Flow installed for {worker_id} via switch port {switch_port}",
                            worker_id=worker_id,
                            worker_ip=host,
                            switch_port=switch_port
                        )

                except Exception as exc:
                    log_event(
                        "SDN_WARNING",
                        f"Flow installation failed: {exc}",
                        worker_id=worker_id
                    )

            with socket.create_connection((host, port), timeout=3) as worker_sock:
                send_json_line(worker_sock, payload)

            log_event(
                "JOB_DISPATCHED",
                f"Job sent to {worker_id}",
                job_id=job_id,
                worker_id=worker_id
            )

        except Exception as exc:
            with lock:
                worker = workers.get(worker_id) if "worker_id" in locals() else None

                if worker:
                    worker["alive"] = False
                    worker["busy"] = False
                    worker["active_jobs"] = 0

                job = jobs.get(job_id)

                if job and job["status"] == "RUNNING":
                    job["worker_id"] = None
                    job["error"] = str(exc)

                    if job["attempts"] < MAX_ATTEMPTS:
                        job["status"] = "QUEUED"
                        add_job_history(
                            job, "DISPATCH_FAILED",
                            str(exc), worker_id if "worker_id" in locals() else None
                        )
                        job_queue.put(job_id)
                    else:
                        job["status"] = "FAILED"
                        job["completed_at"] = now()
                        add_job_history(job, "FAILED", str(exc))

            log_event(
                "DISPATCH_ERROR",
                f"Dispatch failed: {exc}",
                job_id=job_id
            )

        finally:
            job_queue.task_done()


# -------------------- FAILURE RECOVERY --------------------

def mark_worker_failure(worker_id, reason):
    with lock:
        worker = workers.get(worker_id)

        if not worker or not worker["alive"]:
            return

        worker["alive"] = False
        worker["busy"] = False
        worker["active_jobs"] = 0

        log_event(
            "WORKER_FAILED",
            f"{worker_id} unavailable: {reason}",
            worker_id=worker_id
        )

        for job in jobs.values():
            if job["worker_id"] == worker_id and job["status"] == "RUNNING":
                job["worker_id"] = None
                job["error"] = reason

                if job["attempts"] < MAX_ATTEMPTS:
                    job["status"] = "QUEUED"

                    add_job_history(
                        job,
                        "RETRY_QUEUED",
                        f"{worker_id} failed. Job queued for reassignment.",
                        worker_id
                    )

                    job_queue.put(job["job_id"])

                    log_event(
                        "JOB_REQUEUED",
                        f"Job requeued after {worker_id} failure",
                        job_id=job["job_id"],
                        failed_worker=worker_id
                    )

                else:
                    job["status"] = "FAILED"
                    job["completed_at"] = now()
                    add_job_history(job, "FAILED", reason, worker_id)


def monitor_loop():
    while not stopping.wait(MONITOR_INTERVAL):
        current = now()
        stale = []

        with lock:
            for worker_id, worker in workers.items():
                if (worker["alive"]
                        and current - worker["last_heartbeat"] > HEARTBEAT_TIMEOUT):
                    stale.append(worker_id)

        for worker_id in stale:
            mark_worker_failure(
                worker_id,
                f"Heartbeat timeout after {HEARTBEAT_TIMEOUT:.0f}s"
            )


# -------------------- WORKER MESSAGES --------------------

def handle_worker_message(message):
    kind = message.get("type")
    worker_id = message.get("worker_id")

    if kind == "register_worker":
        if not worker_id:
            raise ValueError("worker_id is required.")

        host = message.get("host", "127.0.0.1")
        port = int(message["port"])

        with lock:
            workers[worker_id] = {
                "host": host,
                "port": port,
                "alive": True,
                "busy": False,
                "active_jobs": 0,
                "last_heartbeat": now()
            }

        log_event(
            "WORKER_REGISTERED",
            f"{worker_id} registered at {host}:{port}",
            worker_id=worker_id,
            host=host,
            port=port
        )

        return {"type": "registered", "worker_id": worker_id}

    if kind == "heartbeat":
        with lock:
            worker = workers.get(worker_id)

            if not worker:
                return {
                    "type": "error",
                    "error": "Worker is not registered."
                }

            worker["alive"] = True
            worker["last_heartbeat"] = now()
            worker["busy"] = bool(message.get("busy", worker["busy"]))

        return {"type": "heartbeat_ack", "worker_id": worker_id}

    if kind == "job_result":
        job_id = message.get("job_id")
        attempt = message.get("attempt")

        with lock:
            worker = workers.get(worker_id)

            if worker:
                worker["busy"] = False
                worker["active_jobs"] = max(
                    0, worker["active_jobs"] - 1
                )
                worker["last_heartbeat"] = now()
                worker["alive"] = True

            job = jobs.get(job_id)

            if not job:
                return {"type": "error", "error": "Unknown job_id."}

            # Ignore late responses from an earlier attempt.
            if (job["status"] != "RUNNING"
                    or job["attempts"] != attempt
                    or job["worker_id"] != worker_id):
                return {
                    "type": "result_ignored",
                    "job_id": job_id
                }

            if message.get("status") == "completed":
                job["status"] = "COMPLETED"
                job["result"] = message.get("result")
                job["error"] = None
                job["completed_at"] = now()

                add_job_history(
                    job,
                    "COMPLETED",
                    f"Completed by {worker_id}",
                    worker_id
                )

                log_event(
                    "JOB_COMPLETED",
                    f"Job completed by {worker_id}",
                    job_id=job_id,
                    worker_id=worker_id
                )

            else:
                error = message.get("error", "Worker reported failure.")
                job["error"] = error
                job["worker_id"] = None

                if job["attempts"] < MAX_ATTEMPTS:
                    job["status"] = "QUEUED"

                    add_job_history(
                        job, "RETRY_QUEUED", error, worker_id
                    )

                    job_queue.put(job_id)

                else:
                    job["status"] = "FAILED"
                    job["completed_at"] = now()
                    add_job_history(job, "FAILED", error, worker_id)

                log_event(
                    "JOB_ERROR",
                    f"{job_id}: {error}",
                    job_id=job_id,
                    worker_id=worker_id
                )

        return {"type": "result_ack", "job_id": job_id}

    return {
        "type": "error",
        "error": f"Unknown worker message type: {kind}"
    }


# -------------------- CLIENT MESSAGES --------------------

def handle_client_message(message, client_ip):
    kind = message.get("type")

    if kind == "submit_job":
        job_id = add_job(
            message,
            client_ip,
            message.get("client_host")
        )

        return {
            "type": "ack",
            "job_id": job_id,
            "status": "QUEUED"
        }

    if kind in {"get_status", "get_result"}:
        job_id = message.get("job_id")

        with lock:
            job = jobs.get(job_id)

            if not job:
                return {"type": "error", "error": "Unknown job_id."}

            return {"type": "job_info", **public_job(job)}

    if kind == "list_jobs":
        with lock:
            all_jobs = [
                public_job(job)
                for job in sorted(
                    jobs.values(),
                    key=lambda item: item["created_at"],
                    reverse=True
                )
            ]

        return {"type": "jobs", "jobs": all_jobs}

    if kind == "list_workers":
        with lock:
            return {
                "type": "workers",
                "workers": public_workers()
            }

    if kind == "list_events":
        with lock:
            return {
                "type": "events",
                "events": list(events)
            }

    if kind == "manager_status":
        with lock:
            return {
                "type": "manager_status",
                "status": "ONLINE",
                "job_count": len(jobs),
                "worker_count": len(workers)
            }

    return {
        "type": "error",
        "error": f"Unknown client message type: {kind}"
    }


# -------------------- CONNECTION HANDLING --------------------

def connection_thread(conn, address):
    client_ip = address[0]

    with conn:
        try:
            for message in recv_json_lines(conn):
                try:
                    kind = message.get("type")

                    if kind in {
                        "register_worker",
                        "heartbeat",
                        "job_result"
                    }:
                        response = handle_worker_message(message)
                    else:
                        response = handle_client_message(
                            message, client_ip
                        )

                except (
                    ValueError,
                    TypeError,
                    KeyError,
                    json.JSONDecodeError
                ) as exc:
                    response = {
                        "type": "error",
                        "error": str(exc)
                    }

                send_json_line(conn, response)

        except (ConnectionError, OSError, json.JSONDecodeError) as exc:
            print(
                f"[CONNECTION] {address} closed/error: {exc}",
                flush=True
            )


# -------------------- MAIN SERVER --------------------

def main():
    threading.Thread(
        target=dispatch_loop,
        daemon=True
    ).start()

    threading.Thread(
        target=monitor_loop,
        daemon=True
    ).start()

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen(50)

    log_event(
        "MANAGER_STARTED",
        f"Manager listening on {HOST}:{PORT}"
    )

    print("[MANAGER] Press Ctrl+C to stop.", flush=True)

    try:
        while True:
            conn, address = server.accept()

            threading.Thread(
                target=connection_thread,
                args=(conn, address),
                daemon=True
            ).start()

    except KeyboardInterrupt:
        print("\n[MANAGER] Shutting down...", flush=True)

    finally:
        stopping.set()
        server.close()


if __name__ == "__main__":
    main()

