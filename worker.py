
"""
Jackfruit SDN-Socket Worker
- Registers with the job manager
- Sends periodic heartbeats
- Receives and executes assigned jobs
- Reports results with job ID and attempt number
"""

import json
import math
import socket
import sys
import threading
import time
import signal

WORKER_ID = sys.argv[1] if len(sys.argv) > 1 else "worker-1"
WORKER_PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 9001
WORKER_HOST = sys.argv[3] if len(sys.argv) > 3 else "127.0.0.1"

MANAGER_HOST = sys.argv[4] if len(sys.argv) > 4 else "127.0.0.1"
MANAGER_PORT = 5000

HEARTBEAT_INTERVAL = 2.0

busy_lock = threading.Lock()
busy = False
stop_event = threading.Event()


def send_json_line(sock, obj):
    data = (json.dumps(obj, separators=(",", ":")) + "\n").encode("utf-8")
    sock.sendall(data)


def request_manager(message):
    with socket.create_connection(
        (MANAGER_HOST, MANAGER_PORT), timeout=3
    ) as sock:
        send_json_line(sock, message)

        with sock.makefile("r", encoding="utf-8") as reader:
            line = reader.readline()

        if not line:
            raise ConnectionError("Manager closed connection without replying.")

        return json.loads(line)


def heartbeat_loop():
    while not stop_event.wait(HEARTBEAT_INTERVAL):
        with busy_lock:
            current_busy = busy

        try:
            response = request_manager({
                "type": "heartbeat",
                "worker_id": WORKER_ID,
                "busy": current_busy
            })

            if response.get("type") != "heartbeat_ack":
                print(f"[{WORKER_ID}] Unexpected heartbeat response: {response}",
                      flush=True)

        except (OSError, ConnectionError, ValueError) as exc:
            print(f"[{WORKER_ID}] Heartbeat failed: {exc}", flush=True)


def execute(job_type, payload):
    if job_type == "sum":
        return sum(range(payload["start"], payload["end"] + 1))

    if job_type == "factorial":
        return math.factorial(payload["n"])

    if job_type == "max":
        return max(payload["values"])

    raise ValueError(f"Unsupported job type: {job_type}")


def report_result(job_id, attempt, status, result=None, error=None):
    message = {
        "type": "job_result",
        "worker_id": WORKER_ID,
        "job_id": job_id,
        "attempt": attempt,
        "status": status
    }

    if status == "completed":
        message["result"] = result
    else:
        message["error"] = error or "Unknown worker error."

    try:
        response = request_manager(message)
        print(
            f"[{WORKER_ID}] Result response for {job_id}: {response}",
            flush=True
        )

    except (OSError, ConnectionError, ValueError) as exc:
        print(
            f"[{WORKER_ID}] Could not report result: {exc}",
            flush=True
        )


def handle_assignment(conn):
    global busy

    with conn:
        try:
            with conn.makefile("r", encoding="utf-8") as reader:
                line = reader.readline()

            if not line:
                return

            message = json.loads(line)

        except (json.JSONDecodeError, OSError) as exc:
            print(f"[{WORKER_ID}] Invalid assignment: {exc}", flush=True)
            return

        if message.get("type") != "execute_job":
            return

        job_id = message.get("job_id")
        attempt = message.get("attempt")

        with busy_lock:
            if busy:
                print(
                    f"[{WORKER_ID}] Already busy; rejecting {job_id}",
                    flush=True
                )
                return

            busy = True

        print(
            f"[{WORKER_ID}] STARTED job={job_id} attempt={attempt}",
            flush=True
        )

        start_time = time.time()

        try:
            result = execute(
                message["job_type"],
                message["payload"]
            )

            elapsed = round(time.time() - start_time, 4)

            print(
                f"[{WORKER_ID}] COMPLETED job={job_id} "
                f"result={result} duration={elapsed}s",
                flush=True
            )

            report_result(
                job_id,
                attempt,
                "completed",
                result=result
            )

        except Exception as exc:
            print(
                f"[{WORKER_ID}] FAILED job={job_id}: {exc}",
                flush=True
            )

            report_result(
                job_id,
                attempt,
                "failed",
                error=str(exc)
            )

        finally:
            with busy_lock:
                busy = False


def shutdown_handler(signum, frame):
    print(f"\n[{WORKER_ID}] Shutdown signal received.", flush=True)
    stop_event.set()


def main():
    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    # Bind first so registration does not advertise a port
    # before the worker is ready to receive assignments.
    server.bind(("0.0.0.0", WORKER_PORT))
    server.listen(10)
    server.settimeout(1.0)

    try:
        response = request_manager({
            "type": "register_worker",
            "worker_id": WORKER_ID,
            "host": WORKER_HOST,
            "port": WORKER_PORT
        })

        if response.get("type") != "registered":
            raise RuntimeError(f"Worker registration rejected: {response}")

        print(
            f"[{WORKER_ID}] Registered successfully.",
            flush=True
        )

        print(
            f"[{WORKER_ID}] Listening at "
            f"{WORKER_HOST}:{WORKER_PORT}",
            flush=True
        )

        threading.Thread(
            target=heartbeat_loop,
            daemon=True
        ).start()

        while not stop_event.is_set():
            try:
                conn, address = server.accept()

                threading.Thread(
                    target=handle_assignment,
                    args=(conn,),
                    daemon=True
                ).start()

            except socket.timeout:
                continue

    except Exception as exc:
        print(f"[{WORKER_ID}] Startup error: {exc}", flush=True)

    finally:
        stop_event.set()
        server.close()
        print(f"[{WORKER_ID}] Stopped.", flush=True)


if __name__ == "__main__":
    main()

