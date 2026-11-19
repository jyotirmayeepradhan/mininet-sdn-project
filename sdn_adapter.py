
"""
Jackfruit SDN Adapter
Installs and inspects OpenFlow 1.3 rules through OVS.
"""

import ipaddress
import re
import subprocess

DEFAULT_SWITCH = "s1"
OPENFLOW_VERSION = "OpenFlow13"

WORKER_PORTS = {
    "10.0.0.2": 2,
    "10.0.0.3": 3
}


def valid_switch(switch):
    if not re.fullmatch(r"[a-zA-Z0-9_-]+", switch):
        raise ValueError("Invalid switch name.")
    return switch


def valid_worker_ip(worker_ip):
    return str(ipaddress.IPv4Address(worker_ip))


def run_ovs_command(args, timeout=5):
    try:
        result = subprocess.run(
            ["sudo", "ovs-ofctl", "-O", OPENFLOW_VERSION] + args,
            capture_output=True,
            text=True,
            timeout=timeout
        )

        if result.returncode != 0:
            return {
                "success": False,
                "error": result.stderr.strip() or result.stdout.strip()
            }

        return {
            "success": True,
            "output": result.stdout.strip()
        }

    except subprocess.TimeoutExpired:
        return {"success": False, "error": "ovs-ofctl command timed out."}

    except OSError as exc:
        return {"success": False, "error": str(exc)}


def install_worker_flow(worker_ip, switch_port, switch=DEFAULT_SWITCH):
    """
    Install a destination-IP forwarding rule for a worker.
    Example:
    priority=10,ip,nw_dst=10.0.0.2,actions=output:2
    """

    try:
        worker_ip = valid_worker_ip(worker_ip)
        switch = valid_switch(switch)

        if type(switch_port) is not int or switch_port <= 0:
            raise ValueError("Switch port must be a positive integer.")

        flow = (
            f"priority=10,ip,nw_dst={worker_ip},"
            f"actions=output:{switch_port}"
        )

        result = run_ovs_command([
            "add-flow",
            switch,
            flow
        ])

        if not result["success"]:
            print("[SDN] Flow installation failed:", result["error"], flush=True)
            return False

        print(
            f"[SDN] Flow installed: {worker_ip} -> port {switch_port}",
            flush=True
        )

        return True

    except ValueError as exc:
        print("[SDN] Invalid flow:", exc, flush=True)
        return False


def install_all_worker_flows(switch=DEFAULT_SWITCH):
    """Install the predefined worker forwarding rules."""

    results = {}

    for worker_ip, switch_port in WORKER_PORTS.items():
        results[worker_ip] = install_worker_flow(
            worker_ip,
            switch_port,
            switch
        )

    return results


def get_worker_flows(switch=DEFAULT_SWITCH):
    """Return actual flow-table entries from the OVS switch."""

    switch = valid_switch(switch)

    result = run_ovs_command([
        "dump-flows",
        switch
    ])

    if not result["success"]:
        return {
            "success": False,
            "error": result["error"],
            "flows": []
        }

    flows = []

    for line in result["output"].splitlines():
        if "priority=" not in line:
            continue

        flows.append({
            "raw": line.strip(),
            "priority": extract_field(line, "priority"),
            "match": extract_match(line),
            "actions": extract_actions(line),
            "packets": extract_field(line, "n_packets"),
            "bytes": extract_field(line, "n_bytes")
        })

    return {
        "success": True,
        "switch": switch,
        "flows": flows
    }


def extract_field(line, field):
    match = re.search(r"(?:^|,)" + re.escape(field) + r"=([^, ]+)", line)
    return match.group(1) if match else None


def extract_match(line):
    match = re.search(r"\b(ip(?:,[^ ]*)?)", line)
    return match.group(1) if match else ""


def extract_actions(line):
    match = re.search(r"actions=(.*)$", line)
    return match.group(1).strip() if match else ""


def delete_worker_flow(worker_ip, switch=DEFAULT_SWITCH):
    """Delete the priority-10 rule for one worker."""

    try:
        worker_ip = valid_worker_ip(worker_ip)
        switch = valid_switch(switch)

        result = run_ovs_command([
            "del-flows",
            switch,
            f"priority=10,ip,nw_dst={worker_ip}"
        ])

        if not result["success"]:
            print("[SDN] Flow deletion failed:", result["error"], flush=True)
            return False

        print(f"[SDN] Deleted flow for {worker_ip}", flush=True)
        return True

    except ValueError as exc:
        print("[SDN] Invalid flow deletion:", exc, flush=True)
        return False

