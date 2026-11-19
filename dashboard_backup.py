
import json
import os
import re
import signal
import socket
import subprocess
import threading
import time
import html
from pathlib import Path
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# ============================================================
# JACKFRUIT SDN DASHBOARD
# ============================================================

PORT = 8080
PROJECT = Path("/home/vboxuser/mininetproj")
MANAGER_IP = "10.0.0.1"
MANAGER_PORT = 5000

WORKER_CONFIG = {
    "worker-1": {
        "host": "h2",
        "ip": "10.0.0.2",
        "port": 9001,
        "switch_port": 2
    },
    "worker-2": {
        "host": "h3",
        "ip": "10.0.0.3",
        "port": 9002,
        "switch_port": 3
    }
}

CLIENT_CONFIG = {
    "10.0.0.1": "h1",
    "10.0.0.4": "h4",
    "10.0.0.5": "h5",
    "10.0.0.6": "h6"
}

lock = threading.RLock()
dashboard_events = []
last_snapshot = None


# ============================================================
# GENERAL UTILITIES
# ============================================================

def log(message, event_type="DASHBOARD"):
    with lock:
        dashboard_events.insert(0, {
            "time": time.strftime("%H:%M:%S"),
            "timestamp": time.time(),
            "type": event_type,
            "message": str(message)
        })
        del dashboard_events[100:]


def run_command(command, timeout=6):
    try:
        p = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=timeout
        )
        return {
            "success": p.returncode == 0,
            "stdout": p.stdout.strip(),
            "stderr": p.stderr.strip(),
            "returncode": p.returncode
        }
    except Exception as exc:
        return {
            "success": False,
            "stdout": "",
            "stderr": str(exc),
            "returncode": -1
        }


def find_namespace_pid(host):
    """Find the Mininet namespace process for a specific host."""

    if host not in {"h1", "h2", "h3", "h4"}:
        return None

    result = run_command(["pgrep", "-f", "mininet:" + host])

    for pid in result["stdout"].split():
        check = run_command(["ps", "-p", pid, "-o", "args="])

        if "mininet:" + host in check["stdout"]:
            if "pgrep" not in check["stdout"]:
                return pid

    return None


# ============================================================
# MANAGER COMMUNICATION
# ============================================================

MANAGER_CLIENT_CODE = r'''
import json
import socket
import sys

message = json.load(sys.stdin)

with socket.create_connection(("10.0.0.1", 5000), timeout=4) as sock:
    sock.settimeout(5)
    sock.sendall((json.dumps(message) + "\n").encode())

    with sock.makefile("r", encoding="utf-8") as reader:
        line = reader.readline()

    if not line:
        raise RuntimeError("Manager returned an empty response")

    print(line)
'''


def manager_request(message):
    pid = find_namespace_pid("h1")

    if not pid:
        return {
            "error": "Mininet h1 not found. Start Mininet first."
        }

    try:
        result = subprocess.run(
            [
                "mnexec", "-a", pid,
                "python3", "-c", MANAGER_CLIENT_CODE
            ],
            input=json.dumps(message),
            capture_output=True,
            text=True,
            timeout=8
        )

        if result.returncode != 0:
            return {
                "error": result.stderr.strip()
                or "Could not communicate with manager."
            }

        lines = result.stdout.strip().splitlines()

        if not lines:
            return {"error": "Manager returned no data."}

        return json.loads(lines[-1])

    except Exception as exc:
        return {"error": str(exc)}


# ============================================================
# OPENFLOW INFORMATION
# ============================================================

def get_ovs_data():
    result = run_command([
        "ovs-ofctl",
        "-O", "OpenFlow13",
        "dump-flows",
        "s1"
    ])

    if not result["success"]:
        return {
            "success": False,
            "error": result["stderr"],
            "flows": [],
            "raw": ""
        }

    flows = []

    for line in result["stdout"].splitlines():
        if "priority=" not in line:
            continue

        def field(name):
            match = re.search(
                r"(?:^|,)" + re.escape(name) + r"=([^, ]+)",
                line
            )
            return match.group(1) if match else "-"

        action = re.search(r"actions=(.*)", line)
        destination = re.search(r"nw_dst=([0-9.]+)", line)

        flows.append({
            "priority": field("priority"),
            "packets": field("n_packets"),
            "bytes": field("n_bytes"),
            "destination": destination.group(1) if destination else "-",
            "actions": action.group(1).strip() if action else "-",
            "raw": line.strip()
        })

    return {
        "success": True,
        "flows": flows,
        "raw": result["stdout"],
        "error": ""
    }


# ============================================================
# WORKER CONTROL
# ============================================================

def find_worker_process(worker_id):
    """Find a worker only when its exact configured arguments match."""

    config = WORKER_CONFIG.get(worker_id)

    if not config:
        return None

    namespace_pid = find_namespace_pid(config["host"])

    if not namespace_pid:
        return None

    python_code = r'''
import os
import sys

worker_id = sys.argv[1]
port = sys.argv[2]
worker_ip = sys.argv[3]
manager_ip = sys.argv[4]

for entry in os.listdir("/proc"):
    if not entry.isdigit():
        continue

    try:
        with open("/proc/" + entry + "/cmdline", "rb") as f:
            args = f.read().decode(errors="ignore").split("\0")
        args = [x for x in args if x]

        if len(args) < 5:
            continue

        script = args[1]

        if not script.endswith("/ank/worker.py"):
            continue

        if args[2:6] == [worker_id, port, worker_ip, manager_ip]:
            print(entry)
    except (OSError, IndexError):
        pass
'''

    result = run_command([
        "mnexec", "-a", namespace_pid,
        "python3", "-c", python_code,
        worker_id,
        str(config["port"]),
        config["ip"],
        MANAGER_IP
    ])

    if result["success"] and result["stdout"]:
        return result["stdout"].splitlines()[0].strip()

    return None


def stop_worker(worker_id):
    """Stop only the exact configured worker process."""

    if worker_id not in WORKER_CONFIG:
        return {"error": "Worker is not in the allowed worker list."}

    pid = find_worker_process(worker_id)

    if not pid:
        return {"error": f"{worker_id} process was not found."}

    try:
        os.kill(int(pid), signal.SIGTERM)

        log(
            f"Stop command sent to {worker_id}, PID {pid}",
            "WORKER_STOP"
        )

        return {
            "success": True,
            "message": f"Stop signal sent to {worker_id}."
        }

    except ProcessLookupError:
        return {"error": "Worker process already stopped."}

    except Exception as exc:
        return {"error": str(exc)}


def start_worker(worker_id):
    """Start a configured worker inside its Mininet host namespace."""

    config = WORKER_CONFIG.get(worker_id)

    if not config:
        return {"error": "Worker is not in the allowed worker list."}

    if find_worker_process(worker_id):
        return {"error": f"{worker_id} is already running."}

    namespace_pid = find_namespace_pid(config["host"])

    if not namespace_pid:
        return {
            "error": f"Mininet host {config['host']} was not found."
        }

    worker_script = PROJECT / "ank" / "worker.py"

    if not worker_script.exists():
        return {"error": f"Worker file not found: {worker_script}"}

    command = [
        "mnexec", "-a", namespace_pid,
        "python3", str(worker_script),
        worker_id,
        str(config["port"]),
        config["ip"],
        MANAGER_IP
    ]

    try:
        process = subprocess.Popen(
            command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True
        )

        log(
            f"Restart command sent to {worker_id}; launcher PID {process.pid}",
            "WORKER_START"
        )

        return {
            "success": True,
            "message": f"Restart command sent to {worker_id}. Check registration shortly."
        }

    except Exception as exc:
        return {"error": str(exc)}


# ============================================================
# LIVE STATE
# ============================================================

def get_state():
    worker_response = manager_request({
        "type": "list_workers"
    })

    job_response = manager_request({
        "type": "list_jobs"
    })

    event_response = manager_request({
        "type": "list_events"
    })

    manager_online = (
        worker_response.get("type") == "workers"
        and job_response.get("type") == "jobs"
    )

    workers = worker_response.get("workers", {})
    jobs = job_response.get("jobs", [])
    manager_events = event_response.get("events", [])

    # Derive distinct client hosts from actual source IPs.
    clients = {}

    for job in jobs:
        ip = job.get("client_ip") or "Unknown"
        host = (
            job.get("client_host")
            or CLIENT_CONFIG.get(ip)
            or "Other client"
        )

        if ip not in clients:
            clients[ip] = {
                "ip": ip,
                "host": host,
                "requests": 0
            }

        clients[ip]["requests"] += 1

    ovs = get_ovs_data()

    with lock:
        own_events = list(dashboard_events)

    activity = []

    for event in manager_events:
        activity.append({
            "time": event.get("timestamp")
            or event.get("time"),
            "type": event.get("type", "MANAGER"),
            "message": event.get("message", "")
        })

    activity.extend(own_events)

    activity.sort(
        key=lambda e: str(e.get("time", "")),
        reverse=True
    )

    return {
        "manager": manager_online,
        "manager_error": (
            worker_response.get("error")
            or job_response.get("error")
            or ""
        ),
        "workers": workers,
        "jobs": jobs,
        "clients": list(clients.values()),
        "ovs": ovs,
        "activity": activity[:100],
        "time": time.strftime("%H:%M:%S"),
        "topology": {
            "manager": "h1",
            "workers": WORKER_CONFIG,
            "clients": CLIENT_CONFIG
        }
    }


# ============================================================
# DASHBOARD HTML
# ============================================================

HTML = r"""
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Jackfruit | SDN Control Center</title>

<style>
:root{
--bg:#080d19;
--panel:#111b2e;
--panel2:#17243a;
--line:#293b58;
--text:#e8eef9;
--muted:#8da2c2;
--blue:#718cff;
--green:#39d9a0;
--red:#ff667e;
--yellow:#ffc66d;
}

*{box-sizing:border-box}
body{
margin:0;
background:var(--bg);
color:var(--text);
font-family:Arial,sans-serif;
}

header{
padding:22px 30px;
background:#101a2c;
border-bottom:1px solid var(--line);
display:flex;
justify-content:space-between;
align-items:center;
gap:15px;
flex-wrap:wrap;
}

h1{font-size:23px;margin:0}
header p{color:var(--muted);font-size:12px;margin:7px 0 0}
main{max-width:1600px;margin:auto;padding:22px}

.grid{
display:grid;
grid-template-columns:repeat(5,minmax(0,1fr));
gap:13px;
}

.card{
background:var(--panel);
border:1px solid var(--line);
border-radius:12px;
padding:17px;
margin-bottom:15px;
min-width:0;
}

.card h3{
font-size:12px;
color:var(--muted);
letter-spacing:1px;
margin:0 0 15px;
}

.big{font-size:27px;font-weight:bold}
.muted{color:var(--muted);font-size:12px}
.ok{color:var(--green)}
.bad{color:var(--red)}
.warn{color:var(--yellow)}
.blue{color:var(--blue)}

.layout{
display:grid;
grid-template-columns:1fr 1fr;
gap:15px;
}

input,select{
width:100%;
padding:11px;
margin:7px 0 13px;
border:1px solid var(--line);
border-radius:7px;
background:#091426;
color:white;
}

button{
background:var(--blue);
color:white;
padding:10px 14px;
border:0;
border-radius:7px;
font-weight:bold;
cursor:pointer;
margin:3px;
}

button:hover{filter:brightness(1.15)}
button:disabled{opacity:.4;cursor:not-allowed}
button.danger{background:#b83e57}
button.secondary{background:#344764}

table{
width:100%;
border-collapse:collapse;
font-size:12px;
}

th,td{
padding:11px 8px;
text-align:left;
border-bottom:1px solid var(--line);
vertical-align:top;
overflow-wrap:anywhere;
}

th{color:var(--muted)}
tr.detail-row td{background:#0c1628}
tr.job-row{cursor:pointer}
tr.job-row:hover{background:#1a2942}

pre{
white-space:pre-wrap;
overflow-wrap:anywhere;
font-size:11px;
color:#b9c9e5;
max-height:280px;
overflow:auto;
}

.topology{
display:flex;
align-items:center;
justify-content:center;
gap:12px;
flex-wrap:wrap;
padding:22px 5px;
}

.node{
background:#1b2d49;
border:1px solid #526c99;
border-radius:10px;
padding:14px;
min-width:110px;
text-align:center;
position:relative;
}

.node small{
display:block;
color:var(--muted);
margin-top:6px;
font-size:10px;
}

.node.active{
border-color:var(--green);
box-shadow:0 0 15px #39d9a044;
}

.node.failed{
border-color:var(--red);
box-shadow:0 0 15px #ff667e44;
}

.node.pulse:after{
content:"";
position:absolute;
inset:-5px;
border:2px solid var(--green);
border-radius:13px;
animation:pulse 1.3s infinite;
}

@keyframes pulse{
0%{opacity:1;transform:scale(.98)}
100%{opacity:0;transform:scale(1.13)}
}

.arrow{font-size:23px;color:var(--blue)}
.flowline{
height:3px;
width:40px;
background:var(--blue);
position:relative;
overflow:hidden;
}

.flowline:after{
content:"";
position:absolute;
height:3px;
width:13px;
background:white;
animation:travel 1.2s linear infinite;
}

@keyframes travel{
from{left:-15px}
to{left:45px}
}

.activity{
max-height:330px;
overflow:auto;
}

.event{
padding:10px 4px;
border-bottom:1px solid var(--line);
font-size:12px;
}

.event time{color:var(--muted);margin-right:10px}
.event strong{color:#aebeff}

.badge{
display:inline-block;
padding:4px 7px;
border-radius:5px;
font-size:10px;
font-weight:bold;
background:#293b58;
}

.badge.ok{background:#123d34;color:var(--green)}
.badge.bad{background:#451e2b;color:var(--red)}
.badge.warn{background:#46361b;color:var(--yellow)}

.detail{
padding:12px;
line-height:1.8;
font-size:12px;
}

.client{
display:inline-block;
background:#1b2d49;
border:1px solid var(--line);
padding:10px 13px;
border-radius:8px;
margin:5px;
}

.actions{white-space:nowrap}
#message{margin-top:10px;min-height:20px}
#lastUpdate{font-size:11px;color:var(--muted)}

@media(max-width:1050px){
.grid{grid-template-columns:repeat(3,1fr)}
}
@media(max-width:750px){
.grid,.layout{grid-template-columns:1fr 1fr}
}
@media(max-width:500px){
.grid,.layout{grid-template-columns:1fr}
main{padding:10px}
header{padding:18px}
}
</style>
</head>

<body>
<header>
<div>
<h1>JACKFRUIT | SDN CONTROL CENTER</h1>
<p>Distributed Job Processing · TCP Sockets · Mininet · Ryu · OpenFlow 1.3</p>
</div>
<div id="lastUpdate">Connecting...</div>
</header>

<main>

<div class="grid">
<div class="card">
<h3>JOB MANAGER</h3>
<div class="big" id="manager">...</div>
<p class="muted">h1 · 10.0.0.1 · TCP 5000</p>
</div>

<div class="card">
<h3>ACTIVE WORKERS</h3>
<div class="big" id="workerCount">0 / 0</div>
<p class="muted">Heartbeat monitored</p>
</div>

<div class="card">
<h3>TOTAL JOBS</h3>
<div class="big" id="jobCount">0</div>
<p class="muted">All connected clients</p>
</div>

<div class="card">
<h3>CLIENT HOSTS</h3>
<div class="big" id="clientCount">0</div>
<p class="muted">Distinct source IPs</p>
</div>

<div class="card">
<h3>OPENFLOW RULES</h3>
<div class="big" id="flowCount">0</div>
<p class="muted">Switch s1</p>
</div>
</div>

<div class="layout">

<div class="card">
<h3>SUBMIT A JOB</h3>

<label class="muted">Job type</label>
<select id="kind">
<option value="sum">Sum</option>
<option value="factorial">Factorial</option>
<option value="max">Maximum</option>
</select>

<div id="sumBox">
<label class="muted">Start</label>
<input id="start" type="number" value="1">
<label class="muted">End</label>
<input id="end" type="number" value="1000000">
</div>

<div id="factBox" style="display:none">
<label class="muted">Number (0–500)</label>
<input id="n" type="number" value="10" min="0" max="500">
</div>

<div id="maxBox" style="display:none">
<label class="muted">Numbers separated by commas</label>
<input id="values" value="10,20,30,40">
</div>

<button onclick="submitJob()">SUBMIT JOB</button>
<div id="message" class="muted"></div>
</div>

<div class="card">
<h3>LIVE MININET TOPOLOGY</h3>

<div class="topology">
<div>
<div class="node" id="h1">h1 · MANAGER
<small>10.0.0.1<br>Client 1</small>
</div>
<div class="node" id="h4">h4 · CLIENT 2
<small>10.0.0.4</small>
</div>
</div>

<div class="flowline"></div>

<div class="node" id="s1">s1 · OVS
<small>OpenFlow 1.3</small>
</div>

<div class="flowline"></div>

<div>
<div class="node" id="h2">h2 · WORKER 1
<small>10.0.0.2 · Port 2</small>
</div>
<div class="node" id="h3">h3 · WORKER 2
<small>10.0.0.3 · Port 3</small>
</div>
</div>
</div>

<p class="muted">Ryu controller · 127.0.0.1:6633 · OVS switch s1</p>
<p class="muted">Application path: Client → Manager → Worker</p>
<p class="muted">Network path: Source host → s1 → destination worker</p>
</div>
</div>

<div class="card">
<h3>CONNECTED CLIENT HOSTS</h3>
<div id="clients">Waiting for job submissions...</div>
<p class="muted">Client identity is derived from the TCP source IP recorded by the manager.</p>
</div>

<div class="card">
<h3>WORKER MONITORING AND CONTROLS</h3>
<table>
<thead>
<tr>
<th>Worker</th>
<th>Host / IP</th>
<th>State</th>
<th>Busy</th>
<th>Jobs</th>
<th>Heartbeat age</th>
<th>Controls</th>
</tr>
</thead>
<tbody id="workers">
<tr><td colspan="7">Waiting for manager...</td></tr>
</tbody>
</table>
<p class="muted">Stop sends SIGTERM to the exact configured worker process. Restart launches that worker inside its Mininet host namespace.</p>
</div>

<div class="card">
<h3>ALL JOBS · EXPAND A ROW FOR DETAILS</h3>
<table>
<thead>
<tr>
<th>Job ID</th>
<th>Client</th>
<th>Type</th>
<th>Status</th>
<th>Worker</th>
<th>Attempts</th>
<th>Result</th>
</tr>
</thead>
<tbody id="jobs">
<tr><td colspan="7">No jobs yet.</td></tr>
</tbody>
</table>
</div>

<div class="layout">
<div class="card">
<h3>OPENFLOW TABLE · LIVE COUNTERS</h3>
<table>
<thead>
<tr>
<th>Priority</th>
<th>Destination</th>
<th>Packets</th>
<th>Bytes</th>
<th>Action</th>
</tr>
</thead>
<tbody id="flows">
<tr><td colspan="5">Loading...</td></tr>
</tbody>
</table>
</div>

<div class="card">
<h3>RAW SWITCH FLOW TABLE</h3>
<pre id="rawFlows">Loading...</pre>
</div>
</div>

<div class="card">
<h3>LIVE JOB ROUTING AND RECOVERY</h3>
<div id="routing" class="detail">
Waiting for actual job events...
</div>
</div>

<div class="card">
<h3>SYSTEM ACTIVITY TIMELINE</h3>
<div id="activity" class="activity">Waiting for manager events...</div>
</div>

<div class="card">
<h3>RECORDED BENCHMARK COMPARISON</h3>
<div class="layout">
<div>
<p>Static baseline: <strong>3.2309 seconds</strong></p>
<p class="muted">1.5475 jobs/second</p>
</div>
<div>
<p>SDN-integrated: <strong>0.4388 seconds</strong></p>
<p class="muted">11.3939 jobs/second</p>
</div>
</div>
<p class="muted">Previously recorded individual runs, not repeated controlled trials. This does not establish that SDN alone caused the performance difference.</p>
</div>

</main>

<script>
const $ = id => document.getElementById(id);
let expandedJobs = new Set();
let lastJobs = {};
let refreshBusy = false;

function esc(value){
 return String(value ?? "-").replace(/[&<>"']/g,c=>({
  "&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"
 }[c]));
}

function statusBadge(status){
 let s=String(status||"UNKNOWN").toUpperCase();
 let cls=["COMPLETED","ONLINE","ALIVE"].includes(s)?"ok":
         ["FAILED","DOWN","OFFLINE"].includes(s)?"bad":"warn";
 return `<span class="badge ${cls}">${esc(s)}</span>`;
}

$("kind").onchange=()=>{
 let k=$("kind").value;
 $("sumBox").style.display=k==="sum"?"block":"none";
 $("factBox").style.display=k==="factorial"?"block":"none";
 $("maxBox").style.display=k==="max"?"block":"none";
};

async function api(path,body){
 let options=body===undefined?{}:{
  method:"POST",
  headers:{"Content-Type":"application/json"},
  body:JSON.stringify(body)
 };
 let response=await fetch(path,options);
 let data=await response.json();
 if(!response.ok || data.error) throw Error(data.error||"Request failed");
 return data;
}

async function submitJob(){
 let kind=$("kind").value;
 let payload={};

 if(kind==="sum"){
  let start=Number($("start").value);
  let end=Number($("end").value);
  if(!Number.isInteger(start)||!Number.isInteger(end)||start>end){
   $("message").textContent="Enter valid integer start and end values.";
   return;
  }
  payload={start,end};
 }

 if(kind==="factorial"){
  let n=Number($("n").value);
  if(!Number.isInteger(n)||n<0||n>500){
   $("message").textContent="Enter an integer from 0 to 500.";
   return;
  }
  payload={n};
 }

 if(kind==="max"){
  let values=$("values").value.split(",").map(x=>Number(x.trim()));
  if(!values.length||values.some(x=>!Number.isFinite(x))){
   $("message").textContent="Enter valid comma-separated numbers.";
   return;
  }
  payload={values};
 }

 $("message").textContent="Submitting to manager...";

 try{
  let data=await api("/api/submit",{job_type:kind,payload});
  $("message").textContent="Queued: "+data.job_id;
  refresh();
 }catch(e){
  $("message").textContent="Submission failed: "+e.message;
 }
}

async function workerAction(worker,action){
 let verb=action==="stop"?"STOP":"RESTART";
 if(!confirm(verb+" "+worker+"?"))return;

 try{
  let data=await api("/api/worker/"+action,{worker_id:worker});
  $("message").textContent=data.message;
  setTimeout(refresh,1000);
 }catch(e){
  $("message").textContent="Worker action failed: "+e.message;
 }
}

function renderWorkers(workers){
 let entries=Object.entries(workers||{});
 let online=entries.filter(([id,w])=>w.alive).length;

 $("workerCount").textContent=online+" / "+entries.length;

 if(!entries.length){
  $("workers").innerHTML='<tr><td colspan="7">No registered workers.</td></tr>';
  return;
 }

 $("workers").innerHTML=entries.map(([id,w])=>{
  let cfg=id==="worker-1"?["h2","10.0.0.2"]:["h3","10.0.0.3"];
  let host=w.host||cfg[1];
  let state=w.alive?"ONLINE":"OFFLINE";
  let age=w.last_heartbeat_age??"-";

  return `<tr>
   <td><strong>${esc(id)}</strong></td>
   <td>${esc(cfg[0])} · ${esc(host)}:${esc(w.port)}</td>
   <td>${statusBadge(state)}</td>
   <td>${w.busy?"BUSY":"IDLE"}</td>
   <td>${esc(w.active_jobs??0)}</td>
   <td>${esc(age)} s</td>
   <td class="actions">
    <button class="danger" ${!w.alive?"disabled":""}
     onclick="workerAction('${esc(id)}','stop')">STOP</button>
    <button class="secondary" ${w.alive?"disabled":""}
     onclick="workerAction('${esc(id)}','start')">RESTART</button>
   </td>
  </tr>`;
 }).join("");
}

function renderJobs(jobs){
 $("jobCount").textContent=jobs.length;

 if(!jobs.length){
  $("jobs").innerHTML='<tr><td colspan="7">No jobs submitted yet.</td></tr>';
  $("routing").textContent="No job activity yet.";
  return;
 }

 let active=jobs.find(j=>["RUNNING","QUEUED"].includes(j.status));
 if(active){
  let worker=active.worker_id||"Waiting for available worker";
  $("routing").innerHTML=
   `<strong>${esc(active.job_id)}</strong><br>
   Client ${esc(active.client_ip)} → Manager h1 → ${esc(worker)}<br>
   Status: ${statusBadge(active.status)}<br>
   Attempt: ${esc(active.attempts)}<br>
   The visualization follows manager-reported job state, not simulated packet traffic.`;
 }else{
  $("routing").textContent="No queued or running jobs. Recent jobs are shown in the history below.";
 }

 $("jobs").innerHTML=jobs.map(j=>{
  let id=String(j.job_id||"");
  let open=expandedJobs.has(id);
  let result=j.result===null||j.result===undefined?"-":JSON.stringify(j.result);
  let history=Array.isArray(j.history)?j.history:[];
  let details=`
   <div class="detail">
   <strong>Full job ID:</strong> ${esc(id)}<br>
   <strong>Client IP:</strong> ${esc(j.client_ip)}<br>
   <strong>Client host:</strong> ${esc(j.client_host||"-")}<br>
   <strong>Created:</strong> ${esc(j.created_time||"-")}<br>
   <strong>Updated:</strong> ${esc(j.updated_time||"-")}<br>
   <strong>Completed:</strong> ${esc(j.completed_at||"-")}<br>
   <strong>Error:</strong> ${esc(j.error||"-")}<br>
   <strong>Payload:</strong> ${esc(JSON.stringify(j.payload||{}))}<br>
   <strong>Attempt history:</strong>
   ${history.length?history.map(h=>
    `<div>• ${esc(h.event)} — ${esc(h.message)} (${esc(h.worker_id||"-")})</div>`
   ).join(""):"No detailed history recorded by manager."}
   </div>`;

  return `<tr class="job-row" onclick="toggleJob('${esc(id)}')">
   <td>${esc(id.slice(0,8))}…</td>
   <td>${esc(j.client_host||j.client_ip||"-")}</td>
   <td>${esc(j.job_type)}</td>
   <td>${statusBadge(j.status)}</td>
   <td>${esc(j.worker_id||"-")}</td>
   <td>${esc(j.attempts)}</td>
   <td>${esc(result)}</td>
  </tr>
  ${open?`<tr class="detail-row"><td colspan="7">${details}</td></tr>`:""}`;
 }).join("");
}

function toggleJob(id){
 if(expandedJobs.has(id))expandedJobs.delete(id);
 else expandedJobs.add(id);
 refresh();
}

function renderClients(clients){
 $("clientCount").textContent=clients.length;

 $("clients").innerHTML=clients.length?clients.map(c=>
  `<div class="client">
   <strong>${esc(c.host)}</strong><br>
   <span class="muted">${esc(c.ip)}</span><br>
   ${esc(c.requests)} submitted jobs
  </div>`
 ).join(""):"No client submissions recorded yet.";
}

function renderFlows(ovs){
 let flows=ovs.flows||[];
 $("flowCount").textContent=flows.length;

 $("flows").innerHTML=flows.length?flows.map(f=>
  `<tr>
   <td>${esc(f.priority)}</td>
   <td>${esc(f.destination)}</td>
   <td>${esc(f.packets)}</td>
   <td>${esc(f.bytes)}</td>
   <td>${esc(f.actions)}</td>
  </tr>`
 ).join(""):'<tr><td colspan="5">No OpenFlow rules found.</td></tr>';

 $("rawFlows").textContent=ovs.raw||ovs.error||"No switch output.";
}

function renderActivity(events){
 $("activity").innerHTML=events.length?events.map(e=>
  `<div class="event">
   <time>${esc(e.time)}</time>
   <strong>${esc(e.type)}</strong>
   ${esc(e.message)}
  </div>`
 ).join(""):"No events recorded.";
}

function renderTopology(workers,jobs){
 ["h2","h3"].forEach(id=>{
  $(id).classList.remove("active","failed","pulse");
 });

 let mapping={"worker-1":"h2","worker-2":"h3"};

 for(let [workerId,hostId] of Object.entries(mapping)){
  let w=workers[workerId];
  if(!w)continue;
  $(hostId).classList.toggle("active",!!w.alive);
  $(hostId).classList.toggle("failed",!w.alive);
  if(w.busy)$(hostId).classList.add("pulse");
 }

 let running=jobs.find(j=>j.status==="RUNNING");
 if(running){
  if(running.client_ip==="10.0.0.4")$("h4").classList.add("pulse");
  else $("h1").classList.add("pulse");
 }
}

async function refresh(){
 if(refreshBusy)return;
 refreshBusy=true;

 try{
  let d=await api("/api/state");

  $("manager").innerHTML=d.manager?
   '<span class="ok">ONLINE</span>':
   '<span class="bad">OFFLINE</span>';

  renderWorkers(d.workers);
  renderJobs(d.jobs);
  renderClients(d.clients);
  renderFlows(d.ovs);
  renderActivity(d.activity);
  renderTopology(d.workers,d.jobs);

  $("lastUpdate").textContent="Updated "+d.time;

  if(!d.manager && d.manager_error){
   $("message").textContent=d.manager_error;
  }

 }catch(e){
  $("manager").innerHTML='<span class="bad">CONNECTION ERROR</span>';
  $("lastUpdate").textContent=e.message;
 }finally{
  refreshBusy=false;
 }
}

refresh();
setInterval(refresh,2500);
</script>
</body>
</html>
"""


# ============================================================
# HTTP API
# ============================================================

class Handler(BaseHTTPRequestHandler):

    def send_json(self, data, status=200):
        body = json.dumps(data, default=str).encode()

        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def read_json(self):
        length = int(self.headers.get("Content-Length", "0"))

        if length <= 0 or length > 100000:
            raise ValueError("Invalid request body size.")

        return json.loads(self.rfile.read(length))

    def do_GET(self):
        if self.path == "/":
            body = HTML.encode()

            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        elif self.path == "/api/state":
            self.send_json(get_state())

        else:
            self.send_json({"error": "Not found"}, 404)

    def do_POST(self):
        try:
            data = self.read_json()

            if self.path == "/api/submit":
                kind = data.get("job_type")
                payload = data.get("payload", {})

                if kind not in {"sum", "factorial", "max"}:
                    raise ValueError("Unsupported job type.")

                response = manager_request({
                    "type": "submit_job",
                    "job_type": kind,
                    "payload": payload,
                    "client_host": "dashboard"
                })

                if not response.get("job_id"):
                    self.send_json({
                        "error": response.get(
                            "error",
                            "Manager did not acknowledge the job."
                        )
                    }, 502)
                    return

                log(
                    f"Dashboard submitted {kind}: {response['job_id']}",
                    "JOB_SUBMITTED"
                )

                self.send_json(response)
                return

            if self.path == "/api/worker/stop":
                worker_id = data.get("worker_id")
                response = stop_worker(worker_id)

                self.send_json(
                    response,
                    200 if response.get("success") else 400
                )
                return

            if self.path == "/api/worker/start":
                worker_id = data.get("worker_id")
                response = start_worker(worker_id)

                self.send_json(
                    response,
                    200 if response.get("success") else 400
                )
                return

            self.send_json({"error": "Not found"}, 404)

        except Exception as exc:
            self.send_json({"error": str(exc)}, 400)

    def log_message(self, *args):
        pass


# ============================================================
# START SERVER
# ============================================================

if __name__ == "__main__":
    print("=" * 55)
    print("JACKFRUIT SDN DASHBOARD")
    print("=" * 55)
    print("Dashboard: http://localhost:8080")
    print("Manager:   10.0.0.1:5000")
    print("OpenFlow:  s1 / OpenFlow 1.3")
    print("=" * 55)

    log("Dashboard server started")

    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nDashboard shutting down.")
    finally:
        server.server_close()

