# SDN-Based Distributed Job Scheduling System

A Python-based distributed job scheduling prototype using TCP sockets, worker management, job queues, and Mininet-based network experimentation.

## Project Overview

This project demonstrates a distributed system in which clients submit computational jobs to a central job manager. The manager maintains a job queue, coordinates worker nodes, tracks job execution and handles worker failures.

The project also includes components for network topology experimentation and SDN controller interaction.

## Key Features

- **Client-Server Communication:** TCP socket-based communication between clients and the job manager.
- **Job Submission:** Clients can submit computational jobs and receive acknowledgements.
- **Job Queue:** Maintains submitted jobs until workers are available.
- **Worker Management:** Supports worker registration, monitoring and heartbeat messages.
- **Job Scheduling:** Selects eligible workers for job execution.
- **Failure Handling:** Supports bounded retries when worker failures are detected.
- **Status Tracking:** Allows clients to query job status and results.
- **Dashboard:** Provides a visual interface for monitoring the system.
- **Network Topology:** Includes a Mininet topology for network experimentation.
- **SDN Components:** Includes controller and adapter components for SDN-related functionality. Full application-level integration depends on the controller interface and must be verified separately.

## Project Structure

| File | Description |
|---|---|
| `client.py`, `clientj.py` | Client-side job submission |
| `job_manager.py` | Central job manager |
| `job_queue.py` | Job queue management |
| `scheduler.py` | Worker selection and scheduling |
| `worker.py` | Worker node execution |
| `worker_registry.py` | Worker registration and monitoring |
| `worker_simulator.py` | Worker simulation |
| `controller.py` | Controller-related functionality |
| `sdn_adapter.py` | SDN interaction adapter |
| `topology.py` | Mininet network topology |
| `dashboard.py` | Monitoring dashboard |
| `benchmark.py` | Performance benchmarking |
| `test_system.py` | System testing |

## Technologies Used

- Python
- TCP Sockets
- Mininet
- Software-Defined Networking (SDN)
- Ryu Controller
- Linux / Ubuntu

## Running the Prototype

Requirements:
- Python 3.10 or newer
- Ubuntu/Linux environment recommended
- Mininet and Ryu for network experimentation

Start the job manager:

```bash
python job_manager.py
```

Start worker nodes in separate terminals:

```bash
python worker.py worker-1 9001
```

```bash
python worker.py worker-2 9002
```

Start a client:

```bash
python client.py
```

Submit a job and use the returned job ID to query its status and result.

**Note:** These commands describe the local prototype setup. Mininet deployment and controller integration require their corresponding network and controller configurations.

## Current Limitations

- This is an academic prototype, not a production-grade distributed system.
- Job and worker information may be stored in memory.
- Worker failure recovery uses bounded retries and may provide at-least-once execution.
- Multi-host deployment requires workers to register their reachable IP addresses.
- Full application-level SDN integration and automatic route recovery should be verified against the configured controller interface.

## Project Objective

To explore distributed job execution, socket programming, worker coordination, scheduling, failure handling and the role of Software-Defined Networking in a multi-node environment.
