# DistrQ
Distributed Task Queue



# DistrQ: Distributed Task Queue

A robust, distributed task execution system designed for "Zero Task Loss" and high-concurrency environments.

## 🚀 Features
- **Fault Tolerance:** PostgreSQL-backed state machine ensuring tasks are never lost, even if a worker node fails mid-execution.
- **Worker Heartbeats:** Implemented a watchdog mechanism that monitors worker health and automatically re-queues stale tasks.
- **Scalability:** Horizontal scaling of workers via Docker; handles 10K+ concurrent job submissions.
- **Priority Scheduling:** Intelligent task prioritization to ensure mission-critical jobs are processed first.

## 🛠️ Tech Stack
- **Backend:** Python, FastAPI
- **Broker/State:** PostgreSQL, Redis
- **Infrastructure:** Docker Compose
- **Monitoring:** Heartbeat-based health checks

## 🔧 Workflow
1. Client submits a job via FastAPI.
2. Job is logged in the Postgres "State Machine" as `PENDING`.
3. Worker picks up the job and sends regular heartbeats.
4. If a heartbeat is missed, the system resets the job to `PENDING` for a new worker.
