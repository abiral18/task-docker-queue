# DocQueue — Distributed Task Queue for Document Processing

A distributed task queue system built for high-throughput document processing workloads. Demonstrates infrastructure engineering concepts: priority scheduling via Redis sorted sets, fault tolerance with retries and dead-letter queues, and worker health monitoring.

Simulates the kind of workload orchestration that powers AI document processing platforms — where documents need to be OCR'd, parsed, classified, and extracted in parallel with reliability guarantees.

## Architecture

Clients → API Server (FastAPI) → Redis Priority Queue → Workers
↓
Success → done
Failure → retry or dead-letter

### Components

- **API Server** — Accepts task submissions via REST, stores metadata in Redis hashes, and enqueues tasks into a Redis sorted set where higher-priority jobs are consumed first.
- **Workers** — Independent processes that poll the queue using ZPOPMAX, execute tasks, and handle failures with exponential backoff retries up to a configurable limit.
- **Redis** — Message broker (sorted set for priority queue) and state store (hashes for task metadata).

## Task Types

Simulates document processing with realistic latency:

| Type | Description | Simulated Latency |
|------|-------------|-------------------|
| ocr | Optical character recognition | ~0.3s per page |
| parse | Structured field extraction | 0.5–2.0s |
| classify | Document categorization | 0.3–1.0s |
| extract | Entity and table extraction | 1.0–3.0s |

## Quick Start

### Prerequisites
- Python 3.10+
- Redis (via WSL, Docker, or native install)

### Setup
```bash
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate.bat
pip install fastapi uvicorn redis
```

### Start Redis
```bash
# Via WSL (Windows)
wsl
sudo service redis-server start

# Via Docker
docker run -d --name redis -p 6379:6379 redis:7-alpine
```

### Run the API
```bash
uvicorn api.server:app --reload
```

### Run a Worker
```bash
python -m worker.worker
```

### Submit Tasks
```bash
# Submit a high-priority OCR task
curl -X POST http://localhost:8000/tasks \
  -H "Content-Type: application/json" \
  -d '{"task_type": "ocr", "priority": "high", "payload": {"pages": 10}}'

# Check task status
curl http://localhost:8000/tasks/{task_id}

# View the priority queue
curl http://localhost:8000/queue

# List recent tasks
curl http://localhost:8000/tasks
```

## API Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | /health | Health check with Redis connectivity |
| POST | /tasks | Submit a task |
| GET | /tasks/{id} | Get task status and result |
| GET | /tasks | List recent tasks |
| GET | /queue | View priority queue contents |

## Design Decisions

**Priority Queue via Redis Sorted Set** — ZADD/ZPOPMAX gives O(log N) insertion and O(1) pop of the highest-priority task, avoiding the overhead of multiple named queues per priority level.

**Exponential Backoff on Retry** — Failed tasks are re-queued with a low score so fresh tasks aren't starved by retrying ones. Retries are capped at a configurable max (default 3).

**Dead Letter Queue** — Tasks that exhaust retries are moved to a DLQ for debugging and manual reprocessing, rather than being silently dropped.

**Graceful Shutdown** — Workers catch SIGINT/SIGTERM and finish their current task before exiting, preventing orphaned jobs during deploys.

## Project Structure

task-queue/
├── api/
│   └── server.py        # FastAPI application
├── worker/
│   └── worker.py        # Task consumer with retry logic
├── requirements.txt
├── .gitignore
└── README.md

## Tech Stack

- Python, FastAPI, Redis, Docker