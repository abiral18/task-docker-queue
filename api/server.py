import json
import time
import uuid

import redis
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from enum import Enum
from typing import Optional

app = FastAPI(title="DocQueue")

r = redis.Redis(host="localhost", port=6379, decode_responses=True)


# --- Models ---

class Priority(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


PRIORITY_SCORES = {
    Priority.LOW: 0,
    Priority.MEDIUM: 10,
    Priority.HIGH: 20,
    Priority.CRITICAL: 30,
}


class TaskType(str, Enum):
    OCR = "ocr"
    PARSE = "parse"
    CLASSIFY = "classify"
    EXTRACT = "extract"


class TaskSubmission(BaseModel):
    task_type: TaskType
    priority: Priority = Priority.MEDIUM
    payload: dict = Field(default_factory=dict)
    max_retries: int = Field(default=3, ge=0, le=10)


# --- Helpers ---

def task_key(task_id: str) -> str:
    return f"task:{task_id}"


def get_task(task_id: str) -> dict:
    data = r.hgetall(task_key(task_id))
    if not data:
        raise HTTPException(status_code=404, detail="Task not found")
    return data


# --- Endpoints ---

@app.get("/health")
def health():
    r.ping()
    return {"status": "ok"}


@app.post("/tasks", status_code=201)
def create_task(submission: TaskSubmission):
    task_id = str(uuid.uuid4())
    now = time.time()

    task = {
        "task_id": task_id,
        "status": "queued",
        "task_type": submission.task_type.value,
        "priority": submission.priority.value,
        "payload": json.dumps(submission.payload),
        "max_retries": submission.max_retries,
        "retries": 0,
        "created_at": now,
        "started_at": "",
        "completed_at": "",
        "result": "",
        "error": "",
        "worker_id": "",
    }

    # Store task metadata in a Redis hash
    r.hset(task_key(task_id), mapping=task)

    # Add to priority queue (sorted set — higher score = picked first)
    score = PRIORITY_SCORES[submission.priority]
    r.zadd("task_queue", {task_id: score})

    # Track in recent tasks list
    r.lpush("task_index", task_id)
    r.ltrim("task_index", 0, 999)

    return get_task(task_id)


@app.get("/tasks/{task_id}")
def read_task(task_id: str):
    return get_task(task_id)


@app.get("/tasks")
def list_tasks(limit: int = Query(10, ge=1, le=100)):
    task_ids = r.lrange("task_index", 0, limit - 1)
    return {"tasks": [get_task(tid) for tid in task_ids]}


@app.get("/queue")
def view_queue():
    """See what's in the priority queue, highest priority first."""
    items = r.zrevrange("task_queue", 0, -1, withscores=True)
    return {
        "depth": len(items),
        "tasks": [
            {"task_id": tid, "score": score}
            for tid, score in items
        ],
    }