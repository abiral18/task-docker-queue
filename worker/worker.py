import json
import logging
import os
import random
import signal
import socket
import time
import uuid

import redis

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("worker")

r = redis.Redis(host=os.getenv("REDIS_HOST", "localhost"), port=6379, decode_responses=True)

WORKER_ID = f"worker-{uuid.uuid4().hex[:8]}"
shutdown = False


def handle_signal(signum, frame):
    global shutdown
    log.info("Shutdown signal received, finishing current task...")
    shutdown = True


signal.signal(signal.SIGINT, handle_signal)


# --- Task processors ---

def process_ocr(payload):
    pages = payload.get("pages", 5)
    time.sleep(0.3 * pages)
    return {"pages_processed": pages, "confidence": round(random.uniform(0.85, 0.99), 3)}


def process_parse(payload):
    time.sleep(random.uniform(0.5, 2.0))
    fields = random.sample(["name", "date", "amount", "address"], k=random.randint(2, 4))
    return {"fields_extracted": fields}


def process_classify(payload):
    time.sleep(random.uniform(0.3, 1.0))
    category = random.choice(["invoice", "receipt", "contract", "letter"])
    return {"category": category, "confidence": round(random.uniform(0.7, 0.99), 3)}


def process_extract(payload):
    time.sleep(random.uniform(1.0, 3.0))
    return {"entities_found": random.randint(5, 30), "tables_found": random.randint(0, 5)}


PROCESSORS = {
    "ocr": process_ocr,
    "parse": process_parse,
    "classify": process_classify,
    "extract": process_extract,
}


# --- Worker registration + heartbeat ---

def register():
    r.sadd("workers:active", WORKER_ID)
    r.hset(f"worker:{WORKER_ID}", mapping={
        "worker_id": WORKER_ID,
        "status": "idle",
        "started_at": time.time(),
        "last_heartbeat": time.time(),
        "tasks_completed": 0,
        "tasks_failed": 0,
        "current_task": "",
    })
    log.info(f"Registered as {WORKER_ID}")


def deregister():
    r.srem("workers:active", WORKER_ID)
    r.delete(f"worker:{WORKER_ID}")
    log.info(f"Deregistered {WORKER_ID}")


def heartbeat():
    r.hset(f"worker:{WORKER_ID}", "last_heartbeat", time.time())


# --- Task execution with retries ---

def execute_task(task_id):
    tk = f"task:{task_id}"

    r.hset(tk, mapping={
        "status": "running",
        "started_at": time.time(),
        "worker_id": WORKER_ID,
    })
    r.hset(f"worker:{WORKER_ID}", mapping={
        "status": "busy",
        "current_task": task_id,
    })

    task_type = r.hget(tk, "task_type")
    payload_raw = r.hget(tk, "payload")
    payload = json.loads(payload_raw) if payload_raw else {}
    max_retries = int(r.hget(tk, "max_retries") or 3)
    retries = int(r.hget(tk, "retries") or 0)

    log.info(f"Processing {task_id[:12]}... (type={task_type}, retry={retries}/{max_retries})")

    try:
        if random.random() < 0.2:
            raise RuntimeError("Simulated transient failure")

        processor = PROCESSORS.get(task_type)
        if not processor:
            raise ValueError(f"Unknown task type: {task_type}")

        result = processor(payload)

        r.hset(tk, mapping={
            "status": "completed",
            "completed_at": time.time(),
            "result": json.dumps(result),
        })
        r.hincrby(f"worker:{WORKER_ID}", "tasks_completed", 1)
        log.info(f"Completed {task_id[:12]}...")

    except Exception as e:
        log.warning(f"Failed {task_id[:12]}...: {e}")

        if retries < max_retries:
            new_retries = retries + 1
            r.hset(tk, mapping={
                "status": "retrying",
                "retries": new_retries,
                "error": str(e),
                "worker_id": "",
            })
            r.zadd("task_queue", {task_id: -1})
            log.info(f"Re-queued {task_id[:12]}... (retry {new_retries}/{max_retries})")
        else:
            r.hset(tk, mapping={
                "status": "failed",
                "completed_at": time.time(),
                "error": str(e),
            })
            r.lpush("dead_letter_queue", task_id)
            r.hincrby(f"worker:{WORKER_ID}", "tasks_failed", 1)
            log.error(f"Dead-lettered {task_id[:12]}... after {max_retries} retries")

    finally:
        r.hset(f"worker:{WORKER_ID}", mapping={
            "status": "idle",
            "current_task": "",
        })


# --- Main loop ---

def run():
    register()
    last_hb = time.time()

    try:
        while not shutdown:
            if time.time() - last_hb > 5:
                heartbeat()
                last_hb = time.time()

            result = r.zpopmax("task_queue", count=1)
            if result:
                task_id, score = result[0]
                status = r.hget(f"task:{task_id}", "status")
                if status == "cancelled":
                    log.info(f"Skipping cancelled task {task_id[:12]}...")
                    continue
                execute_task(task_id)
            else:
                time.sleep(0.5)
    except KeyboardInterrupt:
        pass
    finally:
        deregister()
        log.info("Worker shut down cleanly")


if __name__ == "__main__":
    run()