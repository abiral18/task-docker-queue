import logging
import time
import os
import redis

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("monitor")

r = redis.Redis(host=os.getenv("REDIS_HOST", "localhost"), port=6379, decode_responses=True)

HEARTBEAT_TIMEOUT = 15  # seconds before a worker is considered dead


def check_workers():
    worker_ids = r.smembers("workers:active")
    now = time.time()

    for wid in worker_ids:
        last_hb = r.hget(f"worker:{wid}", "last_heartbeat")
        if not last_hb or (now - float(last_hb)) > HEARTBEAT_TIMEOUT:
            log.warning(f"Worker {wid} missed heartbeat — marking dead")

            # Check for orphaned task
            current_task = r.hget(f"worker:{wid}", "current_task")
            if current_task:
                status = r.hget(f"task:{current_task}", "status")
                if status == "running":
                    retries = int(r.hget(f"task:{current_task}", "retries") or 0)
                    max_retries = int(r.hget(f"task:{current_task}", "max_retries") or 3)

                    if retries < max_retries:
                        r.hset(f"task:{current_task}", mapping={
                            "status": "retrying",
                            "retries": retries + 1,
                            "error": f"Worker {wid} died during execution",
                            "worker_id": "",
                        })
                        r.zadd("task_queue", {current_task: 0})
                        log.info(f"Re-queued orphaned task {current_task[:12]}...")
                    else:
                        r.hset(f"task:{current_task}", mapping={
                            "status": "failed",
                            "completed_at": now,
                            "error": f"Worker {wid} died, retries exhausted",
                        })
                        r.lpush("dead_letter_queue", current_task)
                        log.error(f"Dead-lettered orphaned task {current_task[:12]}...")

            # Remove dead worker
            r.srem("workers:active", wid)
            r.delete(f"worker:{wid}")
            log.info(f"Cleaned up dead worker {wid}")


def log_metrics():
    queue_depth = r.zcard("task_queue")
    active_workers = r.scard("workers:active")
    dlq_size = r.llen("dead_letter_queue")

    log.info(
        f"queue={queue_depth}  "
        f"workers={active_workers}  "
        f"dlq={dlq_size}"
    )


def run():
    log.info("Monitor started")
    while True:
        try:
            check_workers()
            log_metrics()
        except Exception as e:
            log.error(f"Monitor error: {e}")
        time.sleep(5)


if __name__ == "__main__":
    run()