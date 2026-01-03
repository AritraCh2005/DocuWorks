# worker/utils.py
import json
import os
import redis

REDIS = redis.from_url(
    os.environ["CELERY_BROKER_URL"],
    decode_responses=True
)

def update_state(task_id: str, **fields):
    """
    Sync-safe Redis update for Celery workers.
    """
    REDIS.hset(task_id, mapping={k: str(v) for k, v in fields.items()})
    REDIS.publish(f"progress:{task_id}", json.dumps(fields))
