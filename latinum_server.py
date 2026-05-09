import os
import uuid
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from arq import create_pool
from arq.connections import RedisSettings

app = FastAPI(title="Latinum Refinery Gateway")

# Configuration from Coolify Environment Variables
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

class WorkUnit(BaseModel):
    task_id: str = str(uuid.uuid4())
    payload: str
    complexity_target: int = 10
    model_required: str = "gemma"

@app.on_event("startup")
async def startup():
    # Connect to the Redis Queue provided by Coolify
    app.state.redis = await create_pool(RedisSettings.from_dsn(REDIS_URL))
    print("Successfully connected to Latinum Redis Queue")

@app.get("/")
async def health_check():
    # Essential for Coolify to see the app as "Healthy"
    return {"status": "online", "network": "Latinum-V4-Beta"}

@app.post("/tasks/submit")
async def submit_task(task: WorkUnit):
    """
    Distributes tasks to the Redis queue for Prospectors to claim.
    """
    await app.state.redis.enqueue_job('process_refinery_task', task.dict())
    return {"status": "queued", "task_id": task.task_id}

@app.get("/network/stats")
async def get_stats():
    # Placeholder for the Shard Scheduler dashboard
    return {
        "active_prospectors": 0,
        "total_complexity_units_processed": 0,
        "quantum_threat_level": "Low"
    }
