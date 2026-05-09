import os
import uuid
import asyncio
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from arq import create_pool
from arq.connections import RedisSettings

app = FastAPI(title="Latinum Refinery Gateway")

# Configuration: We pull the URL directly from your Coolify Env Variables
REDIS_URL = os.getenv("REDIS_URL")

class WorkUnit(BaseModel):
    task_id: str = str(uuid.uuid4())
    payload: str
    complexity_target: int = 10
    model_required: str = "gemma"

@app.on_event("startup")
async def startup():
    """
    Connects to the Redis Queue with a retry loop to prevent 
    startup crashes during deployment.
    """
    retry_count = 0
    max_retries = 5
    
    while retry_count < max_retries:
        try:
            app.state.redis = await create_pool(RedisSettings.from_dsn(REDIS_URL))
            print("✅ Successfully connected to Latinum Redis Queue")
            return
        except Exception as e:
            retry_count += 1
            print(f"⚠️ Redis not ready (Attempt {retry_count}/{max_retries}). Retrying in 2s...")
            await asyncio.sleep(2)
            
    print("❌ Could not connect to Redis. Check your REDIS_URL environment variable.")

@app.get("/")
async def health_check():
    """
    Standard health check for Coolify. 
    Returns 200 OK to stop the 'Restarting' loop.
    """
    return {"status": "online", "network": "Latinum-V4-Beta"}

@app.post("/tasks/submit")
async def submit_task(task: WorkUnit):
    """
    Distributes tasks to the Redis queue for Prospectors to claim.
    """
    if not hasattr(app.state, 'redis'):
        raise HTTPException(status_code=503, detail="Redis connection not established")
        
    await app.state.redis.enqueue_job('process_refinery_task', task.dict())
    return {"status": "queued", "task_id": task.task_id}

@app.get("/network/stats")
async def get_stats():
    return {
        "active_prospectors": 0,
        "total_complexity_units_processed": 0,
        "status": "Operational"
    }
