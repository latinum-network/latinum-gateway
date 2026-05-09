import os
import uuid
import asyncio
import json
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
from arq import create_pool
from arq.connections import RedisSettings

app = FastAPI(title="Latinum Refinery Gateway")
REDIS_URL = os.getenv("REDIS_URL")

class WorkUnit(BaseModel):
    task_id: str = str(uuid.uuid4())
    payload: str
    cu_tier: int = 10
    encrypted_payload: Optional[str] = None

@app.on_event("startup")
async def startup():
    if not REDIS_URL:
        print("❌ REDIS_URL not found in environment variables!")
        return
    try:
        app.state.redis = await create_pool(RedisSettings.from_dsn(REDIS_URL))
        print("✅ Successfully connected to Latinum Redis Queue")
    except Exception as e:
        print(f"❌ Redis Connection Error: {e}")

@app.get("/")
async def health_check():
    return {"status": "online", "network": "Latinum-V4-Beta"}

@app.post("/tasks/submit")
async def submit_task(task: WorkUnit):
    if not hasattr(app.state, 'redis'):
        raise HTTPException(status_code=503, detail="Redis connection not established")
    
    # Push to the specific queue name NotebookLM recommended
    await app.state.redis.rpush("latinum_task_queue", task.json())
    return {"status": "queued", "task_id": task.task_id}

@app.get("/tasks/claim")
async def claim_task(prospector_id: str):
    if not hasattr(app.state, 'redis'):
        raise HTTPException(status_code=503, detail="Redis connection not established")

    task_data = await app.state.redis.lpop("latinum_task_queue")

    if not task_data:
        return {"status": "idle", "message": "No Work Units currently available."}

    task = json.loads(task_data)
    return {
        "status": "success",
        "work_unit": {
            "work_unit_id": task.get("task_id"),
            "assigned_prospector": prospector_id,
            "payload_ml_kem_encrypted": task.get("encrypted_payload") or task.get("payload"),
            "execution_environment": "DinD",
            "cu_reward_tier": task.get("cu_tier", 10),
            "validation_protocol": {
                "method": "sha256_result_hashing",
                "consensus_required": True
            }
        }
    }

@app.get("/network/stats")
async def get_stats():
    return {"status": "Operational", "version": "4.0.1-Refinery"}
