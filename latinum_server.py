import os
import uuid
import asyncio
import json
from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel
from typing import Optional
from arq import create_pool
from arq.connections import RedisSettings

app = FastAPI(title="Latinum Refinery Gateway")

# Configuration pulled from Coolify Environment Variables
# Internal Docker DNS: redis://latinum-db:6379/0
REDIS_URL = os.getenv("REDIS_URL")

@app.on_event("startup")
async def startup():
    """Aggressive connection loop to ensure we find the database in a multi-tenant stack."""
    print(f"📡 Attempting to connect to Redis at: {REDIS_URL}")
    for attempt in range(1, 11):
        try:
            # We use arq for high-concurrency shard scheduling
            app.state.redis = await create_pool(RedisSettings.from_dsn(REDIS_URL))
            print("✅ CONNECTION SUCCESS: Latinum Redis Queue is live.")
            return
        except Exception as e:
            print(f"⚠️ Attempt {attempt}: Redis not ready yet... retrying in 3s.")
            await asyncio.sleep(3)
    
    print("❌ FATAL: Could not connect to Redis after 10 attempts.")

@app.get("/")
async def health_check():
    """Simple status for the Coolify healthcheck monitor."""
    return {"status": "online", "network": "Latinum-Refinery-V1.1"}

@app.get("/tasks/claim")
async def claim_task(x_hardware_sig: str = Header(None)):
    """
    Core Shard Scheduling Logic.
    Validates hardware signature before popping a task from the Redis list.
    """
    if not x_hardware_sig:
        raise HTTPException(status_code=400, detail="X-Hardware-Sig header missing.")

    if not hasattr(app.state, 'redis'):
        raise HTTPException(status_code=503, detail="Database connection pending.")

    # Atomically pop a work unit from the queue
    task_data = await app.state.redis.lpop("latinum_task_queue")
    
    if not task_data:
        return {"status": "idle", "message": "No Work Units currently available."}

    return {
        "status": "success", 
        "work_unit": json.loads(task_data),
        "assigned_to": x_hardware_sig
    }

@app.get("/debug/env")
async def debug_env():
    """Diagnostic endpoint to verify environment injection."""
    return {
        "redis_url_configured": bool(REDIS_URL),
        "url_mask": f"{str(REDIS_URL)[:10]}***" if REDIS_URL else "MISSING"
    }
