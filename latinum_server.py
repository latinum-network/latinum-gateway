import os
import asyncio
import json
from fastapi import FastAPI, HTTPException, Header
from arq import create_pool
from arq.connections import RedisSettings

app = FastAPI(title="Latinum Refinery Gateway")

# Force the connection to the internal Redis container name
# This bypasses any environment variable lag in Coolify
REDIS_URL = "redis://latinum-db:6379/0"

@app.on_event("startup")
async def startup():
    """Direct connection to the DB with an immediate health check."""
    try:
        app.state.redis = await create_pool(RedisSettings(host="latinum-db", port=6379))
        # Internal verify: Check the exact key name we are using
        count = await app.state.redis.llen("latinum:pending_shards")
        print(f"✅ REFINERY ONLINE. Shards detected in queue: {count}")
    except Exception as e:
        print(f"❌ DATABASE ERROR: {e}")

@app.get("/")
async def health():
    return {"status": "online", "key_monitored": "latinum:pending_shards"}

@app.get("/tasks/claim")
async def claim_task(x_hardware_sig: str = Header(None)):
    """Claims a shard from the unified 'latinum:pending_shards' queue."""
    if not x_hardware_sig:
        raise HTTPException(status_code=400, detail="X-Hardware-Sig header missing.")

    if not hasattr(app.state, 'redis'):
        raise HTTPException(status_code=503, detail="Database connection pending.")

    # ATOMIC FIX: Using the exact key we used in the terminal seed command
    task_data = await app.state.redis.lpop("latinum:pending_shards")
    
    if not task_data:
        return {"status": "idle", "message": "Queue empty at latinum:pending_shards"}

    return {
        "status": "success", 
        "work_unit": task_data,
        "assigned_to": x_hardware_sig
    }
