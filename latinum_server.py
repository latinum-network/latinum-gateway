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
# This pulls from your Coolify Environment Variable
REDIS_URL = os.getenv("REDIS_URL")

@app.on_event("startup")
async def startup():
    """Aggressive connection loop to ensure we find the database."""
    print(f"📡 Attempting to connect to Redis at: {REDIS_URL}")
    for attempt in range(1, 11):
        try:
            app.state.redis = await create_pool(RedisSettings.from_dsn(REDIS_URL))
            print("✅ CONNECTION SUCCESS: Latinum Redis Queue is live.")
            return
        except Exception as e:
            print(f"⚠️ Attempt {attempt}: Redis not ready yet... retrying in 3s.")
            await asyncio.sleep(3)
    print("❌ FATAL: Could not connect to Redis after 10 attempts.")

@app.get("/")
async def health_check():
    return {"status": "online", "network": "Latinum-V4-Beta"}

@app.get("/tasks/claim")
async def claim_task(prospector_id: str):
    if not hasattr(app.state, 'redis'):
        # This is the error you keep seeing
        raise HTTPException(status_code=503, detail="Redis connection not established")

    task_data = await app.state.redis.lpop("latinum_task_queue")
    if not task_data:
        return {"status": "idle", "message": "No Work Units currently available."}

    return {"status": "success", "work_unit": json.loads(task_data)}

# Add a simple ping test to see what the server "sees"
@app.get("/debug/env")
async def debug_env():
    return {"redis_url_configured": bool(REDIS_URL), "url_start": str(REDIS_URL)[:15]}
