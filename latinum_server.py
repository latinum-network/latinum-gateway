import os
import asyncio
import json
import time
from fastapi import FastAPI, HTTPException, Header
from arq import create_pool
from arq.connections import RedisSettings

app = FastAPI(title="Latinum AI Refinery")

# V3 Parameters
RECLAMATION_TIMEOUT = 300 # 5 mins for AI training
CHECK_INTERVAL = 30

@app.on_event("startup")
async def startup():
    try:
        app.state.redis = await create_pool(RedisSettings(host="latinum-db", port=6379))
        print("🧠 AI REFINERY ONLINE")
        asyncio.create_task(reclamation_worker())
    except Exception as e:
        print(f"❌ DATABASE STARTUP ERROR: {e}")

async def reclamation_worker():
    while True:
        try:
            leases = await app.state.redis.smembers("latinum:processing_leases")
            now = int(time.time())
            for lease_bytes in leases:
                try:
                    lease = lease_bytes.decode('utf-8')
                    parts = lease.split(":", 2)
                    if len(parts) < 3: continue
                    timestamp, sig, shard_data = parts
                    if now - int(timestamp) > RECLAMATION_TIMEOUT:
                        await app.state.redis.rpush("latinum:pending_training_shards", shard_data)
                        await app.state.redis.srem("latinum:processing_leases", lease_bytes)
                except Exception: continue
        except Exception as e: pass
        await asyncio.sleep(CHECK_INTERVAL)

@app.get("/tasks/claim")
async def claim_task(x_hardware_sig: str = Header(None), x_vram_available: int = Header(0)):
    if not x_hardware_sig: raise HTTPException(status_code=400)
    
    # 1. Try to get an AI Training Shard first
    shard_raw = await app.state.redis.lpop("latinum:pending_training_shards")
    
    if shard_raw:
        task = json.loads(shard_raw)
        # 2. VRAM Gatekeeping
        if x_vram_available < task.get("vram_required", 0):
            await app.state.redis.rpush("latinum:pending_training_shards", shard_raw)
            return {"status": "idle", "reason": "Insufficient VRAM"}
        
        lease_entry = f"{int(time.time())}:{x_hardware_sig}:{shard_raw}"
        await app.state.redis.sadd("latinum:processing_leases", lease_entry)
        return {"status": "success", "task": task}

    return {"status": "idle"}

@app.post("/tasks/submit")
async def submit_task(payload: dict, x_hardware_sig: str = Header(None)):
    shard_id = payload.get("shard_id")
    leases = await app.state.redis.smembers("latinum:processing_leases")
    for lease_bytes in leases:
        lease = lease_bytes.decode('utf-8')
        if f":{x_hardware_sig}:" in lease and shard_id in lease:
            await app.state.redis.srem("latinum:processing_leases", lease_bytes)
            print(f"✨ AI SUCCESS: {x_hardware_sig} finalized {shard_id}")
            return {"status": "accepted"}
    raise HTTPException(status_code=404)
