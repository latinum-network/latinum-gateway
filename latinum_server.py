import os
import asyncio
import json
import time
from fastapi import FastAPI, HTTPException, Header
from arq import create_pool
from arq.connections import RedisSettings

app = FastAPI(title="Latinum Refinery Gateway")

# Configuration Constants
REDIS_URL = "redis://latinum-db:6379/0"
RECLAMATION_TIMEOUT = 60  # Seconds before a shard is considered "stale"
CHECK_INTERVAL = 10       # Background worker frequency

@app.on_event("startup")
async def startup():
    """Initializes DB connection and starts the Reclamation Worker."""
    try:
        app.state.redis = await create_pool(RedisSettings(host="latinum-db", port=6379))
        
        # Initial Queue Check
        count = await app.state.redis.llen("latinum:pending_shards")
        print(f"✅ REFINERY ONLINE. Shards in queue: {count}")
        
        # Start background recovery logic
        asyncio.create_task(reclamation_worker())
        print(f"🛡️ RECLAMATION WORKER ACTIVE (Timeout: {RECLAMATION_TIMEOUT}s)")
        
    except Exception as e:
        print(f"❌ DATABASE STARTUP ERROR: {e}")

async def reclamation_worker():
    """Background loop to recover lost shards from crashed Prospectors."""
    while True:
        try:
            leases = await app.state.redis.smembers("latinum:processing_leases")
            now = int(time.time())
            
            for lease in leases:
                try:
                    # Lease format: "timestamp:hardware_sig:shard_raw_data"
                    parts = lease.split(":", 2)
                    if len(parts) < 3: continue
                    
                    timestamp, sig, shard_data = parts
                    
                    if now - int(timestamp) > RECLAMATION_TIMEOUT:
                        print(f"⚠️ RECLAIMING: Shard from {sig} timed out. Returning to queue.")
                        await app.state.redis.rpush("latinum:pending_shards", shard_data)
                        await app.state.redis.srem("latinum:processing_leases", lease)
                except Exception:
                    continue
        except Exception as e:
            print(f"🛡️ WORKER ERROR: {e}")
        
        await asyncio.sleep(CHECK_INTERVAL)

@app.get("/")
async def health():
    if not hasattr(app.state, 'redis'): return {"status": "starting"}
    count = await app.state.redis.llen("latinum:pending_shards")
    return {"status": "online", "shards_available": count}

@app.get("/tasks/claim")
async def claim_task(x_hardware_sig: str = Header(None)):
    """Claims a shard and starts the lease timer."""
    if not x_hardware_sig:
        raise HTTPException(status_code=400, detail="X-Hardware-Sig missing.")

    shard_raw = await app.state.redis.lpop("latinum:pending_shards")
    if not shard_raw:
        return {"status": "idle"}

    # Create lease: "timestamp:node_id:data"
    lease_entry = f"{int(time.time())}:{x_hardware_sig}:{shard_raw}"
    await app.state.redis.sadd("latinum:processing_leases", lease_entry)

    return {
        "status": "success", 
        "work_unit": json.loads(shard_raw),
        "lease_seconds": RECLAMATION_TIMEOUT
    }

@app.post("/tasks/submit")
async def submit_task(payload: dict, x_hardware_sig: str = Header(None)):
    """Finalizes work and removes the lease with resilient matching."""
    if not x_hardware_sig:
        raise HTTPException(status_code=400, detail="X-Hardware-Sig missing.")
    
    shard_id = payload.get("shard_id")
    if not shard_id:
        raise HTTPException(status_code=422, detail="Missing shard_id in payload.")

    leases = await app.state.redis.smembers("latinum:processing_leases")
    
    for lease in leases:
        # 1. Ensure the lease belongs to the reporting node
        if f":{x_hardware_sig}:" in lease:
            # 2. Resilient check: Does the lease data contain the shard_id?
            if shard_id in lease:
                await app.state.redis.srem("latinum:processing_leases", lease)
                print(f"✨ SUCCESS: {x_hardware_sig} finalized Shard {shard_id}")
                return {"status": "accepted", "shard_id": shard_id}
            
    # If we find no match, the lease either expired or was mislabeled
    print(f"
