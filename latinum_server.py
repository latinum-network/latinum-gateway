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
CHECK_INTERVAL = 10       # How often the worker checks for dead leases

@app.on_event("startup")
async def startup():
    """Initializes DB connection and starts the Reclamation Worker."""
    try:
        # Direct connection to the internal Redis container
        app.state.redis = await create_pool(RedisSettings(host="latinum-db", port=6379))
        
        # Immediate Health Check
        count = await app.state.redis.llen("latinum:pending_shards")
        print(f"✅ REFINERY ONLINE. Shards in queue: {count}")
        
        # Start the background Reclamation Worker
        asyncio.create_task(reclamation_worker())
        print(f"🛡️ RECLAMATION WORKER ACTIVE (Timeout: {RECLAMATION_TIMEOUT}s)")
        
    except Exception as e:
        print(f"❌ DATABASE STARTUP ERROR: {e}")

async def reclamation_worker():
    """Background loop to recover lost shards from crashed Prospectors."""
    while True:
        try:
            # Get all active leases
            leases = await app.state.redis.smembers("latinum:processing_leases")
            now = int(time.time())
            
            for lease in leases:
                # Format: "timestamp:hardware_sig:shard_data"
                try:
                    parts = lease.split(":", 2)
                    if len(parts) < 3: continue
                    
                    timestamp, sig, shard_data = parts
                    
                    # If lease age exceeds timeout, reclaim it
                    if now - int(timestamp) > RECLAMATION_TIMEOUT:
                        print(f"⚠️ RECLAIMING: Shard from {sig} timed out. Returning to queue.")
                        
                        # Atomic Move: Put back in pending and remove the dead lease
                        await app.state.redis.rpush("latinum:pending_shards", shard_data)
                        await app.state.redis.srem("latinum:processing_leases", lease)
                except ValueError:
                    continue
                    
        except Exception as e:
            print(f"🛡️ WORKER ERROR: {e}")
        
        await asyncio.sleep(CHECK_INTERVAL)

@app.get("/")
async def health():
    """Service health check for Coolify monitoring."""
    if not hasattr(app.state, 'redis'):
        return {"status": "starting"}
    count = await app.state.redis.llen("latinum:pending_shards")
    return {"status": "online", "shards_available": count}

@app.get("/tasks/claim")
async def claim_task(x_hardware_sig: str = Header(None)):
    """Claims a shard and starts a 60-second lease."""
    if not x_hardware_sig:
        raise HTTPException(status_code=400, detail="X-Hardware-Sig header missing.")

    if not hasattr(app.state, 'redis'):
        raise HTTPException(status_code=503, detail="Database connection pending.")

    # 1. Pop shard from the main queue
    shard_raw = await app.state.redis.lpop("latinum:pending_shards")
    
    if not shard_raw:
        return {"status": "idle", "message": "No shards available."}

    # 2. Create a lease entry in the processing set
    # Format: "timestamp:node_id:data"
    lease_entry = f"{int(time.time())}:{x_hardware_sig}:{shard_raw}"
    await app.state.redis.sadd("latinum:processing_leases", lease_entry)

    # 3. Return shard to Prospector
    return {
        "status": "success", 
        "work_unit": json.loads(shard_raw),
        "lease_seconds": RECLAMATION_TIMEOUT
    }
