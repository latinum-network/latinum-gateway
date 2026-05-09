import os
import asyncio
import json
import time
import psycopg2
from fastapi import FastAPI, HTTPException, Header
from arq import create_pool
from arq.connections import RedisSettings

app = FastAPI(title="Latinum AI Refinery & Vault")

# Configuration
RECLAMATION_TIMEOUT = 300 
CHECK_INTERVAL = 30
VAULT_DB_URL = os.getenv("VAULT_DB_URL") # Provided via Coolify Env

@app.on_event("startup")
async def startup():
    try:
        app.state.redis = await create_pool(RedisSettings(host="latinum-db", port=6379))
        # Initialize Vault Schema if URL exists
        if VAULT_DB_URL:
            init_vault_schema()
        print("🧠 AI REFINERY & 🔒 VAULT ONLINE")
        asyncio.create_task(reclamation_worker())
    except Exception as e:
        print(f"❌ STARTUP ERROR: {e}")

def init_vault_schema():
    """Ensures the permanent Vault tables exist."""
    try:
        conn = psycopg2.connect(VAULT_DB_URL)
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS shard_history (
                shard_id TEXT PRIMARY KEY,
                node_sig TEXT,
                loss_value FLOAT,
                finalized_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"⚠️ SCHEMA ERROR: {e}")

def archive_to_vault(shard_id, node_sig, loss):
    """Commits finalized results to the permanent SQL Vault."""
    if not VAULT_DB_URL: return
    try:
        conn = psycopg2.connect(VAULT_DB_URL)
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO shard_history (shard_id, node_sig, loss_value) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
            (shard_id, node_sig, loss)
        )
        conn.commit()
        cur.close()
        conn.close()
        print(f"🔒 ARCHIVED: {shard_id} secured in Vault.")
    except Exception as e:
        print(f"⚠️ ARCHIVE ERROR: {e}")

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
    
    shard_raw = await app.state.redis.lpop("latinum:pending_training_shards")
    if shard_raw:
        task = json.loads(shard_raw)
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
    loss = payload.get("loss", 0.0)
    
    leases = await app.state.redis.smembers("latinum:processing_leases")
    for lease_bytes in leases:
        lease = lease_bytes.decode('utf-8')
        if f":{x_hardware_sig}:" in lease and shard_id in lease:
            # 1. Clear the ephemeral lease
            await app.state.redis.srem("latinum:processing_leases", lease_bytes)
            # 2. Permanent Archival
            archive_to_vault(shard_id, x_hardware_sig, loss)
            print(f"✨ SUCCESS: {x_hardware_sig} finalized {shard_id}")
            return {"status": "accepted"}
    raise HTTPException(status_code=404)
