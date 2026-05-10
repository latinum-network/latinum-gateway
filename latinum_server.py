import os
import asyncio
import json
import time
import psycopg2
from fastapi import FastAPI, HTTPException, Query
from arq import create_pool
from arq.connections import RedisSettings

app = FastAPI(title="Latinum AI Refinery & Vault")

# --- CONFIGURATION ---
RECLAMATION_TIMEOUT = 300
CHECK_INTERVAL = 30
# Loaded from your Coolify Environment Variables
VAULT_DB_URL = os.getenv("VAULT_DB_URL") 

# --- STARTUP LOGIC ---
@app.on_event("startup")
async def startup():
    try:
        # Connect to internal Coolify Redis (latinum-db)
        app.state.redis = await create_pool(RedisSettings(host="latinum-db", port=6379))
        
        # Initialize PostgreSQL Schema if URL is present
        if VAULT_DB_URL:
            init_vault_schema()
            
        print("🧠 AI REFINERY & 🔒 VAULT ONLINE")
        asyncio.create_task(reclamation_worker())
    except Exception as e:
        print(f"❌ STARTUP ERROR: {e}")

# --- TASK MANAGEMENT ---

@app.get("/tasks/claim")
async def claim_task(prospector_id: str = Query(None)):
    """
    Claims a granular Work Unit from the Redis queue.
    """
    try:
        task_raw = await app.state.redis.lpop("latinum_task_queue")
        
        if not task_raw:
            return {"status": "idle", "message": "No Work Units available."}

        task = json.loads(task_raw)
        node_name = prospector_id or "UNKNOWN_NODE"
        
        work_unit = {
            "work_unit_id": task.get("task_id", f"LAT-WU-{int(time.time())}"),
            "assigned_prospector": node_name,
            "cu_reward_tier": task.get("cu_tier", 10),
            "validation_protocol": {
                "method": "sha256_result_hashing",
                "consensus_required": True
            }
        }
        
        # Track active task in Redis for reclamation timeout
        await app.state.redis.setex(f"active_task:{work_unit['work_unit_id']}", RECLAMATION_TIMEOUT, node_name)
        
        print(f"🚀 TASK CLAIMED: {work_unit['work_unit_id']} by {node_name}")
        return {"status": "success", "work_unit": work_unit}
        
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/tasks/submit")
async def submit_task(data: dict):
    """
    Receives refinement results from Prospectors and archives them in the Vault.
    """
    try:
        if not VAULT_DB_URL:
            raise HTTPException(status_code=500, detail="Vault Database not configured.")

        conn = psycopg2.connect(VAULT_DB_URL)
        cur = conn.cursor()
        
        # Insert result into shard_history
        cur.execute(
            "INSERT INTO shard_history (shard_id, node_sig, loss_value) VALUES (%s, %s, %s)",
            (data.get('task_id'), data.get('node_id'), 0.0) 
        )
        
        conn.commit()
        cur.close()
        conn.close()
        
        print(f"🔒 RESULT VAULTED: {data.get('task_id')} from {data.get('node_id')}")
        return {"status": "success", "message": "Result archived in Vault."}
    except Exception as e:
        print(f"❌ VAULT ERROR: {e}")
        return {"status": "error", "message": str(e)}

# --- VAULT SCHEMA & RECLAMATION ---

def init_vault_schema():
    """Ensures the PostgreSQL table exists for result archiving."""
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
        print("🏛️ Vault Schema Verified.")
    except Exception as e:
        print(f"⚠️ VAULT SCHEMA ERROR: {e}")

async def reclamation_worker():
    """Background task to monitor stuck work units (Placeholder logic)."""
    while True:
        await asyncio.sleep(CHECK_INTERVAL)

@app.get("/health")
async def health_check():
    return {"status": "online", "network": "Latinum Refinery"}
