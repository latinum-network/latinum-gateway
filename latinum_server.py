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
VAULT_DB_URL = os.getenv("VAULT_DB_URL") 

# --- STARTUP LOGIC ---
@app.on_event("startup")
async def startup():
    try:
        # Connect to internal Coolify Redis
        app.state.redis = await create_pool(RedisSettings(host="latinum-db", port=6379))
        
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
    Claims a granular Work Unit. 
    'prospector_id' is now optional to prevent 400 Bad Request errors.
    """
    try:
        # Pull task from Redis queue
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
        
        # Track active task
        await app.state.redis.setex(f"active_task:{work_unit['work_unit_id']}", RECLAMATION_TIMEOUT, node_name)
        
        print(f"🚀 TASK CLAIMED: {work_unit['work_unit_id']} by {node_name}")
        return {"status": "success", "work_unit": work_unit}
        
    except Exception as e:
        # Return error as JSON instead of crashing
        return {"status": "error", "message": str(e)}

# --- VAULT & RECLAMATION LOGIC ---

def init_vault_schema():
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
        print(f"⚠️ VAULT SCHEMA ERROR: {e}")

async def reclamation_worker():
    while True:
        await asyncio.sleep(CHECK_INTERVAL)

@app.get("/health")
async def health_check():
    return {"status": "online", "network": "Latinum Refinery"}
