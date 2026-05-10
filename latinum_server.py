import os
import asyncio
import json
import time
import psycopg2
from fastapi import FastAPI, HTTPException, Header
from arq import create_pool
from arq.connections import RedisSettings

app = FastAPI(title="Latinum AI Refinery & Vault")

# --- CONFIGURATION ---
RECLAMATION_TIMEOUT = 300
CHECK_INTERVAL = 30
VAULT_DB_URL = os.getenv("VAULT_DB_URL") # Provided via Coolify Env

# --- STARTUP LOGIC ---
@app.on_event("startup")
async def startup():
    try:
        # Connect to internal Coolify Redis
        app.state.redis = await create_pool(RedisSettings(host="latinum-db", port=6379))
        
        # Initialize SQL Vault Schema
        if VAULT_DB_URL:
            init_vault_schema()
            
        print("🧠 AI REFINERY & 🔒 VAULT ONLINE")
        # Start the background worker for task reclamation
        asyncio.create_task(reclamation_worker())
    except Exception as e:
        print(f"❌ STARTUP ERROR: {e}")

# --- TASK MANAGEMENT (THE NEW ENDPOINT) ---

@app.get("/tasks/claim")
async def claim_task(prospector_id: str):
    """
    Claims a granular Work Unit for a Prospector node.
    Matches Latinum V4.0 requirements for CU Tiering and Result Hashing.
    """
    try:
        # Pop a task from the Redis queue (LPOP)
        task_raw = await app.state.redis.lpop("latinum_task_queue")
        
        if not task_raw:
            return {"status": "idle", "message": "No Work Units currently available."}

        task = json.loads(task_raw)
        
        # Structure the payload for the Prospector
        work_unit = {
            "work_unit_id": task.get("task_id", f"LAT-WU-{int(time.time())}"),
            "assigned_prospector": prospector_id,
            "execution_environment": "DinD",
            "cu_reward_tier": task.get("cu_tier", 10), # Default 10 CUs
            "validation_protocol": {
                "method": "sha256_result_hashing",
                "consensus_required": True
            }
        }
        
        # Log the claim for reclamation tracking
        await app.state.redis.setex(f"active_task:{work_unit['work_unit_id']}", RECLAMATION_TIMEOUT, prospector_id)
        
        print(f"🚀 TASK CLAIMED: {work_unit['work_unit_id']} by {prospector_id}")
        return {"status": "success", "work_unit": work_unit}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Gateway Claim Error: {str(e)}")

# --- VAULT & RECLAMATION LOGIC ---

def init_vault_schema():
    """Ensures the permanent Vault tables exist in PostgreSQL."""
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
