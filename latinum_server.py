import os
import hmac
import hashlib
import json
import psycopg2
import redis
from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Latinum AI Refinery & Vault")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- CONFIGURATION ---
VAULT_DB_URL = os.getenv("VAULT_DB_URL")
NODE_SECRET = os.getenv("NODE_SECRET", "LATINUM_REFINERY_SECRET_2026")

# Using Public IP to bypass Docker internal networking issues
REDIS_URL = "redis://203.161.58.9:6379/0"

# Setup Redis connection with a retry fail-safe
try:
    r_db = redis.from_url(REDIS_URL, socket_timeout=2)
except:
    r_db = None

@app.get("/tasks/claim")
async def claim_task(prospector_id: str = Query(None)):
    """Pulls shards from the Redis Vault"""
    if r_db:
        try:
            task_raw = r_db.lpop("latinum_task_queue")
            if task_raw:
                task = json.loads(task_raw.decode('utf-8'))
                return {
                    "status": "success", 
                    "work_unit": {
                        "work_unit_id": task.get("task_id"), 
                        "assigned": prospector_id
                    }
                }
        except Exception as e:
            print(f"Redis Error: {e}")
    
    return {"status": "idle", "message": "Queue is empty on Public IP"}

@app.post("/tasks/submit")
async def submit_task(request: Request):
    """Verifies and Vaults the Work"""
    try:
        data = await request.json()
        received_sig = request.headers.get("X-Latinum-Signature")
        
        expected_sig = hmac.new(
            NODE_SECRET.encode(), 
            data['task_id'].encode(), 
            hashlib.sha256
        ).hexdigest()

        if not received_sig or not hmac.compare_digest(received_sig, expected_sig):
            raise HTTPException(status_code=403, detail="Invalid Node Signature")

        # Record to Postgres Vault
        conn = psycopg2.connect(VAULT_DB_URL)
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO shard_history (shard_id, node_sig, loss_value) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING", 
            (data['task_id'], data['node_id'], 0.0)
        )
        conn.commit()
        cur.close()
        conn.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stats")
async def get_stats():
    """Live JSON Feed for Ticker"""
    try:
        conn = psycopg2.connect(VAULT_DB_URL)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM shard_history;")
        shards = cur.fetchone()[0]
        cur.execute("SELECT SUM(CASE WHEN shard_id LIKE 'REAL-DATA%' THEN 100 ELSE 25 END) FROM shard_history;")
        cu = cur.fetchone()[0] or 0
        cur.close()
        conn.close()
        return {"total_shards_vaulted": shards, "total_complexity_units": cu, "status": "Operational", "active_nodes_24h": 1}
    except:
        return {"status": "Offline"}
