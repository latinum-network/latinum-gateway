import os
import hmac
import hashlib
import json
import psycopg2
import redis
from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Latinum AI Refinery & Vault")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

VAULT_DB_URL = os.getenv("VAULT_DB_URL")
NODE_SECRET = os.getenv("NODE_SECRET", "LATINUM_REFINERY_SECRET_2026")
REDIS_URL = os.getenv("REDIS_URL", "redis://latinum-db:6379/0")

# Setup Redis connection for the queue
r_db = redis.from_url(REDIS_URL)

@app.get("/tasks/claim")
async def claim_task(prospector_id: str = Query(None)):
    """Pull a task from Redis and hand it to a Prospector"""
    task_raw = r_db.lpop("latinum_task_queue")
    if not task_raw:
        return {"status": "idle"}
    
    task = json.loads(task_raw)
    return {
        "status": "success", 
        "work_unit": {
            "work_unit_id": task.get("task_id"), 
            "assigned_prospector": prospector_id or "UNKNOWN"
        }
    }

@app.post("/tasks/submit")
async def submit_task(request: Request):
    data = await request.json()
    received_sig = request.headers.get("X-Latinum-Signature")
    
    # Verification Logic
    expected_sig = hmac.new(
        NODE_SECRET.encode(), 
        data['task_id'].encode(), 
        hashlib.sha256
    ).hexdigest()

    if not hmac.compare_digest(received_sig, expected_sig):
        raise HTTPException(status_code=403, detail="Invalid Signature")

    # Vaulting Logic
    conn = psycopg2.connect(VAULT_DB_URL)
    cur = conn.cursor()
    cur.execute("INSERT INTO shard_history (shard_id, node_sig, loss_value) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING", 
                (data['task_id'], data['node_id'], 0.0))
    conn.commit()
    cur.close()
    conn.close()
    return {"status": "success"}

@app.get("/stats")
async def get_stats():
    """Stats for the Live Ticker"""
    conn = psycopg2.connect(VAULT_DB_URL)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM shard_history;")
    shards = cur.fetchone()[0]
    cur.execute("SELECT SUM(CASE WHEN shard_id LIKE 'REAL-DATA%' THEN 100 ELSE 25 END) FROM shard_history;")
    cu = cur.fetchone()[0] or 0
    cur.close()
    conn.close()
    return {"total_shards_vaulted": shards, "total_complexity_units": cu, "status": "Operational", "active_nodes_24h": 1}
