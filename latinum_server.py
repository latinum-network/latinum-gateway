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

# DYNAMIC REDIS: This pulls the connection string directly from Coolify's environment
# If not found, it defaults to the local loopback for safety
REDIS_URL = "redis://172.18.0.1:6379/0"

print(f"📡 NODE STARTUP: Attempting connection to Redis at {REDIS_URL}")

try:
    r_db = redis.from_url(REDIS_URL, socket_timeout=3)
    # Test connection immediately
    r_db.ping()
    print("✅ DATABASE CONNECTED: Bridge is stable.")
except Exception as e:
    print(f"❌ DATABASE OFFLINE: {e}")
    r_db = None

@app.get("/tasks/claim")
async def claim_task(prospector_id: str = Query(None)):
    """Pulls shards from the Redis queue"""
    if not r_db:
        return {"status": "error", "message": "API disconnected from Database"}
        
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
        return {"status": "error", "message": str(e)}
    
    return {"status": "idle", "message": "Queue is empty."}

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
        return {"total_shards_vaulted": shards, "total_complexity_units": cu, "status": "Operational"}
    except:
        return {"status": "Offline"}

@app.get("/results", response_class=HTMLResponse)
async def get_results():
    """Live Web Dashboard for the Vault"""
    try:
        conn = psycopg2.connect(VAULT_DB_URL)
        cur = conn.cursor()
        cur.execute("SELECT shard_id, node_sig, finalized_at FROM shard_history ORDER BY finalized_at DESC LIMIT 50;")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        html = "<html><body style='font-family:monospace; background:#0d1117; color:#58a6ff; padding:40px;'><h1>💎 LATINUM VAULT</h1><hr>"
        html += "<table border='1' style='width:100%; border-collapse:collapse;'>"
        for row in rows: html += f"<tr><td>{row[0]}</td><td>{row[1]}</td><td>{row[2]}</td></tr>"
        return html + "</table></body></html>"
    except Exception as e:
        return f"<h1>Syncing...</h1>"
