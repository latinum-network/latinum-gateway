import os
import hmac
import hashlib
import json
import psycopg2
import redis
from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware

# 1. Initialize App
app = FastAPI(title="Latinum AI Refinery & Vault")

# 2. Add Bulletproof CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["*"]
)

# --- CONFIGURATION ---
VAULT_DB_URL = os.getenv("VAULT_DB_URL")
NODE_SECRET = os.getenv("NODE_SECRET", "LATINUM_REFINERY_SECRET_2026")

# Using the Docker Gateway IP to ensure a guaranteed connection
REDIS_URL = os.getenv("REDIS_URL", "redis://172.18.0.1:6379/0")

# Setup Redis connection
r_db = redis.from_url(REDIS_URL)

@app.get("/tasks/claim")
async def claim_task(prospector_id: str = Query(None)):
    """Pull a task from Redis and hand it to a Prospector"""
    try:
        task_raw = r_db.lpop("latinum_task_queue")
        if not task_raw:
            return {"status": "idle"}
        
        # DECODE: Convert Redis bytes to UTF-8 string before loading JSON
        task_str = task_raw.decode('utf-8')
        task = json.loads(task_str)
        
        return {
            "status": "success", 
            "work_unit": {
                "work_unit_id": task.get("task_id"), 
                "assigned_prospector": prospector_id or "UNKNOWN"
            }
        }
    except Exception as e:
        print(f"Claim Error: {e}")
        return {"status": "error", "message": str(e)}

@app.post("/tasks/submit")
async def submit_task(request: Request):
    """Verifies the Signature and Vaults the Work"""
    try:
        data = await request.json()
        received_sig = request.headers.get("X-Latinum-Signature")
        
        # Verify the HMAC Signature (The 'Quantum Seal')
        expected_sig = hmac.new(
            NODE_SECRET.encode(), 
            data['task_id'].encode(), 
            hashlib.sha256
        ).hexdigest()

        if not received_sig or not hmac.compare_digest(received_sig, expected_sig):
            raise HTTPException(status_code=403, detail="Invalid Node Signature")

        # Vault the shard to Postgres
        conn = psycopg2.connect(VAULT_DB_URL)
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO shard_history (shard_id, node_sig, loss_value) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING", 
            (data['task_id'], data['node_id'], 0.0)
        )
        conn.commit()
        cur.close()
        conn.close()
        
        return {"status": "success", "verified": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stats")
async def get_stats():
    """Live JSON Feed for the Website Ticker"""
    try:
        conn = psycopg2.connect(VAULT_DB_URL)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM shard_history;")
        shards = cur.fetchone()[0]
        
        cur.execute("SELECT SUM(CASE WHEN shard_id LIKE 'REAL-DATA%' THEN 100 ELSE 25 END) FROM shard_history;")
        cu = cur.fetchone()[0] or 0
        
        cur.close()
        conn.close()
        
        return {
            "network_name": "Latinum Mainnet (Beta)",
            "total_shards_vaulted": shards,
            "total_complexity_units": cu,
            "active_nodes_24h": 1,
            "status": "Operational"
        }
    except Exception as e:
        return {"error": str(e)}

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

        html = f"<html><body style='font-family:monospace; background:#0d1117; color:#58a6ff; padding:40px;'>"
        html += "<h1>💎 LATINUM VAULT ARCHIVE</h1><hr>"
        html += "<table border='1' style='width:100%; border-collapse:collapse;'>"
        for row in rows:
            html += f"<tr><td>{row[0]}</td><td>{row[1]}</td><td>{row[2]}</td></tr>"
        html += "</table></body></html>"
        return html
    except Exception as e:
        return f"<h1>Error</h1><p>{str(e)}</p>"

def init_vault_schema():
    """Ensures the Database is ready for Shards"""
    conn = psycopg2.connect(VAULT_DB_URL)
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS shard_history (shard_id TEXT PRIMARY KEY, node_sig TEXT, loss_value FLOAT, finalized_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);")
    conn.commit()
