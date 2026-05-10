import os, hmac, hashlib, json, psycopg2, redis, uvicorn
from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Latinum AI Refinery & Vault")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# --- CONFIGURATION ---
VAULT_DB_URL = os.getenv("VAULT_DB_URL")
NODE_SECRET = os.getenv("NODE_SECRET", "LATINUM_REFINERY_SECRET_2026")
REDIS_URL = os.getenv("REDIS_URL", "redis://latinum-db-ao3ummhehyagp3e1qttz47if:6379/0")

# Setup Redis connection
r_db = None
try:
    r_db = redis.from_url(REDIS_URL, socket_timeout=3)
    r_db.ping()
    print(f"✅ DATABASE CONNECTED at {REDIS_URL}")
except Exception as e:
    print(f"❌ DATABASE OFFLINE: {e}")

@app.get("/tasks/claim")
async def claim_task(prospector_id: str = Query(None)):
    if not r_db: return {"status": "error", "message": "DB Offline"}
    try:
        task_raw = r_db.lpop("latinum_task_queue")
        if task_raw:
            task = json.loads(task_raw.decode('utf-8'))
            return {"status": "success", "work_unit": {"work_unit_id": task.get("task_id"), "assigned": prospector_id}}
    except Exception as e: return {"status": "error", "message": str(e)}
    return {"status": "idle", "message": "Queue empty"}

@app.post("/tasks/submit")
async def submit_task(request: Request):
    try:
        data = await request.json()
        sig = hmac.new(NODE_SECRET.encode(), data['task_id'].encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(request.headers.get("X-Latinum-Signature", ""), sig):
            raise HTTPException(status_code=403)
        conn = psycopg2.connect(VAULT_DB_URL)
        cur = conn.cursor()
        cur.execute("INSERT INTO shard_history (shard_id, node_sig, loss_value) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING", 
                    (data['task_id'], data['node_id'], 0.0))
        conn.commit()
        cur.close(); conn.close()
        return {"status": "success"}
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

@app.get("/stats")
async def get_stats():
    try:
        conn = psycopg2.connect(VAULT_DB_URL)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM shard_history;")
        shards = cur.fetchone()[0]
        cur.close(); conn.close()
        return {"total_shards_vaulted": shards, "total_complexity_units": shards * 100, "status": "Operational", "active_nodes_24h": 1}
    except: return {"status": "Syncing"}

# 🚀 CRITICAL ENTRY POINT: Forces the app to bind to Port 8000 inside Coolify
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
