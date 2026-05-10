import os
import asyncio
import json
import time
import psycopg2
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from arq import create_pool
from arq.connections import RedisSettings

app = FastAPI(title="Latinum AI Refinery & Vault")

# --- HARDENING: CORS MIDDLEWARE ---
# This allows your WordPress site to pull the Live Ticker data
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Environment Variables
VAULT_DB_URL = os.getenv("VAULT_DB_URL") 

@app.on_event("startup")
async def startup():
    app.state.redis = await create_pool(RedisSettings(host="latinum-db", port=6379))
    if VAULT_DB_URL:
        init_vault_schema()

@app.get("/stats")
async def get_network_stats():
    """
    JSON endpoint for Discord bots and the Website Ticker.
    """
    try:
        conn = psycopg2.connect(VAULT_DB_URL)
        cur = conn.cursor()
        
        # 1. Total Shards Count
        cur.execute("SELECT COUNT(*) FROM shard_history;")
        total_shards = cur.fetchone()[0]
        
        # 2. Total Complexity Units (CUs)
        cur.execute("""
            SELECT 
                SUM(CASE WHEN shard_id LIKE 'REAL-DATA%' THEN 100 ELSE 25 END) 
            FROM shard_history;
        """)
        total_cu = cur.fetchone()[0] or 0
        
        # 3. Active Nodes (Unique signatures in the last 24 hours)
        cur.execute("SELECT COUNT(DISTINCT node_sig) FROM shard_history WHERE finalized_at > NOW() - INTERVAL '24 hours';")
        active_nodes = cur.fetchone()[0]
        
        cur.close()
        conn.close()

        return {
            "network_name": "Latinum Mainnet (Beta)",
            "total_shards_vaulted": total_shards,
            "total_complexity_units": total_cu,
            "active_nodes_24h": active_nodes,
            "status": "Operational",
            "timestamp": time.time()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/results", response_class=HTMLResponse)
async def get_results():
    """Live Web Dashboard for the Vault"""
    try:
        conn = psycopg2.connect(VAULT_DB_URL)
        cur = conn.cursor()
        cur.execute("SELECT shard_id, node_sig, finalized_at FROM shard_history ORDER BY finalized_at DESC LIMIT 50;")
        rows = cur.fetchall()
        
        total_cu = 0
        for row in rows:
            if "REAL-DATA" in row[0] or "QUANTUM" in row[0]:
                total_cu += 100
            else:
                total_cu += 25
        
        cur.close()
        conn.close()

        html = "<html><head><title>Latinum Vault</title></head><body style='font-family:monospace; background:#0d1117; color:#58a6ff; padding:40px;'>"
        html += f"<div style='background:#161b22; padding:20px; border-radius:8px; border:1px solid #30363d; margin-bottom:20px;'>"
        html += f"<h2 style='margin:0; color:#aff5b4;'>💰 Total Yield: {total_cu} Complexity Units</h2>"
        html += f"<p style='color:#8b949e; margin:5px 0 0 0;'>Network: Institutional v4.0 | Status: Operational</p></div>"
        html += "<h1>💎 SHARD ARCHIVE (Last 50)</h1><hr>"
        html += "<table border='1' style='width:100%; border-color:#30363d; border-collapse:collapse; text-align:left;'>"
        html += "<tr style='background:#161b22;'><th>SHARD_ID</th><th>NODE_SIG</th><th>TIMESTAMP</th></tr>"
        for row in rows:
            color = "#aff5b4" if "REAL-DATA" in row[0] else "#58a6ff"
            html += f"<tr style='color:{color};'><td>{row[0]}</td><td>{row[1]}</td><td>{row[2]}</td></tr>"
        html += "</table></body></html>"
        return html
    except Exception as e:
        return f"<h1>❌ Vault Error</h1><p>{str(e)}</p>"

@app.get("/tasks/claim")
async def claim_task(prospector_id: str = Query(None)):
    task_raw = await app.state.redis.lpop("latinum_task_queue")
    if not task_raw: return {"status": "idle"}
    task = json.loads(task_raw)
    work_unit = {"work_unit_id": task.get("task_id"), "assigned_prospector": prospector_id or "UNKNOWN"}
    return {"status": "success", "work_unit": work_unit}

@app.post("/tasks/submit")
async def submit_task(data: dict):
    conn = psycopg2.connect(VAULT_DB_URL)
    cur = conn.cursor()
    cur.execute("INSERT INTO shard_history (shard_id, node_sig, loss_value) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING", (data['task_id'], data['node_id'], 0.0))
    conn.commit()
    cur.close()
    conn.close()
    return {"status": "success"}

def init_vault_schema():
    conn = psycopg2.connect(VAULT_DB_URL)
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS shard_history (shard_id TEXT PRIMARY KEY, node_sig TEXT, loss_value FLOAT, finalized_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);")
    conn.commit()
    cur.close()
    conn.close()
