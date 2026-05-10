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

# 1. Initialize App
app = FastAPI(title="Latinum AI Refinery & Vault")

# 2. Add CORS Middleware IMMEDIATELY after init
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["*"]
)

VAULT_DB_URL = os.getenv("VAULT_DB_URL") 

@app.on_event("startup")
async def startup():
    app.state.redis = await create_pool(RedisSettings(host="latinum-db", port=6379))
    if VAULT_DB_URL:
        init_vault_schema()

@app.get("/stats")
async def get_network_stats():
    try:
        conn = psycopg2.connect(VAULT_DB_URL)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM shard_history;")
        total_shards = cur.fetchone()[0]
        cur.execute("SELECT SUM(CASE WHEN shard_id LIKE 'REAL-DATA%' THEN 100 ELSE 25 END) FROM shard_history;")
        total_cu = cur.fetchone()[0] or 0
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
    try:
        conn = psycopg2.connect(VAULT_DB_URL)
        cur = conn.cursor()
        cur.execute("SELECT shard_id, node_sig, finalized_at FROM shard_history ORDER BY finalized_at DESC LIMIT 50;")
        rows = cur.fetchall()
        total_cu = 0
        for row in rows:
            total_cu += 100 if ("REAL-DATA" in row[0] or "QUANTUM" in row[0]) else 25
        cur.close()
        conn.close()
        html = f"<html><body style='font-family:monospace; background:#0d1117; color:#58a6ff; padding:40px;'>"
        html += f"<div style='background:#161b22; padding:20px; border-radius:8px; border:1px solid #30363d; margin-bottom:20px;'>"
        html += f"<h2>💰 Total Yield: {total_cu} CU</h2><p>Network: Institutional v4.0</p></div>"
        html += "<h1>💎 SHARD ARCHIVE</h1><table border='1' style='width:100%; border-collapse:collapse;'>"
        for row in rows:
            html += f"<tr><td>{row[0]}</td><td>{row[1]}</td><td>{row[2]}</td></tr>"
        html += "</table></body></html>"
        return html
    except Exception as e:
        return f"<h1>Error</h1><p>{str(e)}</p>"

@app.get("/tasks/claim")
async def claim_task(prospector_id: str = Query(None)):
    task_raw = await app.state.redis.lpop("latinum_task_queue")
    if not task_raw: return {"status": "idle"}
    task = json.loads(task_raw)
    return {"status": "success", "work_unit": {"work_unit_id": task.get("task_id"), "assigned_prospector": prospector_id or "UNKNOWN"}}

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
