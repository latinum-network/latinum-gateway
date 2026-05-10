import os
import asyncio
import json
import time
import psycopg2
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from arq import create_pool
from arq.connections import RedisSettings

app = FastAPI(title="Latinum AI Refinery & Vault")

VAULT_DB_URL = os.getenv("VAULT_DB_URL") 

@app.on_event("startup")
async def startup():
    app.state.redis = await create_pool(RedisSettings(host="latinum-db", port=6379))
    if VAULT_DB_URL:
        init_vault_schema()

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
        html = "<html><head><title>Latinum Vault</title></head><body style='font-family:monospace; background:#0d1117; color:#58a6ff; padding:40px;'>"
        html += "<h1>💎 LATINUM REFINERY YIELD</h1><hr>"
        html += "<table border='1' style='width:100%; border-color:#30363d; border-collapse:collapse; text-align:left;'>"
        html += "<tr style='background:#161b22;'><th>SHARD_ID</th><th>NODE_SIG</th><th>TIMESTAMP</th></tr>"
        for row in rows:
            html += f"<tr><td>{row[0]}</td><td>{row[1]}</td><td>{row[2]}</td></tr>"
        html += "</table></body></html>"
        return html
    except Exception as e:
        return f"<h1>❌ Vault Connection Error</h1><p>{str(e)}</p>"

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
