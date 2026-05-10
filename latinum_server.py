@app.get("/results", response_class=HTMLResponse)
async def get_results():
    """Live Web Dashboard with CU Reward Calculation"""
    try:
        conn = psycopg2.connect(VAULT_DB_URL)
        cur = conn.cursor()
        
        # Pull the last 50 shards
        cur.execute("SELECT shard_id, node_sig, finalized_at FROM shard_history ORDER BY finalized_at DESC LIMIT 50;")
        rows = cur.fetchall()
        
        # Simple Logic: REAL-DATA shards = 100 CU, others = 25 CU
        total_cu = 0
        for row in rows:
            if "REAL-DATA" in row[0] or "QUANTUM" in row[0]:
                total_cu += 100
            else:
                total_cu += 25
        
        cur.close()
        conn.close()

        # Dashboard UI
        html = "<html><head><title>Latinum Vault</title></head><body style='font-family:monospace; background:#0d1117; color:#58a6ff; padding:40px;'>"
        html += f"<div style='background:#161b22; padding:20px; border-radius:8px; border:1px solid #30363d; margin-bottom:20px;'>"
        html += f"<h2 style='margin:0; color:#aff5b4;'>💰 Total Yield: {total_cu} Complexity Units</h2>"
        html += f"<p style='color:#8b949e; margin:5px 0 0 0;'>Node: PROSPECTOR-SAMBO-01 | Network: Institutional v4.0</p></div>"
        
        html += "<h1>💎 SHARD ARCHIVE</h1><hr>"
        html += "<table border='1' style='width:100%; border-color:#30363d; border-collapse:collapse; text-align:left;'>"
        html += "<tr style='background:#161b22;'><th>SHARD_ID</th><th>NODE_SIG</th><th>TIMESTAMP</th></tr>"
        for row in rows:
            color = "#aff5b4" if "REAL-DATA" in row[0] else "#58a6ff"
            html += f"<tr style='color:{color};'><td>{row[0]}</td><td>{row[1]}</td><td>{row[2]}</td></tr>"
        html += "</table></body></html>"
        return html
    except Exception as e:
        return f"<h1>❌ Vault Error</h1><p>{str(e)}</p>"
