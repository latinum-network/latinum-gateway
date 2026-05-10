import os
import hmac
import hashlib
from fastapi import FastAPI, HTTPException, Request

app = FastAPI(title="Latinum AI Refinery & Vault")

# HARDENING: Secret key for node authentication
# In production, move this to a Coolify Environment Variable
NODE_SECRET = os.getenv("NODE_SECRET", "LATINUM_REFINERY_SECRET_2026")

@app.post("/tasks/submit")
async def submit_task(request: Request):
    data = await request.json()
    received_sig = request.headers.get("X-Latinum-Signature")
    
    # Verify the "Quantum Seal" (HMAC-SHA256 for now, ML-KEM handshake ready)
    expected_sig = hmac.new(
        NODE_SECRET.encode(), 
        data['task_id'].encode(), 
        hashlib.sha256
    ).hexdigest()

    if not hmac.compare_digest(received_sig, expected_sig):
        raise HTTPException(status_code=403, detail="Invalid Node Signature: Proof Rejected")

    # If signature is valid, proceed to vault the data
    conn = psycopg2.connect(VAULT_DB_URL)
    cur = conn.cursor()
    cur.execute("INSERT INTO shard_history (shard_id, node_sig, loss_value) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING", 
                (data['task_id'], data['node_id'], 0.0))
    conn.commit()
    cur.close()
    conn.close()
    return {"status": "success", "verified": True}
