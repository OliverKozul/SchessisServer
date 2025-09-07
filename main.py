# server.py
import asyncio
import json
import uuid
from datetime import datetime
from typing import Dict, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Query
import psycopg2
import os
import logging

logger = logging.getLogger("uvicorn")
app = FastAPI()

K = 32  # Elo K-factor
DATABASE_URL = os.getenv("DATABASE_URL")  # Render injects this

# In-memory matchmaking state (protected by locks)
matchmaking_queue: Dict[str, dict] = {}   # steam_id -> {steam_id, steam_name, elo, joined_at, max_diff, ws}
queue_lock = asyncio.Lock()

matches: Dict[str, dict] = {}             # match_id -> {a, b, host, created_at}
match_lock = asyncio.Lock()

player_match_map: Dict[str, str] = {}     # steam_id -> match_id
ws_connections: Dict[str, WebSocket] = {} # steam_id -> websocket
ws_lock = asyncio.Lock()

MATCHMAKER_INTERVAL = 1.0  # seconds


# --------------------------
# Database helpers (sync) - run via asyncio.to_thread
# --------------------------
def get_conn():
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def init_db_sync():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS players (
            steam_id TEXT PRIMARY KEY,
            steam_name TEXT,
            elo INTEGER DEFAULT 1000,
            wins INTEGER DEFAULT 0,
            losses INTEGER DEFAULT 0
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS match_history (
            id SERIAL PRIMARY KEY,
            player_id TEXT,
            opponent_id TEXT,
            result BOOLEAN,
            timestamp TIMESTAMP DEFAULT NOW(),
            FOREIGN KEY (player_id) REFERENCES players (steam_id) ON DELETE CASCADE
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS matches (
            id SERIAL PRIMARY KEY,
            match_id TEXT UNIQUE,
            player1_id TEXT,
            player2_id TEXT,
            host_id TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    conn.commit()
    cur.close()
    conn.close()

def get_match_history(steam_id: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT opponent_id, result, timestamp 
        FROM match_history
        WHERE player_id = %s
        ORDER BY timestamp DESC
    """, (steam_id,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [{"opponent_id": r[0], "won": r[1], "timestamp": r[2]} for r in rows]

def get_player_sync(steam_id: str, steam_name: str = "Anon"):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT steam_id, steam_name, elo, wins, losses FROM players WHERE steam_id = %s", (steam_id,))
    row = cur.fetchone()
    if row:
        # update name if provided and different
        if steam_name != "Anon" and (row[1] is None or row[1] != steam_name):
            cur.execute("UPDATE players SET steam_name = %s WHERE steam_id = %s", (steam_name, steam_id))
            conn.commit()
        player = {"steam_id": row[0], "steam_name": steam_name if steam_name != "Anon" else row[1], "elo": row[2], "wins": row[3], "losses": row[4]}
    else:
        player = {"steam_id": steam_id, "steam_name": steam_name, "elo": 1000, "wins": 0, "losses": 0}
        cur.execute("INSERT INTO players (steam_id, steam_name, elo, wins, losses) VALUES (%s, %s, %s, %s, %s)",
                    (steam_id, steam_name, 1000, 0, 0))
        conn.commit()
    cur.close()
    conn.close()
    return player

def update_player_sync(player: dict):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE players
        SET steam_name = %s, elo = %s, wins = %s, losses = %s
        WHERE steam_id = %s
    """, (player["steam_name"], player["elo"], player["wins"], player["losses"], player["steam_id"]))
    conn.commit()
    cur.close()
    conn.close()


def add_match_history_sync(player_id, opponent_id, won: bool):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO match_history (player_id, opponent_id, result, timestamp)
        VALUES (%s, %s, %s, %s)
    """, (player_id, opponent_id, won, datetime.utcnow()))
    conn.commit()
    cur.close()
    conn.close()

def insert_match_record_sync(match_id: str, a: str, b: str, host: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO matches (match_id, player1_id, player2_id, host_id, created_at)
        VALUES (%s, %s, %s, %s, %s)
    """, (match_id, a, b, host, datetime.utcnow()))
    conn.commit()
    cur.close()
    conn.close()


# Run DB init at startup using thread
@app.on_event("startup")
async def startup_event():
    await asyncio.to_thread(init_db_sync)
    # start matchmaking loop
    asyncio.create_task(matchmaker_loop())
    logger.info("Server started and DB initialized.")

# --------------------------
# Utility (ELO)
# --------------------------
def calculate_elo(player_rating, opponent_rating, won: bool):
    expected = 1 / (1 + 10 ** ((opponent_rating - player_rating) / 400))
    score = 1.0 if won else 0.0
    return max(round(player_rating + K * (score - expected)), 0)


# --------------------------
# Matchmaker background task
# --------------------------
async def matchmaker_loop():
    while True:
        await asyncio.sleep(MATCHMAKER_INTERVAL)
        await try_matchmaking_round()


async def try_matchmaking_round():
    # simple O(n^2) pair search; fine for small queues
    async with queue_lock:
        steam_ids = list(matchmaking_queue.keys())
    if len(steam_ids) < 2:
        return

    paired = set()
    for i in range(len(steam_ids)):
        a_id = steam_ids[i]
        async with queue_lock:
            if a_id not in matchmaking_queue:  # might have been removed
                continue
            a_entry = matchmaking_queue[a_id]
        if a_id in paired:
            continue

        # try to find a suitable opponent for a_id
        for j in range(i + 1, len(steam_ids)):
            b_id = steam_ids[j]
            async with queue_lock:
                if b_id not in matchmaking_queue:
                    continue
                b_entry = matchmaking_queue[b_id]

            if b_id in paired:
                continue

            # compute allowed diff (we respect client's requested max_diff if present)
            max_diff_a = a_entry.get("max_diff", 100)
            max_diff_b = b_entry.get("max_diff", 100)
            allowed = max(max_diff_a, max_diff_b)

            elo_diff = abs(a_entry["elo"] - b_entry["elo"])
            if elo_diff <= allowed:
                # create match
                match_id = uuid.uuid4().hex
                host = a_id if a_id < b_id else b_id  # lower steam id hosts
                async with match_lock:
                    matches[match_id] = {"a": a_id, "b": b_id, "host": host, "created_at": datetime.utcnow()}
                    player_match_map[a_id] = match_id
                    player_match_map[b_id] = match_id

                # persist match record (sync DB in thread)
                await asyncio.to_thread(insert_match_record_sync, match_id, a_id, b_id, host)

                # remove from queue
                async with queue_lock:
                    matchmaking_queue.pop(a_id, None)
                    matchmaking_queue.pop(b_id, None)

                # notify both players (if connected)
                async with ws_lock:
                    ws_a = ws_connections.get(a_id)
                    ws_b = ws_connections.get(b_id)

                payload_a = {
                    "type": "match_found",
                    "match_id": match_id,
                    "opponent_id": b_id,
                    "opponent_elo": b_entry["elo"],
                    "host": host
                }
                payload_b = {
                    "type": "match_found",
                    "match_id": match_id,
                    "opponent_id": a_id,
                    "opponent_elo": a_entry["elo"],
                    "host": host
                }

                if ws_a:
                    try:
                        await ws_a.send_text(json.dumps(payload_a))
                    except Exception:
                        logger.exception("Failed to send match_found to %s", a_id)
                if ws_b:
                    try:
                        await ws_b.send_text(json.dumps(payload_b))
                    except Exception:
                        logger.exception("Failed to send match_found to %s", b_id)

                paired.add(a_id)
                paired.add(b_id)
                break


# --------------------------
# WebSocket endpoint: join queue & relay
# Client connects to /ws/{steam_id}?max_diff=100&steam_name=Foo
# After connecting we auto-add to matchmaking queue.
# Relay message format: {"type":"relay","match_id":"...","action":"left"|"right","payload":{...}}
# --------------------------
@app.websocket("/ws/{steam_id}")
async def websocket_endpoint(websocket: WebSocket, steam_id: str, max_diff: Optional[int] = Query(100), steam_name: Optional[str] = Query(None)):
    await websocket.accept()
    logger.info("WS connected: %s", steam_id)

    # register ws
    async with ws_lock:
        ws_connections[steam_id] = websocket

    # ensure player exists and fetch elo (run DB sync in thread)
    player = await asyncio.to_thread(get_player_sync, steam_id, steam_name or "Anon")

    # add to matchmaking queue
    async with queue_lock:
        if steam_id not in matchmaking_queue:
            matchmaking_queue[steam_id] = {
                "steam_id": steam_id,
                "steam_name": player.get("steam_name"),
                "elo": player.get("elo", 1000),
                "joined_at": datetime.utcnow(),
                "max_diff": int(max_diff or 100)
            }
    # ack
    try:
        await websocket.send_text(json.dumps({"type": "queued", "elo": player.get("elo", 1000)}))

        # main receive loop
        while True:
            raw = await websocket.receive_text()
            try:
                msg = json.loads(raw)
            except Exception:
                continue

            mtype = msg.get("type")

            if mtype == "ping":
                await websocket.send_text(json.dumps({"type": "pong"}))
                continue

            if mtype == "leave_queue":
                async with queue_lock:
                    if steam_id in matchmaking_queue:
                        matchmaking_queue.pop(steam_id, None)
                await websocket.send_text(json.dumps({"type": "left_queue"}))
                continue

            if mtype == "get_elo":
                # fetch latest elo
                p = await asyncio.to_thread(get_player_sync, steam_id)
                await websocket.send_text(json.dumps({"type": "elo", "elo": p["elo"]}))
                continue

            if mtype == "relay":
                # Relay message to opponent within same match
                match_id = msg.get("match_id")
                action = msg.get("action")
                payload = msg.get("payload", {})
                if not match_id or action not in ("left", "right"):
                    continue
                # verify membership
                async with match_lock:
                    mid = player_match_map.get(steam_id)
                    if mid != match_id:
                        # not in match or mismatch
                        continue
                    match = matches.get(match_id)
                    if not match:
                        continue
                    # determine opponent id
                    opponent_id = match["b"] if match["a"] == steam_id else match["a"]
                async with ws_lock:
                    target_ws = ws_connections.get(opponent_id)
                if target_ws:
                    fwd = {"type": "relay", "from": steam_id, "action": action, "payload": payload, "match_id": match_id}
                    try:
                        await target_ws.send_text(json.dumps(fwd))
                    except Exception:
                        logger.exception("Failed to forward relay to %s", opponent_id)
                continue

            if mtype == "report_result":
                # optional: client can notify server of result; reuse existing report_result endpoint logic via thread
                # message should include opponent_id and won bool
                opponent_id = msg.get("opponent_id")
                won = msg.get("won", False)
                # run report_result logic in a thread
                def _report():
                    # call your existing report_result-like logic directly
                    p = get_player_sync(steam_id)
                    opp = get_player_sync(opponent_id)
                    opponent_rating = opp["elo"]
                    new_elo = calculate_elo(p["elo"], opponent_rating, won)
                    p["elo"] = new_elo
                    if won:
                        p["wins"] += 1
                    else:
                        p["losses"] += 1
                    update_player_sync(p)
                    add_match_history_sync(p["steam_id"], opponent_id, won)
                    opp = get_player_sync(opponent_id)
                    new_opp_elo = calculate_elo(opp["elo"], p["elo"], not won)
                    opp["elo"] = new_opp_elo
                    if not won:
                        opp["wins"] += 1
                    else:
                        opp["losses"] += 1
                    update_player_sync(opp)
                    add_match_history_sync(opp["steam_id"], p["steam_id"], not won)
                await asyncio.to_thread(_report)
                await websocket.send_text(json.dumps({"type": "reported"}))
                continue

            # unknown message types ignored
    except WebSocketDisconnect:
        logger.info("WS disconnected: %s", steam_id)
    except Exception:
        logger.exception("WS error for %s", steam_id)
    finally:
        # cleanup on disconnect
        async with ws_lock:
            ws_connections.pop(steam_id, None)
        # remove from queue if present
        async with queue_lock:
            matchmaking_queue.pop(steam_id, None)
        # if in a match, cancel and inform opponent
        async with match_lock:
            if steam_id in player_match_map:
                m_id = player_match_map.pop(steam_id)
                match = matches.pop(m_id, None)
                if match:
                    other = match["b"] if match["a"] == steam_id else match["a"]
                    player_match_map.pop(other, None)
                    # notify opponent if connected
                    async with ws_lock:
                        other_ws = ws_connections.get(other)
                    if other_ws:
                        try:
                            await other_ws.send_text(json.dumps({"type": "match_cancelled", "match_id": m_id, "reason": "disconnect", "initiator": steam_id}))
                        except Exception:
                            pass


# --------------------------
# Existing REST endpoints (report_result, get_player, leaderboard)
# Keep these as-is, they call the same sync DB helpers via threads when needed.
# --------------------------
from pydantic import BaseModel  # already imported earlier above

class MatchResult(BaseModel):
    steam_id: str
    opponent_id: str
    won: bool


@app.post("/report_result")
async def report_result(result: MatchResult):
    # run the same synchronous logic in a thread
    def _report():
        p = get_player_sync(result.steam_id)
        opponent = get_player_sync(result.opponent_id)
        opponent_rating = opponent["elo"]
        new_player_elo = calculate_elo(p["elo"], opponent_rating, result.won)
        p["elo"] = new_player_elo
        if result.won:
            p["wins"] += 1
        else:
            p["losses"] += 1
        update_player_sync(p)
        add_match_history_sync(p["steam_id"], result.opponent_id, result.won)
        if opponent is not None:
            new_opponent_elo = calculate_elo(opponent["elo"], p["elo"], not result.won)
            opponent["elo"] = new_opponent_elo
            if not result.won:
                opponent["wins"] += 1
            else:
                opponent["losses"] += 1
            update_player_sync(opponent)
            add_match_history_sync(opponent["steam_id"], p["steam_id"], not result.won)
        return {"player": p, "opponent": opponent if opponent else "AI"}
    return await asyncio.to_thread(_report)

@app.get("/get_player/{steam_id}")
async def get_player_info(steam_id: str):
    p = await asyncio.to_thread(get_player_sync, steam_id)
    if not p:
        raise HTTPException(status_code=404, detail="Player not found")
    # fetch history
    history = await asyncio.to_thread(lambda: [
        {"opponent_id": r["opponent_id"], "won": r["won"], "timestamp": r["timestamp"].isoformat()}
        for r in get_match_history(steam_id)
    ])
    return {"player": p, "history": history}

@app.get("/leaderboard")
async def get_leaderboard():
    def _lb():
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT steam_id, steam_name, elo, wins, losses FROM players ORDER BY elo DESC LIMIT 10")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    rows = await asyncio.to_thread(_lb)
    leaderboard = [{"steam_id": r[0], "steam_name": r[1], "elo": r[2], "wins": r[3], "losses": r[4]} for r in rows]
    return {"leaderboard": leaderboard}
