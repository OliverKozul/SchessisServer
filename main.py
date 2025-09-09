import asyncio
import json
import uuid
import asyncpg
import os
import logging

from datetime import datetime
from typing import Dict, Optional
from pydantic import BaseModel
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Query


logger = logging.getLogger("uvicorn")
app = FastAPI()

K = 32  # Elo K-factor
DATABASE_URL = os.getenv("DATABASE_URL")  # Render injects this

# In-memory matchmaking state (protected by locks)
matchmaking_queue: Dict[str, dict] = {}   # steam_id -> entry dict
matchmaking_sorted: list = []  # sorted list of tuples (elo, steam_id)
queue_lock = asyncio.Lock()

matches: Dict[str, dict] = {}             # match_id -> {a, b, host, created_at}
match_lock = asyncio.Lock()

player_match_map: Dict[str, str] = {}     # steam_id -> match_id
ws_connections: Dict[str, WebSocket] = {} # steam_id -> websocket
ws_lock = asyncio.Lock()

db_pool: Optional[asyncpg.pool.Pool] = None


############################
# Async DB helpers (asyncpg)
############################

async def init_db_async():
    async with db_pool.acquire() as conn:
        # Use a single transaction for all DDL
        async with conn.transaction():
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS players (
                    steam_id TEXT PRIMARY KEY,
                    steam_name TEXT,
                    elo INTEGER DEFAULT 1000,
                    wins INTEGER DEFAULT 0,
                    losses INTEGER DEFAULT 0
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS match_history (
                    id SERIAL PRIMARY KEY,
                    player_id TEXT,
                    opponent_id TEXT,
                    result BOOLEAN,
                    timestamp TIMESTAMP DEFAULT NOW(),
                    FOREIGN KEY (player_id) REFERENCES players (steam_id) ON DELETE CASCADE
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS matches (
                    id SERIAL PRIMARY KEY,
                    match_id TEXT UNIQUE,
                    player1_id TEXT,
                    player2_id TEXT,
                    host_id TEXT,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)

async def get_match_history(steam_id: str):
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT opponent_id, result, timestamp
            FROM match_history
            WHERE player_id = $1
            ORDER BY timestamp DESC
            """, steam_id)
    return [{"opponent_id": r[0], "won": r[1], "timestamp": r[2]} for r in rows]

async def get_player(steam_id: str, steam_name: str = "Anon"):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT steam_id, steam_name, elo, wins, losses FROM players WHERE steam_id = $1", steam_id)
        if row:
            current_name = row[1]
            if steam_name != "Anon" and (current_name is None or current_name != steam_name):
                await conn.execute("UPDATE players SET steam_name = $1 WHERE steam_id = $2", steam_name, steam_id)
                current_name = steam_name
            player = {"steam_id": row[0], "steam_name": current_name, "elo": row[2], "wins": row[3], "losses": row[4]}
        else:
            await conn.execute(
                "INSERT INTO players (steam_id, steam_name, elo, wins, losses) VALUES ($1, $2, $3, $4, $5)",
                steam_id, steam_name, 1000, 0, 0
            )
            player = {"steam_id": steam_id, "steam_name": steam_name, "elo": 1000, "wins": 0, "losses": 0}
    return player

async def update_player(player: dict):
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE players
            SET steam_name = $1, elo = $2, wins = $3, losses = $4
            WHERE steam_id = $5
            """,
            player["steam_name"], player["elo"], player["wins"], player["losses"], player["steam_id"]
        )

async def add_match_history(player_id: str, opponent_id: str, won: bool):
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO match_history (player_id, opponent_id, result, timestamp)
            VALUES ($1, $2, $3, $4)
            """,
            player_id, opponent_id, won, datetime.utcnow()
        )

async def insert_match_record(match_id: str, a: str, b: str, host: str):
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO matches (match_id, player1_id, player2_id, host_id, created_at)
            VALUES ($1, $2, $3, $4, $5)
            """,
            match_id, a, b, host, datetime.utcnow()
        )


# Run DB init at startup using thread
@app.on_event("startup")
async def startup_event():
    global db_pool
    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
    await init_db_async()
    logger.info("Server started and DB initialized (asyncpg pool ready).")

# --------------------------
# Utility (ELO)
# --------------------------
def calculate_elo(player_rating, opponent_rating, won: bool):
    expected = 1 / (1 + 10 ** ((opponent_rating - player_rating) / 400))
    score = 1.0 if won else 0.0
    return max(round(player_rating + K * (score - expected)), 0)


#############################
# Event-driven matchmaking  #
#############################

def _binary_insert_index(elo: int):
    lo, hi = 0, len(matchmaking_sorted)
    while lo < hi:
        mid = (lo + hi) // 2
        if matchmaking_sorted[mid][0] < elo:
            lo = mid + 1
        else:
            hi = mid
    return lo

def _find_opponent_locked(idx: int) -> Optional[str]:
    """Return opponent steam_id if either immediate neighbor qualifies; list is Elo-sorted so only neighbors matter."""
    if idx < 0 or idx >= len(matchmaking_sorted):
        return None
    elo, steam_id = matchmaking_sorted[idx]
    seeker = matchmaking_queue.get(steam_id)
    if not seeker:
        return None
    max_diff_seeker = seeker.get("max_diff", 100)
    best = None  # (diff, steam_id)
    for n_idx in (idx - 1, idx + 1):
        if 0 <= n_idx < len(matchmaking_sorted):
            n_elo, n_id = matchmaking_sorted[n_idx]
            if n_id == steam_id:
                continue
            cand = matchmaking_queue.get(n_id)
            if not cand:
                continue
            allowed = max(max_diff_seeker, cand.get("max_diff", 100))
            diff = abs(n_elo - elo)
            if diff <= allowed:
                if best is None or diff < best[0]:
                    best = (diff, n_id)
    return best[1] if best else None

async def attempt_instant_match(steam_id: str, idx: Optional[int] = None):
    """Attempt to form a match for steam_id using optional precomputed idx."""
    async with queue_lock:
        global matchmaking_sorted
        if idx is None:
            idx = next((i for i, (_, sid) in enumerate(matchmaking_sorted) if sid == steam_id), None)
        if idx is None:
            return
        opponent_id = _find_opponent_locked(idx)
        if not opponent_id:
            return
        seeker_entry = matchmaking_queue.get(steam_id)
        opponent_entry = matchmaking_queue.get(opponent_id)
        if not seeker_entry or not opponent_entry:
            return
        a_id, b_id = steam_id, opponent_id
        host = a_id if a_id < b_id else b_id
        match_id = uuid.uuid4().hex
        remove_set = {a_id, b_id}
        matchmaking_sorted = [t for t in matchmaking_sorted if t[1] not in remove_set]
        matchmaking_queue.pop(a_id, None)
        matchmaking_queue.pop(b_id, None)
        async with match_lock:
            matches[match_id] = {"a": a_id, "b": b_id, "host": host, "created_at": datetime.utcnow()}
            player_match_map[a_id] = match_id
            player_match_map[b_id] = match_id
    # persist & notify outside queue lock
    await insert_match_record(match_id, a_id, b_id, host)
    async with ws_lock:
        ws_a = ws_connections.get(a_id)
        ws_b = ws_connections.get(b_id)
    payload_a = {"type": "match_found", "match_id": match_id, "opponent_id": b_id, "opponent_name": opponent_entry.get("steam_name"), "opponent_elo": opponent_entry.get("elo"), "host": host}
    payload_b = {"type": "match_found", "match_id": match_id, "opponent_id": a_id, "opponent_name": seeker_entry.get("steam_name"), "opponent_elo": seeker_entry.get("elo"), "host": host}
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

async def enqueue_player(steam_id: str, steam_name: str, elo: int, max_diff: int):
    idx = None
    async with queue_lock:
        if steam_id in matchmaking_queue:
            return
        entry = {"steam_id": steam_id, "steam_name": steam_name, "elo": elo, "joined_at": datetime.utcnow(), "max_diff": max_diff}
        matchmaking_queue[steam_id] = entry
        idx = _binary_insert_index(elo)
        matchmaking_sorted.insert(idx, (elo, steam_id))
    await attempt_instant_match(steam_id, idx)

async def remove_from_queue(steam_id: str):
    async with queue_lock:
        if steam_id in matchmaking_queue:
            matchmaking_queue.pop(steam_id, None)
            # rebuild sorted list without steam_id
            global matchmaking_sorted
            matchmaking_sorted = [t for t in matchmaking_sorted if t[1] != steam_id]


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
    player = await get_player(steam_id, steam_name or "Anon")

    # add to matchmaking queue (event-driven)
    await enqueue_player(steam_id, player.get("steam_name"), player.get("elo", 1000), int(max_diff or 100))
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
                await remove_from_queue(steam_id)
                await websocket.send_text(json.dumps({"type": "left_queue"}))
                continue

            if mtype == "get_elo":
                # fetch latest elo
                p = await get_player(steam_id)
                await websocket.send_text(json.dumps({"type": "elo", "elo": p["elo"]}))
                continue

            if mtype == "relay":
                # Relay message to opponent within same match
                match_id = msg.get("match_id")
                action = msg.get("action")
                payload = msg.get("payload", {})
                logger.info(f"Relay from {steam_id} in match {match_id}: action={action}")
                if not match_id or action not in ("start_game", "left_click", "right_click", "fetch_player_info", "color_chosen", "draft_end_started", "random_draft_requested", "yes_rematch", "no_rematch", "disable_clicking"):
                    logger.warning(f"Invalid relay from {steam_id} in match {match_id}: action={action}")
                    continue
                # verify membership
                async with match_lock:
                    mid = player_match_map.get(steam_id)
                    if mid != match_id:
                        # not in match or mismatch
                        logger.warning(f"Relay from {steam_id} for invalid match {match_id}. Match does not match map.")
                        continue
                    match = matches.get(match_id)
                    if not match:
                        logger.warning(f"Relay from {steam_id} for invalid match {match_id}. Match does not exist.")
                        continue
                    # determine opponent id
                    opponent_id = match["b"] if match["a"] == steam_id else match["a"]
                async with ws_lock:
                    target_ws = ws_connections.get(opponent_id)
                if target_ws:
                    fwd = {"type": "relay", "from": steam_id, "action": action, "payload": payload, "match_id": match_id}
                    logger.info(f"Forwarding relay from {steam_id} to {opponent_id}: {fwd}")
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
                # async inline implementation
                p = await get_player(steam_id)
                opp = await get_player(opponent_id)
                opponent_rating = opp["elo"]
                new_elo = calculate_elo(p["elo"], opponent_rating, won)
                p["elo"] = new_elo
                if won:
                    p["wins"] += 1
                else:
                    p["losses"] += 1
                await update_player(p)
                await add_match_history(p["steam_id"], opponent_id, won)
                # refresh opponent after potential concurrent change not needed here; reuse opp
                new_opp_elo = calculate_elo(opp["elo"], p["elo"], not won)
                opp["elo"] = new_opp_elo
                if not won:
                    opp["wins"] += 1
                else:
                    opp["losses"] += 1
                await update_player(opp)
                await add_match_history(opp["steam_id"], p["steam_id"], not won)
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
        await remove_from_queue(steam_id)
        async with match_lock:
            if steam_id in player_match_map:
                m_id = player_match_map.pop(steam_id)
                match = matches.pop(m_id, None)
                if match:
                    other = match["b"] if match["a"] == steam_id else match["a"]
                    player_match_map.pop(other, None)
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

class MatchResult(BaseModel):
    steam_id: str
    opponent_id: str
    won: bool


@app.post("/report_result")
async def report_result(result: MatchResult):
    p = await get_player(result.steam_id)
    opponent = await get_player(result.opponent_id)
    opponent_rating = opponent["elo"]
    new_player_elo = calculate_elo(p["elo"], opponent_rating, result.won)
    p["elo"] = new_player_elo
    if result.won:
        p["wins"] += 1
    else:
        p["losses"] += 1
    await update_player(p)
    await add_match_history(p["steam_id"], result.opponent_id, result.won)
    new_opponent_elo = calculate_elo(opponent["elo"], p["elo"], not result.won)
    opponent["elo"] = new_opponent_elo
    if not result.won:
        opponent["wins"] += 1
    else:
        opponent["losses"] += 1
    await update_player(opponent)
    await add_match_history(opponent["steam_id"], p["steam_id"], not result.won)
    return {"player": p, "opponent": opponent}

@app.get("/get_player/{steam_id}")
async def get_player_info(steam_id: str):
    p = await get_player(steam_id)
    if not p:
        raise HTTPException(status_code=404, detail="Player not found")
    raw_history = await get_match_history(steam_id)
    history = [{"opponent_id": r["opponent_id"], "won": r["won"], "timestamp": r["timestamp"].isoformat()} for r in raw_history]
    return {"player": p, "history": history}

@app.get("/leaderboard")
async def get_leaderboard():
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT steam_id, steam_name, elo, wins, losses FROM players ORDER BY elo DESC LIMIT 10")
    leaderboard = [{"steam_id": r[0], "steam_name": r[1], "elo": r[2], "wins": r[3], "losses": r[4]} for r in rows]
    return {"leaderboard": leaderboard}
