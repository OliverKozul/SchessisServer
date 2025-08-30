import asyncio
from fastapi import FastAPI, HTTPException
from fastapi.params import Query
from pydantic import BaseModel
import psycopg2
import os
import logging
from datetime import datetime
from typing import Dict

logger = logging.getLogger("uvicorn")

app = FastAPI()

K = 32  # sensitivity constant for Elo

# --------------------------
# Database setup
# --------------------------
DATABASE_URL = os.getenv("DATABASE_URL")  # Render injects this
matchmaking_queue: Dict[str, Dict] = {}  # steam_id -> player_data

def get_conn():
    return psycopg2.connect(DATABASE_URL, sslmode="require")

def init_db():
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
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name='players' AND column_name='steam_name'
            ) THEN
                ALTER TABLE players ADD COLUMN steam_name TEXT;
            END IF;
        END
        $$;
    """)

    conn.commit()
    cur.close()
    conn.close()

def print_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM players")
    rows = cur.fetchall()
    logger.info("Current players in DB:")
    for row in rows:
        logger.info(f"Player: {row[0]}, Elo: {row[1]}, Wins: {row[2]}, Losses: {row[3]}")
    cur.close()
    conn.close()

init_db()
print_db()

# --------------------------
# Models
# --------------------------
class MatchResult(BaseModel):
    steam_id: str
    opponent_id: str
    won: bool
    opponent_is_ai: bool = False
    ai_rating: int = 1000

class QueueRequest(BaseModel):
    steam_id: str
    steam_lobby_id: str

# --------------------------
# Helpers
# --------------------------
def get_player(steam_id: str, steam_name: str = "Anon"):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT steam_id, steam_name, elo, wins, losses FROM players WHERE steam_id = %s", (steam_id,))
    row = cur.fetchone()
    if row and steam_name != "Anon" and row[1] != steam_name:
        update_player({"steam_id": steam_id, "steam_name": steam_name, "elo": row[2], "wins": row[3], "losses": row[4]})
        player = {"steam_id": row[0], "steam_name": steam_name, "elo": row[2], "wins": row[3], "losses": row[4]}
    else:
        player = {"steam_id": steam_id, "steam_name": steam_name, "elo": 1000, "wins": 0, "losses": 0}
        cur.execute("INSERT INTO players (steam_id, steam_name, elo, wins, losses) VALUES (%s, %s, %s, %s, %s)", 
                    (steam_id, steam_name, 1000, 0, 0))
        conn.commit()
    cur.close()
    conn.close()
    return player


def update_player(player):
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

def add_match_history(player_id, opponent_id, won):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO match_history (player_id, opponent_id, result, timestamp)
        VALUES (%s, %s, %s, %s)
    """, (player_id, opponent_id, won, datetime.utcnow()))
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

def calculate_elo(player_rating, opponent_rating, won):
    expected = 1 / (1 + 10 ** ((opponent_rating - player_rating) / 400))
    score = 1 if won else 0
    return max(round(player_rating + K * (score - expected)), 0)


# --------------------------
# API endpoints
# --------------------------
@app.post("/report_result")
def report_result(result: MatchResult):
    logging.info(f"Received match result: {result}")
    # Get player
    player = get_player(result.steam_id)

    # Get opponent (AI or human)
    if result.opponent_is_ai:
        opponent_rating = result.ai_rating
        opponent = None
    else:
        opponent = get_player(result.opponent_id)
        opponent_rating = opponent["elo"]

    # Update Elo
    new_player_elo = calculate_elo(player["elo"], opponent_rating, result.won)
    player["elo"] = new_player_elo
    if result.won:
        player["wins"] += 1
    else:
        player["losses"] += 1
    update_player(player)

    # Save match history
    add_match_history(player["steam_id"], result.opponent_id, result.won)

    # If opponent is human, update their rating and history too
    if opponent is not None:
        new_opponent_elo = calculate_elo(opponent["elo"], player["elo"], not result.won)
        opponent["elo"] = new_opponent_elo
        if not result.won:
            opponent["wins"] += 1
        else:
            opponent["losses"] += 1
        update_player(opponent)

        add_match_history(opponent["steam_id"], player["steam_id"], not result.won)

    return {"player": player, "opponent": opponent if opponent else "AI"}


@app.get("/get_player/{steam_id}")
def get_player_info(steam_id: str, steam_name: str = Query(None)):
    player = get_player(steam_id, steam_name)
    if not player:
        raise HTTPException(status_code=404, detail="Player not found")
    history = get_match_history(steam_id)
    return {"player": player, "history": history}


# --------------------------
# Matchmaking API endpoints
# --------------------------
@app.post("/queue")
async def join_queue(request: QueueRequest):
    """Add a player to the matchmaking queue"""
    # Get player data
    player = get_player(request.steam_id)
    
    # Add to queue if not already there
    if request.steam_id not in matchmaking_queue:
        matchmaking_queue[request.steam_id] = {
            "steam_id": request.steam_id,
            "elo": player["elo"],
            "steam_lobby_id": request.steam_lobby_id,
            "joined_at": datetime.utcnow()
        }
        logger.info(f"Player {request.steam_id} joined queue")
    
    return {"status": "in_queue", "position": len(matchmaking_queue)}

@app.post("/leave_queue")
async def leave_queue(request: QueueRequest):
    """Remove a player from the matchmaking queue"""
    if request.steam_id in matchmaking_queue:
        del matchmaking_queue[request.steam_id]
        logger.info(f"Player {request.steam_id} left queue")
    
    return {"status": "left_queue"}

@app.get("/find_opponent")
async def find_opponent(request: QueueRequest):
    """Find a suitable opponent for the player"""
    if request.steam_id not in matchmaking_queue:
        return {"status": "not_in_queue"}

    player_data = matchmaking_queue[request.steam_id]
    player_elo = player_data["elo"]

    # Find an opponent with a similar Elo rating
    for opponent_id, opponent_data in matchmaking_queue.items():
        if opponent_id != request.steam_id and abs(opponent_data["elo"] - player_elo) < 100:
            # Found a suitable opponent
            logger.info(f"Found opponent for {request.steam_id}: {opponent_id}")
            return {"status": "found_opponent", "opponent": opponent_data}

    logger.info(f"No opponent found for {request.steam_id}")
    return {"status": "waiting_for_opponent", "player_count": len(matchmaking_queue)}

@app.get("/leaderboard")
def get_leaderboard():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT steam_id, steam_name, elo, wins, losses FROM players ORDER BY elo DESC LIMIT 10")
    rows = cur.fetchall()
    leaderboard = []
    for row in rows:
        leaderboard.append({
            "steam_id": row[0],
            "steam_name": row[1],
            "elo": row[2],
            "wins": row[3],
            "losses": row[4]
        })
    cur.close()
    conn.close()
    return {"leaderboard": leaderboard}