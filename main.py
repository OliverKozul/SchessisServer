from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import psycopg2
import os
import logging
from datetime import datetime

logger = logging.getLogger("uvicorn")

app = FastAPI()

K = 32  # sensitivity constant for Elo

# --------------------------
# Database setup
# --------------------------
DATABASE_URL = os.getenv("DATABASE_URL")  # Render injects this

def get_conn():
    return psycopg2.connect(DATABASE_URL, sslmode="require")

def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS players (
            steam_id TEXT PRIMARY KEY,
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


# --------------------------
# Helpers
# --------------------------
def get_player(steam_id: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT steam_id, elo, wins, losses FROM players WHERE steam_id = %s", (steam_id,))
    row = cur.fetchone()
    if row:
        player = {"steam_id": row[0], "elo": row[1], "wins": row[2], "losses": row[3]}
    else:
        player = {"steam_id": steam_id, "elo": 1000, "wins": 0, "losses": 0}
        cur.execute("INSERT INTO players (steam_id, elo, wins, losses) VALUES (%s, %s, %s, %s)", 
                    (steam_id, 1000, 0, 0))
        conn.commit()
    cur.close()
    conn.close()
    return player


def update_player(player):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE players
        SET elo = %s, wins = %s, losses = %s
        WHERE steam_id = %s
    """, (player["elo"], player["wins"], player["losses"], player["steam_id"]))
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
    return round(player_rating + K * (score - expected))


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
def get_player_info(steam_id: str):
    player = get_player(steam_id)
    if not player:
        raise HTTPException(status_code=404, detail="Player not found")
    history = get_match_history(steam_id)
    return {"player": player, "history": history}
