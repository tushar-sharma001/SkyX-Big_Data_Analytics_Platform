"""
db.py - lightweight SQLite persistence layer.

In production this centralized store is what a Kafka/Spark streaming layer
would ultimately write into (e.g. a data lake + a serving store such as
Postgres/Elasticsearch). SQLite is used here so the whole platform runs
standalone for a hackathon demo; `ingestion/worker.py` is written so the
write path can be swapped for a Kafka producer + Spark Structured Streaming
consumer without touching the ML layer or the API contract.
"""

import os
import sqlite3
import threading

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "weatherpulse.db")

_lock = threading.Lock()

SCHEMA = """
CREATE TABLE IF NOT EXISTS reports (
    report_id TEXT PRIMARY KEY,
    timestamp TEXT NOT NULL,
    source TEXT,
    user_handle TEXT,
    state TEXT,
    district TEXT,
    latitude REAL,
    longitude REAL,
    text TEXT,
    reported_event_type TEXT,
    severity TEXT,
    source_credibility REAL,
    predicted_event_type TEXT,
    event_confidence REAL,
    fake_probability REAL,
    duplicate_of TEXT,
    duplicate_similarity REAL,
    corroboration_verdict TEXT,
    corroboration_score REAL,
    matched_bulletin_id TEXT,
    trust_score REAL,
    status TEXT,
    admin_override TEXT,
    ingested_at TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_reports_ts ON reports(timestamp);
CREATE INDEX IF NOT EXISTS idx_reports_event ON reports(predicted_event_type);
CREATE INDEX IF NOT EXISTS idx_reports_state ON reports(state);
CREATE INDEX IF NOT EXISTS idx_reports_status ON reports(status);

CREATE TABLE IF NOT EXISTS ingestion_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT DEFAULT (datetime('now')),
    batch_size INTEGER,
    source_mix TEXT
);
"""


def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(reset=False):
    if reset and os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    conn = get_conn()
    conn.executescript(SCHEMA)
    conn.commit()
    conn.close()


def insert_report(processed: dict):
    with _lock:
        conn = get_conn()
        conn.execute("""
            INSERT OR IGNORE INTO reports
            (report_id, timestamp, source, user_handle, state, district, latitude, longitude,
             text, reported_event_type, severity, source_credibility, predicted_event_type,
             event_confidence, fake_probability, duplicate_of, duplicate_similarity,
             corroboration_verdict, corroboration_score, matched_bulletin_id, trust_score, status)
            VALUES (:report_id, :timestamp, :source, :user_handle, :state, :district,
                    :latitude, :longitude, :text, :event_type, :severity, :source_credibility,
                    :predicted_event_type, :event_confidence, :fake_probability,
                    :duplicate_of, :duplicate_similarity,
                    :corroboration_verdict, :corroboration_score, :matched_bulletin_id,
                    :trust_score, :status)
        """, processed)
        conn.commit()
        conn.close()


def log_batch(batch_size: int, source_mix: str):
    with _lock:
        conn = get_conn()
        conn.execute("INSERT INTO ingestion_log (batch_size, source_mix) VALUES (?, ?)",
                      (batch_size, source_mix))
        conn.commit()
        conn.close()


def query_reports(filters: dict, limit=500):
    conn = get_conn()
    clauses, params = [], []
    if filters.get("event_type"):
        clauses.append("predicted_event_type = ?")
        params.append(filters["event_type"])
    if filters.get("state"):
        clauses.append("state = ?")
        params.append(filters["state"])
    if filters.get("status"):
        clauses.append("status = ?")
        params.append(filters["status"])
    if filters.get("date_from"):
        clauses.append("timestamp >= ?")
        params.append(filters["date_from"])
    if filters.get("date_to"):
        clauses.append("timestamp <= ?")
        params.append(filters["date_to"])
    if filters.get("q"):
        clauses.append("text LIKE ?")
        params.append(f"%{filters['q']}%")

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    sql = f"SELECT * FROM reports {where} ORDER BY timestamp DESC LIMIT ?"
    params.append(limit)
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_stats():
    conn = get_conn()
    total = conn.execute("SELECT COUNT(*) c FROM reports").fetchone()["c"]
    by_event = conn.execute(
        "SELECT predicted_event_type e, COUNT(*) c FROM reports GROUP BY e ORDER BY c DESC"
    ).fetchall()
    by_status = conn.execute(
        "SELECT status s, COUNT(*) c FROM reports GROUP BY s"
    ).fetchall()
    by_state = conn.execute(
        "SELECT state, COUNT(*) c FROM reports GROUP BY state ORDER BY c DESC LIMIT 15"
    ).fetchall()
    timeline = conn.execute(
        "SELECT substr(timestamp,1,10) day, COUNT(*) c FROM reports GROUP BY day ORDER BY day"
    ).fetchall()
    avg_fake = conn.execute("SELECT AVG(fake_probability) a FROM reports").fetchone()["a"]
    avg_trust = conn.execute("SELECT AVG(trust_score) a FROM reports").fetchone()["a"]
    corroborated = conn.execute(
        "SELECT COUNT(*) c FROM reports WHERE corroboration_verdict='corroborated'"
    ).fetchone()["c"]
    conn.close()
    return {
        "total": total,
        "by_event": [dict(r) for r in by_event],
        "by_status": [dict(r) for r in by_status],
        "by_state": [dict(r) for r in by_state],
        "timeline": [dict(r) for r in timeline],
        "avg_fake_probability": round(avg_fake or 0.0, 3),
        "avg_trust_score": round(avg_trust or 0.0, 3),
        "imd_corroborated": corroborated,
        "imd_corroborated_pct": round(100 * corroborated / total, 1) if total else 0.0,
    }


def set_admin_status(report_id: str, new_status: str):
    with _lock:
        conn = get_conn()
        conn.execute("UPDATE reports SET status=?, admin_override=? WHERE report_id=?",
                      (new_status, new_status, report_id))
        conn.commit()
        conn.close()
