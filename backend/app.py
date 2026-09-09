"""
app.py - SkyX — National Weather Intelligence Platform (SIH26069 reference build)

Flask API + dashboard server. Wires together:
  db.py              - centralized SQLite store (stand-in for the big-data
                        serving layer; see db.py docstring for the
                        Kafka/Spark swap-in point)
  ml/pipeline.py      - classification, fake-report detection, dedup
  ingestion/worker.py - background streaming ingestion simulator

Run:
    python3 app.py
Then open http://localhost:5000
"""

import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from flask import Flask, jsonify, request, render_template
from flask_cors import CORS

import db as dbmod
from ml.pipeline import WeatherMLPipeline
from ingestion.worker import IngestionWorker, bulk_load_seed

app = Flask(__name__, static_folder="static", template_folder="templates")
CORS(app)

ML = WeatherMLPipeline()
WORKER = IngestionWorker(ML)


def bootstrap():
    fresh = not os.path.exists(dbmod.DB_PATH)
    dbmod.init_db(reset=False)
    if fresh:
        print("[bootstrap] empty DB detected — backfilling historical reports...")
        bulk_load_seed(ML, limit=400)
    WORKER.start()


# ---------------------------------------------------------------------------
# Pages
# ---------------------------------------------------------------------------
@app.route("/")
def dashboard():
    return render_template("dashboard.html")


@app.route("/admin")
def admin():
    return render_template("admin.html")


# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------
@app.route("/api/reports")
def api_reports():
    filters = {
        "event_type": request.args.get("event_type") or None,
        "state": request.args.get("state") or None,
        "status": request.args.get("status") or None,
        "date_from": request.args.get("date_from") or None,
        "date_to": request.args.get("date_to") or None,
        "q": request.args.get("q") or None,
    }
    limit = int(request.args.get("limit", 500))
    rows = dbmod.query_reports(filters, limit=limit)
    return jsonify({"count": len(rows), "reports": rows})


@app.route("/api/stats")
def api_stats():
    return jsonify(dbmod.get_stats())


@app.route("/api/states")
def api_states():
    conn = dbmod.get_conn()
    rows = conn.execute("SELECT DISTINCT state FROM reports ORDER BY state").fetchall()
    conn.close()
    return jsonify([r["state"] for r in rows if r["state"]])


@app.route("/api/ingest", methods=["POST"])
def api_ingest():
    """Manual ingestion endpoint — e.g. a citizen-report mobile app or a
    public API poller would POST here in production."""
    payload = request.get_json(force=True)
    required = ["text", "source", "state", "district"]
    missing = [k for k in required if k not in payload]
    if missing:
        return jsonify({"error": f"missing fields: {missing}"}), 400

    import uuid
    from datetime import datetime
    raw = {
        "report_id": str(uuid.uuid4())[:8],
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": payload.get("source", "citizen_app"),
        "user_handle": payload.get("user_handle", "@anonymous"),
        "state": payload["state"],
        "district": payload["district"],
        "latitude": payload.get("latitude", 22.9734),
        "longitude": payload.get("longitude", 78.6569),
        "text": payload["text"],
        "event_type": payload.get("event_type", "rainfall"),
        "severity": payload.get("severity", "reported"),
        "source_credibility": payload.get("source_credibility", 0.5),
    }
    processed = ML.process(raw)
    dbmod.insert_report(processed)
    return jsonify(processed), 201


@app.route("/api/admin/verify", methods=["POST"])
def api_admin_verify():
    payload = request.get_json(force=True)
    report_id = payload.get("report_id")
    new_status = payload.get("status")
    if new_status not in {"verified", "verified_by_imd", "flagged_fake", "duplicate", "rejected", "pending_review"}:
        return jsonify({"error": "invalid status"}), 400
    dbmod.set_admin_status(report_id, new_status)
    return jsonify({"ok": True, "report_id": report_id, "status": new_status})


@app.route("/api/health")
def api_health():
    return jsonify({"status": "ok", "ingestion_running": WORKER._thread.is_alive() if WORKER._thread else False})


bootstrap()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False, use_reloader=False)
