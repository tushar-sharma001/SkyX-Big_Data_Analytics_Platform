#!/bin/bash
# Quick start script for the SkyX — National Weather Intelligence Platform
set -e
cd "$(dirname "$0")"
python3 -m venv venv 2>/dev/null || true
source venv/bin/activate
pip install -q -r requirements.txt
cd backend/data && python3 build_dataset.py && cd ../..
cd backend/ml && python3 pipeline.py && cd ../..
cd backend && python3 app.py
