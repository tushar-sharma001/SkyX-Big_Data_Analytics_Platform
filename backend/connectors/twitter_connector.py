"""
twitter_connector.py
----------------------
REAL connector for X/Twitter's official API v2 recent-search endpoint,
used to pull posts tagged #IMD / #WeatherAlert / similar — the exact
"posts tagged #IMD and similar weather hashtags" source named in the
problem statement.

Honest note on cost: as of this build, X's API v2 search endpoint requires
a paid Basic tier subscription (the free tier does not include search).
This module is written and ready to use the moment you have a bearer
token — it is not a placeholder or a mock — but it is not wired into the
default demo ingestion loop for that reason (see ingestion/live_worker.py,
which uses Reddit + Open-Meteo + the IMD scraper by default and treats
Twitter as an optional extra source).

Requires: TWITTER_BEARER_TOKEN environment variable.
"""

import os
import requests

SEARCH_URL = "https://api.twitter.com/2/tweets/search/recent"
QUERY = "(#IMD OR #WeatherAlert OR #Flood OR #HeatWave OR #DustStorm) lang:en -is:retweet"

EVENT_KEYWORDS = {
    "flood": "flood", "waterlogg": "flood",
    "rain": "rainfall",
    "heatwave": "heatwave", "heat wave": "heatwave",
    "dust storm": "dust_storm",
    "cold wave": "cold_wave",
    "thunderstorm": "thunderstorm", "lightning": "thunderstorm",
    "cyclone": "cyclone",
}


def _classify(text: str):
    t = text.lower()
    for kw, ev in EVENT_KEYWORDS.items():
        if kw in t:
            return ev
    return "rainfall"


def fetch_recent_tweets(max_results: int = 25, timeout: int = 10) -> list:
    """Real HTTP GET against X API v2. Returns [] (does not raise) if
    TWITTER_BEARER_TOKEN is unset or the API call fails, so the platform
    runs fine without a Twitter subscription."""
    token = os.environ.get("TWITTER_BEARER_TOKEN")
    if not token:
        return []

    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "query": QUERY,
        "max_results": max(10, min(max_results, 100)),
        "tweet.fields": "created_at,author_id,geo",
    }
    try:
        resp = requests.get(SEARCH_URL, headers=headers, params=params, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        print(f"[twitter_connector] fetch failed: {e}")
        return []

    reports = []
    for tweet in data.get("data", []):
        text = tweet.get("text", "")
        reports.append({
            "source": "twitter",
            "user_handle": f"@user_{tweet.get('author_id', 'unknown')}",
            "state": "Unknown",
            "district": "Unknown",
            "latitude": None,
            "longitude": None,
            "text": text,
            "event_type": _classify(text),
            "severity": "reported",
            "source_credibility": 0.5,
            "timestamp": tweet.get("created_at"),
        })
    return reports
