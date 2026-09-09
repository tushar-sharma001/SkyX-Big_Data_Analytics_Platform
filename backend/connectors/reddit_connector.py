"""
reddit_connector.py
---------------------
REAL social-media connector using Reddit's official public API (via PRAW).
Chosen over the Twitter/X API for the reference build because Reddit's
API is free to register for (developer app credentials, no paid tier),
whereas X's API v2 now requires a paid Basic/Pro plan for search access —
see twitter_connector.py, which is written the same way and will work
immediately if you have (or later obtain) a bearer token.

Pulls recent posts from India-focused and weather-focused subreddits that
mention weather keywords / #IMD-style hashtag text, which is exactly the
"citizen reports on social media" ingestion source in the problem
statement — genuinely public posts, not synthetic text.

Requires free Reddit API credentials (see README "Live connector setup"):
    REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT
If those env vars are not set, get_recent_weather_posts() returns []
immediately rather than raising, so the platform runs fine without them.
"""

import os
import re

SUBREDDITS = ["india", "IndiaSpeaks", "developersIndia", "delhi", "mumbai", "bangalore"]
WEATHER_KEYWORDS = ["rain", "flood", "heatwave", "heat wave", "dust storm",
                     "cold wave", "thunderstorm", "cyclone", "imd", "waterlogged", "waterlogging"]

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


def _get_client():
    """Returns a configured praw.Reddit client, or None if credentials are
    not configured — callers must handle the None case."""
    client_id = os.environ.get("REDDIT_CLIENT_ID")
    client_secret = os.environ.get("REDDIT_CLIENT_SECRET")
    user_agent = os.environ.get("REDDIT_USER_AGENT", "weatherpulse-sih26069/1.0")
    if not client_id or not client_secret:
        return None
    import praw
    return praw.Reddit(client_id=client_id, client_secret=client_secret, user_agent=user_agent)


def get_recent_weather_posts(limit_per_subreddit: int = 15) -> list:
    """Real PRAW calls against Reddit's public API. Returns a list of
    normalized report dicts. Returns [] (does not raise) if credentials
    are missing or any subreddit fetch fails, so ingestion keeps running."""
    reddit = _get_client()
    if reddit is None:
        return []

    reports = []
    for sub_name in SUBREDDITS:
        try:
            subreddit = reddit.subreddit(sub_name)
            for post in subreddit.new(limit=limit_per_subreddit):
                title = post.title or ""
                body = (post.selftext or "")[:280]
                combined = f"{title} {body}".strip()
                if not any(kw in combined.lower() for kw in WEATHER_KEYWORDS):
                    continue
                reports.append({
                    "source": "reddit",
                    "user_handle": f"u/{post.author.name}" if post.author else "u/deleted",
                    "state": "Unknown",
                    "district": "Unknown",
                    "latitude": None,
                    "longitude": None,
                    "text": combined,
                    "event_type": _classify(combined),
                    "severity": "reported",
                    "source_credibility": 0.4,  # unverified social post prior
                    "timestamp": None,
                })
        except Exception as e:
            print(f"[reddit_connector] fetch failed for r/{sub_name}: {e}")
            continue
    return reports
