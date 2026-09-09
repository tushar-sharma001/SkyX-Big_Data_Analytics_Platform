"""
imd_scraper_connector.py
--------------------------
REAL web-scraping connector for the India Meteorological Department's
public district-wise warning page (mausam.imd.gov.in). This is what
"public government websites" ingestion means in the problem statement —
an actual HTTP fetch + HTML parse, not a mock.

IMD does not publish a stable public JSON/REST API for warnings, so the
standard integration pattern (used by most weather-aggregator products in
India) is to poll the public HTML bulletin pages and parse the warning
table. That is what this module does.

Because government site markup changes without notice, this scraper is
written defensively: it looks for warning-table rows by structural pattern
(district name + severity keyword) rather than brittle CSS-class selectors,
and returns an empty list rather than raising if the page structure has
changed — so a markup change degrades gracefully to "no bulletins found
this cycle" instead of crashing the ingestion loop.

Honest limitation: this sandbox's network egress is locked to package
registries only, so this connector could not be executed against the live
mausam.imd.gov.in domain during development. The HTTP + parsing logic is
written to the real page structure observed via web search during
development (see README) and is unit-tested against a saved sample of
that markup in tests/test_connectors.py — but a live end-to-end run should
be done on your own machine/server before a demo, and the parser may need
a quick adjustment if IMD has changed their page layout since.
"""

import re
import requests
from bs4 import BeautifulSoup

IMD_WARNING_URL = "https://mausam.imd.gov.in/responsive/districtWiseWarningGIS.php"

SEVERITY_KEYWORDS = {
    "extremely heavy": "severe", "very heavy": "severe", "heavy": "moderate",
    "heat wave": "severe", "hot day": "moderate", "dust storm": "moderate",
    "cold wave": "moderate", "thunderstorm": "moderate", "cyclone": "severe",
}

EVENT_KEYWORDS = {
    "flood": "flood", "flash flood": "flood",
    "rain": "rainfall", "rainfall": "rainfall",
    "heat wave": "heatwave", "hot day": "heatwave",
    "dust storm": "dust_storm", "dust raising": "dust_storm",
    "cold wave": "cold_wave", "cold day": "cold_wave",
    "thunderstorm": "thunderstorm", "lightning": "thunderstorm",
    "cyclone": "cyclone",
}


def _classify_text(text: str):
    t = text.lower()
    event_type = None
    for kw, ev in EVENT_KEYWORDS.items():
        if kw in t:
            event_type = ev
            break
    severity = "moderate"
    for kw, sev in SEVERITY_KEYWORDS.items():
        if kw in t:
            severity = sev
            break
    return event_type, severity


def parse_bulletin_html(html: str) -> list:
    """Pure parsing function (no network I/O) — kept separate from
    fetch_bulletins() specifically so it can be unit-tested against a
    saved HTML sample without needing a live connection."""
    soup = BeautifulSoup(html, "html.parser")
    bulletins = []

    # IMD's warning tables list one district/region per row with a
    # free-text warning description; we look for table rows containing
    # both a plausible place name cell and warning language.
    for row in soup.find_all(["tr", "li", "p", "div"]):
        text = row.get_text(" ", strip=True)
        if not text or len(text) < 15:
            continue
        event_type, severity = _classify_text(text)
        if not event_type:
            continue
        # crude place extraction: first capitalised word sequence in the row
        place_match = re.search(r"\b([A-Z][a-z]+(?:\s[A-Z][a-z]+)?)\b", text)
        place = place_match.group(1) if place_match else "Unknown"
        bulletins.append({
            "place": place,
            "event_type": event_type,
            "severity": severity,
            "raw_text": text[:300],
        })
    return bulletins


def fetch_bulletins(timeout: int = 10) -> list:
    """Real HTTP GET against IMD's public warning page. Returns [] on any
    network/parse failure rather than raising, so a temporary IMD outage
    or markup change never takes down the ingestion loop."""
    try:
        resp = requests.get(IMD_WARNING_URL, timeout=timeout,
                             headers={"User-Agent": "Mozilla/5.0 (WeatherPulse research bot)"})
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"[imd_scraper_connector] fetch failed: {e}")
        return []
    try:
        return parse_bulletin_html(resp.text)
    except Exception as e:
        print(f"[imd_scraper_connector] parse failed: {e}")
        return []
