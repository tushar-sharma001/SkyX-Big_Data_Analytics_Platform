"""
corroboration.py — the platform's differentiator.

Problem statement explicitly asks for "verify sources" in addition to
fake-filtering, dedup, and classification. Most implementations of this
idea stop at a fake/not-fake language classifier. This module goes one
step further: it cross-checks every incoming citizen report against a
table of official IMD bulletins (district + event type + a date window)
and produces one of three verdicts:

    corroborated        - an official bulletin exists for this district,
                           event type, within +/-1 day of the report
    no_official_match   - no matching bulletin found (report may still be
                           genuine - e.g. a very local/early event IMD
                           hasn't bulletined yet - so this is a signal,
                           not an automatic rejection)
    contradicted         - an official bulletin exists for this district/date
                           but for a *different* event type (e.g. someone
                           reporting "flood" where IMD's bulletin for that
                           district/day says "heatwave")

The corroboration score is then blended with the ML fake-probability and
the source credibility into a single trust_score that drives the final
status. This is the "verify sources" requirement made concrete, and it's
what separates this build from a plain fake-news text classifier.

In production `official_bulletins.csv` would be replaced by a live poll of
the IMD API / RSS bulletin feed - the matching logic below does not change.
"""

import os
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BULLETIN_PATH = os.path.join(BASE_DIR, "data", "imd_official_bulletins.csv")

DATE_WINDOW_DAYS = 1


class CorroborationEngine:
    def __init__(self, bulletin_path=BULLETIN_PATH):
        self.df = pd.read_csv(bulletin_path)
        self.df["date"] = pd.to_datetime(self.df["date"])
        self._by_loc = {}
        for _, row in self.df.iterrows():
            key = (row["state"], row["district"])
            self._by_loc.setdefault(key, []).append(row)

    def check(self, state: str, district: str, event_type: str, timestamp: str):
        """Returns dict: verdict, corroboration_score (0-1), matched_bulletin_id."""
        try:
            report_date = pd.to_datetime(timestamp).normalize()
        except Exception:
            report_date = pd.Timestamp.now().normalize()

        candidates = self._by_loc.get((state, district), [])
        if not candidates:
            return {"verdict": "no_official_match", "corroboration_score": 0.0,
                    "matched_bulletin_id": None}

        best_same_event = None
        best_diff_event = None
        for row in candidates:
            delta = abs((row["date"] - report_date).days)
            if delta <= DATE_WINDOW_DAYS:
                if row["event_type"] == event_type:
                    if best_same_event is None or delta < best_same_event[1]:
                        best_same_event = (row, delta)
                else:
                    if best_diff_event is None or delta < best_diff_event[1]:
                        best_diff_event = (row, delta)

        if best_same_event:
            row, delta = best_same_event
            score = 1.0 if delta == 0 else 0.75
            return {"verdict": "corroborated", "corroboration_score": score,
                    "matched_bulletin_id": row["bulletin_id"]}
        if best_diff_event:
            row, delta = best_diff_event
            return {"verdict": "contradicted", "corroboration_score": 0.1,
                    "matched_bulletin_id": row["bulletin_id"]}
        return {"verdict": "no_official_match", "corroboration_score": 0.35,
                "matched_bulletin_id": None}


def compute_trust_score(corroboration_score: float, fake_probability: float,
                         source_credibility: float) -> float:
    """Weighted blend that the final status decision is based on.
    Corroboration against an official source is weighted highest because
    it is the strongest available signal of ground truth."""
    trust = (0.5 * corroboration_score) + (0.3 * (1 - fake_probability)) + (0.2 * source_credibility)
    return round(min(max(trust, 0.0), 1.0), 3)
