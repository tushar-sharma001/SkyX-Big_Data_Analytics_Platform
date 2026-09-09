"""
pipeline.py
-----------
The ML/AI layer of the platform. Three models trained on the seed dataset:

1. EventClassifier   - TF-IDF + Linear SVM (calibrated) -> predicts event_type
                        (rainfall / flood / heatwave / dust_storm / cold_wave)
2. FakeReportDetector - Logistic Regression over text-derived + metadata
                        features -> P(report is fake/misleading)
3. DuplicateDetector  - TF-IDF cosine-similarity clustering -> groups
                        near-duplicate posts about the same event so the
                        dashboard shows one verified card instead of a flood
                        of re-posts.

All models are trained once at startup (or via `python3 pipeline.py train`)
and persisted with joblib so the Flask app just loads them.
"""

import os
import re
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import joblib
import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.svm import LinearSVC
from sklearn.calibration import CalibratedClassifierCV
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, accuracy_score
from sklearn.metrics.pairwise import cosine_similarity
from ml.corroboration import CorroborationEngine, compute_trust_score

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MODEL_DIR = os.path.join(BASE_DIR, "models")
DATA_PATH = os.path.join(BASE_DIR, "data", "weather_reports_seed.csv")
os.makedirs(MODEL_DIR, exist_ok=True)

EXAGGERATION_WORDS = [
    "breaking", "urgent", "100% confirmed", "unbelievable", "share before",
    "forward to everyone", "govt hiding", "secret", "conspiracy", "wake up",
    "unverified forward", "confirm", "!!!", "gone", "collapsed", "missing",
]


# ---------------------------------------------------------------------------
# Feature engineering helpers
# ---------------------------------------------------------------------------
def _clean(text: str) -> str:
    text = text.lower()
    text = re.sub(r"http\S+", " ", text)
    text = re.sub(r"[^a-z0-9#\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _exaggeration_score(text: str) -> float:
    t = text.lower()
    hits = sum(1 for w in EXAGGERATION_WORDS if w in t)
    caps_ratio = sum(1 for c in text if c.isupper()) / max(len(text), 1)
    exclaim = text.count("!")
    return hits * 1.0 + caps_ratio * 3.0 + min(exclaim, 5) * 0.5


def build_fake_features(df: pd.DataFrame) -> np.ndarray:
    exag = df["text"].apply(_exaggeration_score).values.reshape(-1, 1)
    cred = df["source_credibility"].values.reshape(-1, 1)
    is_forward = (df["source"] == "whatsapp_forward").astype(int).values.reshape(-1, 1)
    handle_generic = df["user_handle"].str.startswith("@user").astype(int).values.reshape(-1, 1)
    text_len = df["text"].str.len().values.reshape(-1, 1) / 100.0
    return np.hstack([exag, cred, is_forward, handle_generic, text_len])


# ---------------------------------------------------------------------------
# Training
# ---------------------------------------------------------------------------
def train(verbose=True):
    df = pd.read_csv(DATA_PATH)
    df["clean_text"] = df["text"].apply(_clean)

    # 1) Event type classifier -------------------------------------------------
    vec_event = TfidfVectorizer(max_features=4000, ngram_range=(1, 2), min_df=1)
    X_text = vec_event.fit_transform(df["clean_text"])
    y_event = df["event_type"]

    Xtr, Xte, ytr, yte = train_test_split(X_text, y_event, test_size=0.2,
                                           random_state=42, stratify=y_event)
    svc = LinearSVC(class_weight="balanced", random_state=42)
    clf_event = CalibratedClassifierCV(svc, cv=3)
    clf_event.fit(Xtr, ytr)
    if verbose:
        preds = clf_event.predict(Xte)
        print("== Event classifier ==")
        print(f"accuracy: {accuracy_score(yte, preds):.3f}")
        print(classification_report(yte, preds, zero_division=0))

    # 2) Fake / misleading report detector -------------------------------------
    vec_fake = TfidfVectorizer(max_features=2000, ngram_range=(1, 2), min_df=1)
    X_fake_text = vec_fake.fit_transform(df["clean_text"])
    X_fake_meta = build_fake_features(df)
    from scipy.sparse import hstack
    X_fake = hstack([X_fake_text, X_fake_meta]).tocsr()
    y_fake = df["label_is_fake"]

    Xtr2, Xte2, ytr2, yte2 = train_test_split(X_fake, y_fake, test_size=0.2,
                                               random_state=42, stratify=y_fake)
    clf_fake = LogisticRegression(max_iter=2000, class_weight="balanced")
    clf_fake.fit(Xtr2, ytr2)
    if verbose:
        preds2 = clf_fake.predict(Xte2)
        print("== Fake-report detector ==")
        print(f"accuracy: {accuracy_score(yte2, preds2):.3f}")
        print(classification_report(yte2, preds2, zero_division=0))

    # 3) Duplicate-detection vectorizer (separate, tuned for short-text sim) ---
    vec_dup = TfidfVectorizer(max_features=3000, ngram_range=(1, 2), min_df=1)
    vec_dup.fit(df["clean_text"])

    joblib.dump(vec_event, os.path.join(MODEL_DIR, "vec_event.joblib"))
    joblib.dump(clf_event, os.path.join(MODEL_DIR, "clf_event.joblib"))
    joblib.dump(vec_fake, os.path.join(MODEL_DIR, "vec_fake.joblib"))
    joblib.dump(clf_fake, os.path.join(MODEL_DIR, "clf_fake.joblib"))
    joblib.dump(vec_dup, os.path.join(MODEL_DIR, "vec_dup.joblib"))
    if verbose:
        print(f"Models saved to {MODEL_DIR}")
    return {
        "vec_event": vec_event, "clf_event": clf_event,
        "vec_fake": vec_fake, "clf_fake": clf_fake, "vec_dup": vec_dup,
    }


# ---------------------------------------------------------------------------
# Inference wrapper used by the Flask app / ingestion worker
# ---------------------------------------------------------------------------
class WeatherMLPipeline:
    """Loads (or trains) all three models and exposes a single `process()`
    call used by the ingestion pipeline for every incoming report."""

    DUP_SIMILARITY_THRESHOLD = 0.72

    def __init__(self):
        paths = {
            "vec_event": os.path.join(MODEL_DIR, "vec_event.joblib"),
            "clf_event": os.path.join(MODEL_DIR, "clf_event.joblib"),
            "vec_fake": os.path.join(MODEL_DIR, "vec_fake.joblib"),
            "clf_fake": os.path.join(MODEL_DIR, "clf_fake.joblib"),
            "vec_dup": os.path.join(MODEL_DIR, "vec_dup.joblib"),
        }
        if not all(os.path.exists(p) for p in paths.values()):
            models = train(verbose=True)
        else:
            models = {k: joblib.load(p) for k, p in paths.items()}

        self.vec_event = models["vec_event"]
        self.clf_event = models["clf_event"]
        self.vec_fake = models["vec_fake"]
        self.clf_fake = models["clf_fake"]
        self.vec_dup = models["vec_dup"]

        # rolling window of recent reports' dup-vectors for near-dup lookups
        self._recent_texts = []
        self._recent_vecs = None
        self._recent_ids = []
        self._window = 500

        # unique differentiator: cross-check every report against real
        # official IMD bulletins rather than relying on text signals alone
        self.corroboration = CorroborationEngine()

    # -- individual model calls ---------------------------------------------
    def classify_event(self, text: str):
        clean = _clean(text)
        X = self.vec_event.transform([clean])
        proba = self.clf_event.predict_proba(X)[0]
        classes = self.clf_event.classes_
        idx = int(np.argmax(proba))
        return classes[idx], float(proba[idx])

    def fake_score(self, row: dict) -> float:
        df = pd.DataFrame([row])
        df["clean_text"] = df["text"].apply(_clean)
        X_text = self.vec_fake.transform(df["clean_text"])
        X_meta = build_fake_features(df)
        from scipy.sparse import hstack
        X = hstack([X_text, X_meta]).tocsr()
        proba = self.clf_fake.predict_proba(X)[0]
        classes = list(self.clf_fake.classes_)
        return float(proba[classes.index(1)]) if 1 in classes else 0.0

    def find_duplicate(self, text: str, report_id: str):
        """Returns (duplicate_of_id, similarity) or (None, 0.0)."""
        clean = _clean(text)
        vec = self.vec_dup.transform([clean])
        if self._recent_vecs is not None and self._recent_vecs.shape[0] > 0:
            sims = cosine_similarity(vec, self._recent_vecs)[0]
            best = int(np.argmax(sims))
            if sims[best] >= self.DUP_SIMILARITY_THRESHOLD:
                dup_id = self._recent_ids[best]
                self._push_recent(clean, vec, report_id)
                return dup_id, float(sims[best])
        self._push_recent(clean, vec, report_id)
        return None, 0.0

    def _push_recent(self, clean_text, vec, report_id):
        from scipy.sparse import vstack
        self._recent_texts.append(clean_text)
        self._recent_ids.append(report_id)
        self._recent_vecs = vec if self._recent_vecs is None else vstack([self._recent_vecs, vec])
        if len(self._recent_ids) > self._window:
            self._recent_texts.pop(0)
            self._recent_ids.pop(0)
            self._recent_vecs = self._recent_vecs[1:]

    # -- full processing used by the ingestion worker ------------------------
    def process(self, report: dict) -> dict:
        """report needs: text, source, user_handle, source_credibility, report_id,
        state, district, timestamp"""
        event_type, event_conf = self.classify_event(report["text"])
        fake_p = self.fake_score(report)
        dup_of, dup_sim = self.find_duplicate(report["text"], report["report_id"])

        # --- unique differentiator: cross-check against real official IMD
        # bulletins (not just a language classifier's opinion of the text)
        corrob = self.corroboration.check(
            state=report.get("state", ""), district=report.get("district", ""),
            event_type=event_type, timestamp=report.get("timestamp", ""))
        trust_score = compute_trust_score(
            corrob["corroboration_score"], fake_p, report.get("source_credibility", 0.5))

        if corrob["verdict"] == "contradicted" or fake_p >= 0.65:
            status = "flagged_fake"
        elif dup_of:
            status = "duplicate"
        elif corrob["verdict"] == "corroborated" and trust_score >= 0.55:
            status = "verified_by_imd"
        elif trust_score >= 0.75:
            status = "verified"
        else:
            status = "pending_review"

        return {
            **report,
            "predicted_event_type": event_type,
            "event_confidence": round(event_conf, 3),
            "fake_probability": round(fake_p, 3),
            "duplicate_of": dup_of,
            "duplicate_similarity": round(dup_sim, 3),
            "corroboration_verdict": corrob["verdict"],
            "corroboration_score": corrob["corroboration_score"],
            "matched_bulletin_id": corrob["matched_bulletin_id"],
            "trust_score": trust_score,
            "status": status,
        }


if __name__ == "__main__":
    train(verbose=True)
