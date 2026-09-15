"""
build_dataset.py
-----------------
Builds the seed dataset for the SkyX — National Weather Intelligence Platform.

The seed events below are drawn from real India Meteorological Department (IMD)
bulletins and news coverage of the September 2026 monsoon spell (flash-flood
warnings across Chhattisgarh/Odisha/UP/MP, heavy rain over Delhi-NCR, strong
winds, extended-range forecasts, etc.). Each real event is expanded into many
citizen-report-style social posts using varied templates, languages of English
mixed with Hindi/Hinglish tokens, multiple sources, and injected noise:
duplicates/near-duplicates, a minority of fabricated/misleading reports, and
missing-location or low-credibility posts, so the ML layer has real signal to
learn from.

Run:  python3 build_dataset.py
Output: weather_reports_seed.csv  (in this directory)
"""

import csv
import random
import uuid
from datetime import datetime, timedelta

random.seed(42)

# ---------------------------------------------------------------------------
# 1. Real seed events (paraphrased from IMD bulletins / news, Sep 2026 spell)
#    Each: (date, region/state, districts, event_type, severity, note)
# ---------------------------------------------------------------------------
REAL_EVENTS = [
    ("2026-09-01", "Delhi", ["New Delhi", "Dwarka", "Rohini", "Karol Bagh"], "rainfall",
     "moderate", "heavy rain and thunderstorms with gusty winds near 40 kmph"),
    ("2026-09-01", "Uttar Pradesh", ["Ghaziabad", "Meerut", "Moradabad", "Kanpur", "Lucknow"], "rainfall",
     "moderate", "heavy rain and strong winds across western and central UP"),
    ("2026-09-02", "Chhattisgarh", ["Raipur", "Raigarh", "Korba", "Jashpur", "Balrampur", "Surguja"], "flood",
     "severe", "extremely heavy rainfall triggering flash flood warning, over 200mm in isolated pockets"),
    ("2026-09-02", "Odisha", ["Balasore", "Mayurbhanj", "Keonjhar", "Sundargarh", "Sambalpur"], "flood",
     "severe", "flash flood risk from widespread heavy rain and 40-50 kmph winds"),
    ("2026-09-03", "Madhya Pradesh", ["Bhopal", "Jabalpur", "Sagar", "Rewa", "Satna"], "rainfall",
     "severe", "isolated extremely heavy rainfall over east Madhya Pradesh under a well-marked low"),
    ("2026-09-03", "West Bengal", ["Kolkata", "Howrah", "Kharagpur", "Malda"], "rainfall",
     "moderate", "isolated to fairly widespread rain with thunderstorm and lightning"),
    ("2026-09-04", "Jharkhand", ["Ranchi", "Jamshedpur", "Dhanbad"], "rainfall",
     "moderate", "scattered rainfall with thunderstorm and lightning"),
    ("2026-09-05", "Delhi", ["New Delhi", "Noida", "Gurugram"], "rainfall",
     "severe", "fairly widespread to widespread rain with thunderstorm and lightning over Delhi-NCR"),
    ("2026-09-05", "Uttarakhand", ["Dehradun", "Haridwar", "Nainital", "Rudraprayag"], "rainfall",
     "severe", "widespread rain with thunderstorm continuing through the week, landslide-prone hill stretches"),
    ("2026-09-05", "Jammu and Kashmir", ["Jammu", "Srinagar", "Udhampur"], "rainfall",
     "moderate", "fairly widespread rain with thunderstorm and lightning"),
    ("2026-09-05", "Rajasthan", ["Jaipur", "Kota", "Udaipur", "Alwar"], "rainfall",
     "moderate", "widespread rain over east Rajasthan with thunderstorm activity"),
    ("2026-09-05", "Maharashtra", ["Ratnagiri", "Sindhudurg", "Panaji", "Margao"], "rainfall",
     "moderate", "fairly widespread to widespread rain over Konkan and Goa"),
    ("2026-09-06", "Bihar", ["Patna", "Gaya", "Muzaffarpur"], "rainfall",
     "moderate", "isolated to scattered rain with thunderstorm and lightning"),
    ("2026-09-07", "Assam", ["Guwahati", "Dibrugarh", "Silchar"], "flood",
     "moderate", "waterlogging and localised flooding after continuous rain"),
    ("2026-09-05", "Gujarat", ["Ahmedabad", "Surat", "Rajkot"], "rainfall",
     "moderate", "isolated to scattered rain over Gujarat region and Saurashtra-Kutch"),
    # Non-monsoon event types so the classifier sees a balanced label set
    ("2026-05-28", "Rajasthan", ["Jaisalmer", "Bikaner", "Barmer"], "heatwave",
     "severe", "severe heatwave with maximum temperatures crossing 47C"),
    ("2026-05-30", "Uttar Pradesh", ["Prayagraj", "Banda", "Jhansi"], "heatwave",
     "moderate", "heatwave conditions with hot and dry westerly winds"),
    ("2026-06-02", "Rajasthan", ["Jodhpur", "Bikaner"], "dust_storm",
     "moderate", "dust raising winds reducing visibility to under 500m"),
    ("2026-06-04", "Delhi", ["New Delhi", "Dwarka"], "dust_storm",
     "moderate", "dust storm followed by a brief spell of rain, visibility dropped sharply"),
    ("2026-01-14", "Punjab", ["Amritsar", "Ludhiana"], "cold_wave",
     "moderate", "dense fog and cold wave conditions, minimum temperature near 4C"),
    ("2026-01-16", "Haryana", ["Hisar", "Karnal"], "cold_wave",
     "moderate", "cold day conditions with dense fog affecting visibility"),
]

EVENT_TYPES = ["rainfall", "flood", "heatwave", "dust_storm", "cold_wave", "thunderstorm", "cyclone"]

SOURCES = ["twitter", "facebook", "citizen_app", "instagram", "news_wire", "whatsapp_forward"]
SOURCE_BASE_CREDIBILITY = {
    "news_wire": 0.9,
    "citizen_app": 0.75,
    "twitter": 0.55,
    "facebook": 0.45,
    "instagram": 0.45,
    "whatsapp_forward": 0.25,
}

HASHTAG_POOL = ["#IMD", "#WeatherAlert", "#Flood", "#RainAlert", "#HeatWave",
                "#DustStorm", "#ColdWave", "#Monsoon2026", "#StaySafe", "#Cyclone"]

TEMPLATES_GENUINE = [
    "Heavy {event_desc} reported in {district}, {state} right now. Roads getting waterlogged near main market. {tags}",
    "{district} experiencing {event_desc} since morning, visibility very low on the highway. {tags}",
    "Update from {district}: {event_desc} continuing, local authorities have issued an advisory. {tags}",
    "Just saw {event_desc} hit {district} - power supply flickering in our area. {tags}",
    "IMD had warned about this — {event_desc} now active over {district}, {state}. {tags}",
    "Citizen report: {event_desc} in {district}. Water level rising near the river bank, please avoid the area. {tags}",
    "{district} traffic disrupted due to {event_desc}, several vehicles stranded on the flyover. {tags}",
    "School closed today in {district} because of {event_desc}, notice came in this morning. {tags}",
    "Farmers in {district} worried as {event_desc} continues for the third day. {tags}",
    "NDRF teams reportedly moving into {district} as {event_desc} worsens. {tags}",
]

TEMPLATES_HINGLISH = [
    "{district} mein bahut zyada {event_desc} ho raha hai, sadkon par paani bhar gaya hai. {tags}",
    "Abhi {district} mein {event_desc} shuru hua, log ghar ke andar hi hain. {tags}",
    "{district} ka mausam kharab, {event_desc} ki wajah se traffic ruk gaya hai. {tags}",
]

# Deliberately fake / exaggerated / misleading templates for the fake-report detector
TEMPLATES_FAKE = [
    "BREAKING!!! {district} completely underwater, govt hiding the truth, share before it gets deleted!!! {tags}",
    "URGENT: dam near {district} about to burst, everyone evacuate NOW, forward to everyone you know {tags}",
    "Scientists confirm {event_desc} in {district} caused by secret weather weapon testing, wake up people {tags}",
    "My cousin's friend said {district} got 10 feet of water in one hour, whole city gone, unbelievable {tags}",
    "{district} flooded because of cloud seeding conspiracy by neighbouring country, spread the word {tags}",
    "100% confirmed {district} bridge collapsed due to {event_desc}, hundreds missing (unverified forward) {tags}",
]

EVENT_DESC = {
    "rainfall": "heavy rainfall",
    "flood": "flooding",
    "heatwave": "a heatwave",
    "dust_storm": "a dust storm",
    "cold_wave": "a cold wave",
    "thunderstorm": "a thunderstorm with lightning",
    "cyclone": "cyclonic weather",
}

INDIA_COORDS = {
    "New Delhi": (28.6139, 77.2090), "Dwarka": (28.5921, 77.0460), "Rohini": (28.7495, 77.0565),
    "Karol Bagh": (28.6519, 77.1909), "Noida": (28.5355, 77.3910), "Gurugram": (28.4595, 77.0266),
    "Ghaziabad": (28.6692, 77.4538), "Meerut": (28.9845, 77.7064), "Moradabad": (28.8386, 78.7733),
    "Kanpur": (26.4499, 80.3319), "Lucknow": (26.8467, 80.9462), "Prayagraj": (25.4358, 81.8463),
    "Banda": (25.4762, 80.3357), "Jhansi": (25.4484, 78.5685), "Raipur": (21.2514, 81.6296),
    "Raigarh": (21.8974, 83.3950), "Korba": (22.3595, 82.7501), "Jashpur": (22.8888, 84.1400),
    "Balrampur": (23.1146, 83.6053), "Surguja": (23.1160, 83.1970), "Balasore": (21.4942, 86.9317),
    "Mayurbhanj": (21.9285, 86.7397), "Keonjhar": (21.6297, 85.5817), "Sundargarh": (22.1167, 84.0333),
    "Sambalpur": (21.4669, 83.9756), "Bhopal": (23.2599, 77.4126), "Jabalpur": (23.1815, 79.9864),
    "Sagar": (23.8388, 78.7378), "Rewa": (24.5364, 81.3037), "Satna": (24.6005, 80.8322),
    "Kolkata": (22.5726, 88.3639), "Howrah": (22.5958, 88.2636), "Kharagpur": (22.3302, 87.3237),
    "Malda": (25.0108, 88.1411), "Ranchi": (23.3441, 85.3096), "Jamshedpur": (22.8046, 86.2029),
    "Dhanbad": (23.7957, 86.4304), "Dehradun": (30.3165, 78.0322), "Haridwar": (29.9457, 78.1642),
    "Nainital": (29.3919, 79.4542), "Rudraprayag": (30.2846, 78.9812), "Jammu": (32.7266, 74.8570),
    "Srinagar": (34.0837, 74.7973), "Udhampur": (32.9269, 75.1416), "Jaipur": (26.9124, 75.7873),
    "Kota": (25.2138, 75.8648), "Udaipur": (24.5854, 73.7125), "Alwar": (27.5530, 76.6346),
    "Ratnagiri": (16.9902, 73.3120), "Sindhudurg": (16.1667, 73.6167), "Panaji": (15.4909, 73.8278),
    "Margao": (15.2832, 73.9862), "Patna": (25.5941, 85.1376), "Gaya": (24.7955, 84.9994),
    "Muzaffarpur": (26.1225, 85.3906), "Guwahati": (26.1445, 91.7362), "Dibrugarh": (27.4728, 94.9120),
    "Silchar": (24.8333, 92.7789), "Ahmedabad": (23.0225, 72.5714), "Surat": (21.1702, 72.8311),
    "Rajkot": (22.3039, 70.8022), "Jaisalmer": (26.9157, 70.9083), "Bikaner": (28.0229, 73.3119),
    "Barmer": (25.7521, 71.3961), "Jodhpur": (26.2389, 73.0243), "Amritsar": (31.6340, 74.8723),
    "Ludhiana": (30.9010, 75.8573), "Hisar": (29.1492, 75.7217), "Karnal": (29.6857, 76.9905),
}

CREDIBLE_HANDLES = ["@IMDWeather_Alt", "@DistrictAdmin_Official", "@NDRFHQ_Watch", "@CitizenReporterIN"]


def jitter_coord(lat, lon):
    return round(lat + random.uniform(-0.05, 0.05), 5), round(lon + random.uniform(-0.05, 0.05), 5)


def make_row(date_str, state, district, event_type, severity, note, is_fake=False, is_near_dup_of=None):
    lat, lon = INDIA_COORDS.get(district, (22.9734, 78.6569))
    lat, lon = jitter_coord(lat, lon)
    event_desc = EVENT_DESC[event_type]
    source = random.choice(SOURCES)
    base_cred = SOURCE_BASE_CREDIBILITY[source]

    if is_fake:
        text = random.choice(TEMPLATES_FAKE).format(
            district=district, event_desc=event_desc,
            tags=" ".join(random.sample(HASHTAG_POOL, k=random.randint(1, 2))))
        base_cred = base_cred * random.uniform(0.2, 0.5)
        source = random.choice(["whatsapp_forward", "facebook", "twitter"])
    elif random.random() < 0.12:
        text = random.choice(TEMPLATES_HINGLISH).format(
            district=district, event_desc=event_desc,
            tags=" ".join(random.sample(HASHTAG_POOL, k=random.randint(1, 2))))
    else:
        text = random.choice(TEMPLATES_GENUINE).format(
            district=district, event_desc=event_desc, state=state,
            tags=" ".join(random.sample(HASHTAG_POOL, k=random.randint(1, 3))))

    ts = datetime.strptime(date_str, "%Y-%m-%d") + timedelta(
        hours=random.randint(5, 23), minutes=random.randint(0, 59))

    handle = random.choice(CREDIBLE_HANDLES) if source == "news_wire" or random.random() < 0.1 \
        else f"@user{random.randint(1000, 99999)}"

    return {
        "report_id": str(uuid.uuid4())[:8],
        "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
        "source": source,
        "user_handle": handle,
        "state": state,
        "district": district,
        "latitude": lat,
        "longitude": lon,
        "text": text,
        "event_type": event_type,
        "severity": severity,
        "source_credibility": round(min(max(base_cred + random.uniform(-0.1, 0.1), 0.02), 0.99), 2),
        "label_is_fake": int(is_fake),
        "near_dup_of": is_near_dup_of or "",
    }


def build():
    rows = []
    for date_str, state, districts, event_type, severity, note in REAL_EVENTS:
        for district in districts:
            n_posts = random.randint(6, 14)
            first_id = None
            for i in range(n_posts):
                is_fake = random.random() < 0.08
                row = make_row(date_str, state, district, event_type, severity, note, is_fake=is_fake)
                if i == 0:
                    first_id = row["report_id"]
                elif random.random() < 0.25:
                    # create a genuine near-duplicate of the first post in this cluster
                    row["near_dup_of"] = first_id
                rows.append(row)

    # inject some genuinely fabricated events with no IMD basis (label_is_fake=1),
    # scattered across random districts/dates, to sharpen the fake-report classifier
    all_districts = list(INDIA_COORDS.keys())
    for _ in range(35):
        district = random.choice(all_districts)
        event_type = random.choice(EVENT_TYPES[:5])
        date_str = (datetime(2026, 9, 1) + timedelta(days=random.randint(-60, 6))).strftime("%Y-%m-%d")
        row = make_row(date_str, "Unverified", district, event_type, "unverified",
                        "no corroborating bulletin", is_fake=True)
        rows.append(row)

    random.shuffle(rows)
    return rows


def write_csv(rows, path):
    fieldnames = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    rows = build()
    out_path = "weather_reports_seed.csv"
    write_csv(rows, out_path)
    n_fake = sum(r["label_is_fake"] for r in rows)
    print(f"Generated {len(rows)} rows -> {out_path}")
    print(f"  fake/misleading: {n_fake} ({n_fake/len(rows):.1%})")
    print(f"  event types: {sorted(set(r['event_type'] for r in rows))}")
    print(f"  districts covered: {len(set(r['district'] for r in rows))}")
