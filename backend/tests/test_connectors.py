"""
test_connectors.py
--------------------
Proves the live-connector parsing logic is correct WITHOUT requiring
network access — this build's sandbox cannot reach external domains
(egress is locked to package registries), so these tests use the
`responses` library to mock realistic HTTP payloads and assert the
connectors parse them correctly. Run these yourself with
`python3 -m pytest backend/tests/test_connectors.py -v` on a machine
with normal internet to have full confidence, then delete the mocks and
hit the real endpoints once (see README "Live connector setup").

This is what separates "the code claims to call a live API" from
"the code has been shown to correctly parse what that API returns."
"""

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import responses
from connectors import open_meteo_connector, imd_scraper_connector


@responses.activate
def test_open_meteo_parses_heavy_rain_as_flood():
    """Mocks a realistic Open-Meteo JSON payload (real schema, from their
    published API docs) and checks our classifier correctly reads it."""
    fake_payload = {
        "current": {
            "time": "2026-09-08T10:00",
            "temperature_2m": 27.5,
            "precipitation": 24.0,   # heavy -> should classify as flood
            "weather_code": 65,      # heavy rain
            "wind_speed_10m": 18.2,
        }
    }
    responses.add(
        responses.GET,
        open_meteo_connector.OPEN_METEO_URL,
        json=fake_payload,
        status=200,
    )
    result = open_meteo_connector.fetch_live_conditions(
        lat=21.25, lon=81.63, place_name="Raipur", state="Chhattisgarh")

    assert result["event_type"] == "flood"
    assert result["district"] == "Raipur"
    assert result["source"] == "public_api_open_meteo"
    assert "27.5" in result["text"]
    print("PASS: open_meteo_connector correctly classified 24mm precipitation as flood")


@responses.activate
def test_open_meteo_parses_heatwave():
    fake_payload = {
        "current": {
            "time": "2026-05-28T14:00",
            "temperature_2m": 47.2,
            "precipitation": 0.0,
            "weather_code": 0,
            "wind_speed_10m": 12.0,
        }
    }
    responses.add(
        responses.GET,
        open_meteo_connector.OPEN_METEO_URL,
        json=fake_payload,
        status=200,
    )
    result = open_meteo_connector.fetch_live_conditions(
        lat=26.9, lon=70.9, place_name="Jaisalmer", state="Rajasthan")
    assert result["event_type"] == "heatwave"
    print("PASS: open_meteo_connector correctly classified 47.2C as heatwave")


def test_open_meteo_handles_network_failure_gracefully():
    """No mock registered -> requests will fail to connect; fetch_many
    must not raise, it must skip the failed location and return []."""
    results = open_meteo_connector.fetch_many(
        [("Nowhere", "Nowhere State", 0.0, 0.0)], timeout=1)
    assert results == []
    print("PASS: open_meteo_connector.fetch_many degrades gracefully on network failure")


def test_imd_scraper_parses_sample_warning_table():
    """Realistic sample HTML modeled on IMD's public district-warning page
    structure (table rows with district name + free-text warning)."""
    sample_html = """
    <html><body>
    <table>
      <tr><td>Raipur</td><td>Isolated extremely heavy rainfall likely, flash flood risk</td></tr>
      <tr><td>Jaisalmer</td><td>Severe heat wave conditions expected, maximum temp 47C</td></tr>
      <tr><td>Amritsar</td><td>Dense fog and cold wave conditions likely tonight</td></tr>
      <tr><td>NoWarningHere</td><td>Skies clear, pleasant weather expected</td></tr>
    </table>
    </body></html>
    """
    bulletins = imd_scraper_connector.parse_bulletin_html(sample_html)
    event_types = {b["event_type"] for b in bulletins}

    assert "flood" in event_types
    assert "heatwave" in event_types
    assert "cold_wave" in event_types
    # the clear-skies row should NOT be classified as any weather event
    assert not any("NoWarningHere" in b["raw_text"] for b in bulletins if b["event_type"])
    print(f"PASS: imd_scraper_connector correctly parsed {len(bulletins)} real warning rows "
          f"from sample markup, ignored the non-warning row")


def test_imd_scraper_handles_malformed_html_gracefully():
    bulletins = imd_scraper_connector.parse_bulletin_html("<html><body>not a table</body></html>")
    assert bulletins == []
    print("PASS: imd_scraper_connector returns [] instead of raising on markup with no warnings")


if __name__ == "__main__":
    test_open_meteo_parses_heavy_rain_as_flood()
    test_open_meteo_parses_heatwave()
    test_open_meteo_handles_network_failure_gracefully()
    test_imd_scraper_parses_sample_warning_table()
    test_imd_scraper_handles_malformed_html_gracefully()
    print("\nAll connector unit tests passed.")
