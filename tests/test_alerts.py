import pandas as pd
import sys, os
sys.path.append(os.path.abspath("."))

from src.alerts import detect_partial_load


def test_alert_missing_hour_coverage_flags_when_hours_missing_or_zero():
    """
    Scenario:
      - Today should have 24 hourly rows.
      - We only have 2 hourly rows -> 22 hours are missing.
      - That exceeds missing_hours_threshold (default 4).
    Expectation:
      - detect_partial_load adds a "missing_hour_coverage" flag.
    """
    ingest_date = "2025-12-10"

    hourly = pd.DataFrame({
        "ingest_date": [ingest_date, ingest_date],
        "hour_utc": [
            pd.Timestamp("2025-12-10T00:00:00Z"),
            pd.Timestamp("2025-12-10T01:00:00Z"),
        ],
        "event_count": [10, 10],
    })

    alerts = detect_partial_load(
        ingest_date=ingest_date,
        hourly_events=hourly,
        daily_metrics_history=None, 
        expected_hours=24,
        missing_hours_threshold=4,
    )

    assert any(f.get("type") == "missing_hour_coverage" for f in alerts["flags"])


def test_alert_volume_drop_vs_trailing_median_flags_on_day2():
    """
    Scenario:
      - `hist` includes today's row + prior day rows.
      - Prior day events_clean is 1000, today is 100 (90% drop).
      - With volume_drop_pct=0.50, we should flag.
    Expectation:
      - detect_partial_load adds a "volume_drop_vs_trailing_median" flag.
    """
    ingest_date = "2025-12-10"

    hourly = pd.DataFrame({
        "ingest_date": [ingest_date],
        "hour_utc": [pd.Timestamp("2025-12-10T00:00:00Z")],
        "event_count": [10],
    })

    hist = pd.DataFrame({
        "ingest_date": ["2025-12-09", ingest_date],   # prior + today
        "events_clean": [1000, 100],                  # huge drop
    })

    alerts = detect_partial_load(
        ingest_date=ingest_date,
        hourly_events=hourly,
        daily_metrics_history=hist,
        trailing_days=7,
        volume_drop_pct=0.50,
        expected_hours=24,
    )

    assert any(f.get("type") == "volume_drop_vs_trailing_median" for f in alerts["flags"])



def test_no_alerts_when_data_is_complete_and_volume_is_stable():
    """
    Scenario:
      - All 24 hours are present (no missing hour coverage).
      - Today's volume is similar to trailing history.
    Expectation:
      - detect_partial_load returns an empty flags list.
    """
    ingest_date = "2025-12-10"

    # 24 hours, all with events
    hourly = pd.DataFrame({
        "ingest_date": [ingest_date] * 24,
        "hour_utc": [
            pd.Timestamp(f"2025-12-10T{h:02d}:00:00Z") for h in range(24)
        ],
        "event_count": [100] * 24,
    })

    # History includes prior day + today
    hist = pd.DataFrame({
        "ingest_date": ["2025-12-09", ingest_date],
        "events_clean": [2300, 2400],
    })

    alerts = detect_partial_load(
        ingest_date=ingest_date,
        hourly_events=hourly,
        daily_metrics_history=hist,
        expected_hours=24,
        missing_hours_threshold=4,
        volume_drop_pct=0.50,
    )

    assert alerts["flags"] == []


