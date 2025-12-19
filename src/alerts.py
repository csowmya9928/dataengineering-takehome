\
from __future__ import annotations

import pandas as pd

def detect_partial_load(
    ingest_date: str,
    hourly_events: pd.DataFrame,
    daily_metrics_history: pd.DataFrame | None,
    *,
    # heuristic thresholds (tune as needed)
    missing_hours_threshold: int = 4,     # flag if >4 hours have 0 events
    trailing_days: int = 7,               # compare against last 7 days
    volume_drop_pct: float = 0.50,        # flag if today drops >50% vs trailing median
    expected_hours: int = 24,             # expect 24 hours/day
) -> dict:
    """
    Return alerts dict for ingest_date using simple heuristics:
      1) Missing hour coverage: too many hours with 0 events
      2) Volume drop: today's volume much lower than trailing N-day median

    IMPORTANT: For the volume-drop heuristic to work on Day 2+, `daily_metrics_history`
    must include today's row already (i.e., read history AFTER upserting today's metrics).
    """

    alerts = {"ingest_date": ingest_date, "flags": []}

    # Missing hour coverage
    # hourly_events expected columns: ingest_date, hour_utc, event_count
    if hourly_events is None or hourly_events.empty:
        alerts["flags"].append({
            "type": "missing_hour_coverage",
            "severity": "high",
            "details": {
                "reason": "hourly_events is empty for ingest_date",
                "missing_hours": expected_hours,
                "expected_hours": expected_hours
            }
        })
    else:
        day_df = hourly_events[hourly_events["ingest_date"] == ingest_date].copy()

        if day_df.empty:
            alerts["flags"].append({
                "type": "missing_hour_coverage",
                "severity": "high",
                "details": {
                    "reason": "no hourly rows found for ingest_date",
                    "missing_hours": expected_hours,
                    "expected_hours": expected_hours
                }
            })
        else:
            # hours with explicit 0 count
            zero_hours = int((day_df["event_count"].fillna(0) == 0).sum())

            # if hourly table doesn't contain all hours, count missing hours too
            present_hours = int(day_df["hour_utc"].nunique())
            missing_hours = int(max(0, expected_hours - present_hours))

            total_bad_hours = zero_hours + missing_hours

            if total_bad_hours > missing_hours_threshold:
                alerts["flags"].append({
                    "type": "missing_hour_coverage",
                    "severity": "medium",
                    "details": {
                        "zero_hours": zero_hours,
                        "missing_hours": missing_hours,
                        "total_bad_hours": total_bad_hours,
                        "threshold": missing_hours_threshold,
                        "expected_hours": expected_hours
                    }
                })

    # Large volume drop vs trailing median
    if daily_metrics_history is None or daily_metrics_history.empty:
        return alerts

    hist = daily_metrics_history.copy()

    # must contain today's row + the metric we compare
    if "ingest_date" not in hist.columns or "events_clean" not in hist.columns:
        return alerts

    hist["ingest_date"] = hist["ingest_date"].astype(str)
    hist = hist.sort_values("ingest_date")

    # today's volume
    today_row = hist[hist["ingest_date"] == ingest_date]
    if today_row.empty:
        # If this happens, you likely called detect_partial_load BEFORE upserting today's metrics.
        return alerts

    today_events_clean = float(today_row.iloc[0]["events_clean"])

    # trailing baseline (prior days only)
    prior = hist[hist["ingest_date"] < ingest_date].tail(trailing_days)
    if prior.empty:
        return alerts

    trailing_median = float(prior["events_clean"].median())

    if trailing_median > 0:
        drop_ratio = (trailing_median - today_events_clean) / trailing_median
        if drop_ratio > volume_drop_pct:
            alerts["flags"].append({
                "type": "volume_drop_vs_trailing_median",
                "severity": "high",
                "details": {
                    "today_events_clean": today_events_clean,
                    "trailing_days_used": int(len(prior)),
                    "trailing_median_events_clean": trailing_median,
                    "drop_ratio": float(drop_ratio),
                    "threshold_drop_pct": volume_drop_pct
                }
            })

    return alerts


