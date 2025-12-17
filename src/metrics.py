\
from __future__ import annotations

import pandas as pd

def compute_hourly_events(events_clean: pd.DataFrame) -> pd.DataFrame:
    """
    Compute hourly event counts in UTC.

    TODO: Candidate should ensure event_time_utc exists and is datetime; handle empty data gracefully.
    """
    if events_clean.empty:
        return pd.DataFrame(columns=["ingest_date", "hour_utc", "event_count"])
    df = events_clean.copy()
    df["hour_utc"] = df["event_time_utc"].dt.floor("H")
    out = df.groupby(["ingest_date", "hour_utc"], as_index=False).size()
    out = out.rename(columns={"size": "event_count"})
    return out

def compute_daily_metrics(ingest_date: str,
                          customers_total: int, customers_clean: int, customers_quarantine: int,
                          events_total: int, events_clean: int, events_quarantine: int,
                          orders_total: int, orders_clean: int, orders_quarantine: int,
                          events_clean_df: pd.DataFrame,
                          orders_clean_df: pd.DataFrame,
                          validation_report: dict) -> pd.DataFrame:
    """
    Produce a single-row daily metrics dataframe.

    TODO: Candidate: add duplicate rates, null rates, p50/p95, breakdowns, orphan rates, etc.
    """
    row = {
        "ingest_date": ingest_date,
        "customers_total": customers_total,
        "customers_clean": customers_clean,
        "customers_quarantine": customers_quarantine,
        "events_total": events_total,
        "events_clean": events_clean,
        "events_quarantine": events_quarantine,
        "orders_total": orders_total,
        "orders_clean": orders_clean,
        "orders_quarantine": orders_quarantine,
        "active_customers_events": int(events_clean_df["customer_id"].nunique()) if not events_clean_df.empty else 0,
        "active_customers_orders": int(orders_clean_df["customer_id"].nunique()) if not orders_clean_df.empty else 0,
        "quarantine_rate_events": (events_quarantine / events_total) if events_total else 0.0,
        "quarantine_rate_orders": (orders_quarantine / orders_total) if orders_total else 0.0,
        "quarantine_rate_customers": (customers_quarantine / customers_total) if customers_total else 0.0,
    }
    return pd.DataFrame([row])
