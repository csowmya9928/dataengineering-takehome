\
from __future__ import annotations

import pandas as pd
from typing import Optional,List

def compute_hourly_events(events_clean: pd.DataFrame) -> pd.DataFrame:
    """
    Compute hourly event counts in UTC.

    TODO: Candidate should ensure event_time_utc exists and is datetime; handle empty data gracefully.
    """
    if events_clean.empty:
        return pd.DataFrame(columns=["ingest_date", "hour_utc", "event_count"])
    df = events_clean.copy()
    df["hour_utc"] = df["event_time_utc"].dt.floor("h")
    out = df.groupby(["ingest_date", "hour_utc"], as_index=False).size()
    out = out.rename(columns={"size": "event_count"})
    return out

def compute_daily_metrics(
    ingest_date: str,
    customers_clean: pd.DataFrame,
    customers_quarantine: pd.DataFrame,
    events_clean: pd.DataFrame,
    events_quarantine: pd.DataFrame,
    orders_clean: pd.DataFrame,
    orders_quarantine: pd.DataFrame,
    customers_cleaned_raw: Optional[pd.DataFrame] = None,
    events_cleaned_raw: Optional[pd.DataFrame] = None,
    orders_cleaned_raw: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Returns ONE-row dataframe of metrics for a given ingest_date.

    """
    def safe_rate(num: int, den: int) -> float:
        return float(num) / float(den) if den else 0.0

    def null_rate(df: pd.DataFrame, col: str) -> float:
        if df is None or df.empty or col not in df.columns:
            return 0.0
        return float(df[col].isna().mean())

    def p50_p95(df: pd.DataFrame, col: str) -> tuple[float, float]:
        if df is None or df.empty or col not in df.columns:
            return (0.0, 0.0)
        s = pd.to_numeric(df[col], errors="coerce").dropna()
        if s.empty:
            return (0.0, 0.0)
        return (float(s.quantile(0.50)), float(s.quantile(0.95)))

    def id_duplicate_rate(df: pd.DataFrame, id_col: str) -> float:
        """
        % of rows whose id appears more than once.
        """
        if df is None or df.empty or id_col not in df.columns:
            return 0.0
        dup_mask = df[id_col].duplicated(keep=False)
        return float(dup_mask.mean())

    def full_row_duplicate_rate(df: pd.DataFrame, subset_cols: Optional[List[str]] = None) -> float:
        """
        % of rows that are duplicates (based on subset or full row).
        """
        if df is None or df.empty:
            return 0.0
        if subset_cols:
            subset_cols = [c for c in subset_cols if c in df.columns]
            if not subset_cols:
                return 0.0
            dup_mask = df.duplicated(subset=subset_cols, keep=False)
        else:
            dup_mask = df.duplicated(keep=False)
        return float(dup_mask.mean())

    def orphan_rate(df: pd.DataFrame, customers_df: pd.DataFrame, customer_id_col: str = "customer_id") -> float:
        """
        % of rows where customer_id is not present in customers_clean
        """
        if df is None or df.empty or customer_id_col not in df.columns:
            return 0.0
        if customers_df is None or customers_df.empty or customer_id_col not in customers_df.columns:
            # if customers is empty, treat everything as orphan
            return 1.0
        valid = set(customers_df[customer_id_col].dropna().astype(str))
        cid = df[customer_id_col].astype(str)
        return float((~cid.isin(valid)).mean())

    def breakdown_counts(df: pd.DataFrame, col: str) -> dict:
        if df is None or df.empty or col not in df.columns:
            return {}
        return df[col].astype("string").fillna("NULL").value_counts().to_dict()

    # ---------- counts ----------
    customers_total = len(customers_clean) + len(customers_quarantine)
    events_total = len(events_clean) + len(events_quarantine)
    orders_total = len(orders_clean) + len(orders_quarantine)

    # ---------- active customers ----------
    active_customers_events = int(events_clean["customer_id"].nunique()) if (not events_clean.empty and "customer_id" in events_clean.columns) else 0
    active_customers_orders = int(orders_clean["customer_id"].nunique()) if (not orders_clean.empty and "customer_id" in orders_clean.columns) else 0

    # ---------- quarantine rates ----------
    quarantine_rate_customers = safe_rate(len(customers_quarantine), customers_total)
    quarantine_rate_events = safe_rate(len(events_quarantine), events_total)
    quarantine_rate_orders = safe_rate(len(orders_quarantine), orders_total)

    # ---------- quality: invalid timestamp rates ----------
    # we approximate using (clean + quarantine) as clean_raw
    events_for_ts = events_cleaned_raw if events_cleaned_raw is not None else pd.concat([events_clean, events_quarantine], ignore_index=True)
    orders_for_ts = orders_cleaned_raw if orders_cleaned_raw is not None else pd.concat([orders_clean, orders_quarantine], ignore_index=True)

    invalid_event_ts_rate = null_rate(events_for_ts, "event_time_utc")   # invalid parse typically becomes NaT
    invalid_order_ts_rate = null_rate(orders_for_ts, "order_time_utc")

    # ---------- quality: orphan customer_id rates ----------
    orphan_rate_events = orphan_rate(events_clean, customers_clean, "customer_id")
    orphan_rate_orders = orphan_rate(orders_clean, customers_clean, "customer_id")

    # ---------- quality: duplicate rates ----------
    # Use cleaned_raw if you want duplicates BEFORE validation split
    events_for_dups = events_cleaned_raw if events_cleaned_raw is not None else pd.concat([events_clean, events_quarantine], ignore_index=True)
    orders_for_dups = orders_cleaned_raw if orders_cleaned_raw is not None else pd.concat([orders_clean, orders_quarantine], ignore_index=True)

    events_dup_id_rate = id_duplicate_rate(events_for_dups, "event_id")
    orders_dup_id_rate = id_duplicate_rate(orders_for_dups, "order_id")

    # full-row duplicates (you can tune subset_cols)
    events_dup_fullrow_rate = full_row_duplicate_rate(events_for_dups)
    orders_dup_fullrow_rate = full_row_duplicate_rate(orders_for_dups)

    # ---------- quality: null rates for key columns ----------
    customers_email_null_rate = null_rate(customers_clean, "email")
    customers_country_null_rate = null_rate(customers_clean, "country")
    orders_amount_null_rate = null_rate(orders_clean, "amount")
    orders_currency_null_rate = null_rate(orders_clean, "currency")

    # ---------- numeric sanity (p50/p95) ----------
    duration_p50, duration_p95 = p50_p95(events_clean, "duration_ms")
    amount_p50, amount_p95 = p50_p95(orders_clean, "amount")

    # ---------- breakdowns ----------
    events_by_event_type = breakdown_counts(events_clean, "event_type")
    events_by_platform = breakdown_counts(events_clean, "platform")
    orders_by_status = breakdown_counts(orders_clean, "status")

    row = {
        "ingest_date": ingest_date,

        # volume
        "customers_total": int(customers_total),
        "customers_clean": int(len(customers_clean)),
        "customers_quarantine": int(len(customers_quarantine)),
        "events_total": int(events_total),
        "events_clean": int(len(events_clean)),
        "events_quarantine": int(len(events_quarantine)),
        "orders_total": int(orders_total),
        "orders_clean": int(len(orders_clean)),
        "orders_quarantine": int(len(orders_quarantine)),

        # distinct active customers
        "active_customers_events": active_customers_events,
        "active_customers_orders": active_customers_orders,

        # rates
        "quarantine_rate_customers": float(quarantine_rate_customers),
        "quarantine_rate_events": float(quarantine_rate_events),
        "quarantine_rate_orders": float(quarantine_rate_orders),

        # quality
        "invalid_event_timestamp_rate": float(invalid_event_ts_rate),
        "invalid_order_timestamp_rate": float(invalid_order_ts_rate),
        "orphan_customer_rate_events": float(orphan_rate_events),
        "orphan_customer_rate_orders": float(orphan_rate_orders),
        "duplicate_id_rate_events": float(events_dup_id_rate),
        "duplicate_id_rate_orders": float(orders_dup_id_rate),
        "duplicate_fullrow_rate_events": float(events_dup_fullrow_rate),
        "duplicate_fullrow_rate_orders": float(orders_dup_fullrow_rate),

        "null_rate_customers_email": float(customers_email_null_rate),
        "null_rate_customers_country": float(customers_country_null_rate),
        "null_rate_orders_amount": float(orders_amount_null_rate),
        "null_rate_orders_currency": float(orders_currency_null_rate),

        # numeric sanity
        "duration_ms_p50": float(duration_p50),
        "duration_ms_p95": float(duration_p95),
        "amount_p50": float(amount_p50),
        "amount_p95": float(amount_p95),

        # breakdowns (stored as dict in parquet)
        "events_by_event_type": events_by_event_type,
        "events_by_platform": events_by_platform,
        "orders_by_status": orders_by_status,
    }

    return pd.DataFrame([row])
