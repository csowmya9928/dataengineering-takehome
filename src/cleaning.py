\
from __future__ import annotations

import pandas as pd
from .utils import (
    is_valid_email,
    normalize_currency,
    normalize_platform,
    normalize_customer_status,
    normalize_order_status,
    parse_timestamp_to_utc,
    safe_float,
)

def clean_customers(df: pd.DataFrame, ingest_date: str) -> pd.DataFrame:
    df = df.copy()
    df["customer_id"] = df["customer_id"].astype(str).str.strip()
    df["email"] = df["email"].astype(str).str.strip()
    df["email_valid"] = df["email"].apply(is_valid_email)

    df["created_at_utc"] = df["created_at"].apply(parse_timestamp_to_utc)
    df["country"] = df["country"].astype(str).str.strip().str.upper().replace({"NAN": None})
    df["status"] = df["status"].apply(normalize_customer_status)
    df["ingest_date"] = ingest_date

    # TODO: Candidate: implement dedupe strategy (e.g., latest created_at_utc, then prefer valid email)
    df = df.sort_values(["customer_id", "created_at_utc"], ascending=[True, True])
    df = df.drop_duplicates(subset=["customer_id"], keep="last")
    return df

def clean_events(df: pd.DataFrame, ingest_date: str) -> pd.DataFrame:
    df = df.copy()
    df["event_id"] = df["event_id"].astype(str).str.strip()
    df["customer_id"] = df["customer_id"].astype(str).str.strip()
    df["event_time_utc"] = df["event_time"].apply(parse_timestamp_to_utc)
    df["event_type"] = df["event_type"].astype(str).str.strip().str.lower()
    df["platform"] = df["platform"].apply(normalize_platform)
    df["session_id"] = df["session_id"].astype(str).str.strip()
    df["duration_ms"] = df["duration_ms"].apply(safe_float)
    df["ingest_date"] = ingest_date

    # TODO: Candidate: define & implement dedupe policy (event_id duplicates vs full row duplicates)
    return df

def clean_orders(df: pd.DataFrame, ingest_date: str) -> pd.DataFrame:
    df = df.copy()
    df["order_id"] = df["order_id"].astype(str).str.strip()
    df["customer_id"] = df["customer_id"].astype(str).str.strip()
    df["order_time_utc"] = df["order_time"].apply(parse_timestamp_to_utc)
    df["amount"] = df["amount"].apply(safe_float)
    df["currency"] = df["currency"].apply(normalize_currency)
    df["status"] = df["status"].apply(normalize_order_status)
    df["ingest_date"] = ingest_date

    # TODO: Candidate: define & implement dedupe policy (order_id duplicates vs full row duplicates)
    return df
