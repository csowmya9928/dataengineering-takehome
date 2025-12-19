\
from __future__ import annotations


import pandas as pd
from .utils import (
    is_valid_email,
    normalize_currency,
    normalize_platform,
    normalize_customer_status,
    normalize_customer_id,
    normalize_order_status,
    normalize_country,
    parse_timestamp_to_utc,
    safe_float,
)

def clean_customers(df: pd.DataFrame, ingest_date: str) -> pd.DataFrame:
    df = df.copy()
    df["customer_id"] = df["customer_id"].apply(normalize_customer_id)
    df["email"] = df["email"].astype(str).str.strip()
    df["email_valid"] = df["email"].apply(is_valid_email)

    df["created_at_utc"] = df["created_at"].apply(parse_timestamp_to_utc)
    df["country"] = df["country"].apply(normalize_country)
    df["status"] = df["status"].apply(normalize_customer_status)
    df["ingest_date"] = ingest_date

    # TODO: Candidate: implement dedupe strategy (e.g., latest created_at_utc, then prefer valid email)
    # df = df.sort_values(["customer_id", "created_at_utc"], ascending=[True, True])
    # df = df.drop_duplicates(subset=["customer_id"], keep="last")
    df["has_created_at"] = df["created_at_utc"].notna()

    df = df.sort_values(
    ["customer_id", "has_created_at", "created_at_utc", "email_valid"],
    ascending=[True, True, True, True]
    )
    df = df.drop_duplicates(subset=["customer_id"], keep="last")
    df = df.drop(columns=["has_created_at"])
    return df

def clean_events(df: pd.DataFrame, ingest_date: str) -> pd.DataFrame:
    df = df.copy()
    df["event_id"] = df["event_id"].astype(str).str.strip()
    df["customer_id"] = df["customer_id"].apply(normalize_customer_id)

    df["event_time_utc"] = df["event_time"].apply(parse_timestamp_to_utc)
    df["event_type"] = df["event_type"].astype(str).str.strip().str.lower()
    df["platform"] = df["platform"].apply(normalize_platform)
    df["session_id"] = df["session_id"].astype(str).str.strip()
    df["duration_ms"] = df["duration_ms"].apply(safe_float)

    df["ingest_date"] = ingest_date

    # TODO: Candidate: define & implement dedupe policy (event_id duplicates vs full row duplicates)
    # Full-row duplicates:

    df = df.drop_duplicates(keep="last")

    # event_id dedupe policy:
    # Prefer (in order):
    #   event_time_utc present
    #   customer_id present
    #   event_type present
    #   platform present
    #   session_id present
    #   duration_ms present
    #   latest event_time_utc
    #   otherwise keep last occurrence

    # treat empty/whitespace and string nulls as missing for text columns
    def has_real_value(series: pd.Series) -> pd.Series:
        s = series.astype("string").str.strip()
        return s.notna() & s.ne("") & ~s.str.lower().isin(["nan", "none", "null"])

    #I create helper flags to rank records by quality.
    df["has_event_time"] = df["event_time_utc"].notna().astype(int)
    df["has_customer"] = has_real_value(df["customer_id"]).astype(int)
    df["has_event_type"] = has_real_value(df["event_type"]).astype(int)
    df["has_platform"] = df["platform"].notna().astype(int)  # normalize_platform returns None if unknown
    df["has_session"] = has_real_value(df["session_id"]).astype(int)
    df["has_duration"] = df["duration_ms"].notna().astype(int)

    # Sort so the best record becomes LAST per event_id; then keep="last"
    df = df.sort_values(
        by=[
            "event_id",
            "has_event_time",
            "has_customer",
            "has_event_type",
            "has_platform",
            "has_session",
            "has_duration",
            "event_time_utc" 
                  
        ],
        ascending=[True, True, True, True, True, True, True, True],
    )

    df = df.drop_duplicates(subset=["event_id"], keep="last")

    # cleanup helper columns
    df = df.drop(
        columns=[
            "has_event_time",
            "has_customer",
            "has_event_type",
            "has_platform",
            "has_session",
            "has_duration",
        ]
    )

    return df

def clean_orders(df: pd.DataFrame, ingest_date: str) -> pd.DataFrame:
    df = df.copy()
    df["order_id"] = df["order_id"].astype(str).str.strip()
    df["customer_id"] = df["customer_id"].apply(normalize_customer_id)

    df["order_time_utc"] = df["order_time"].apply(parse_timestamp_to_utc)
    df["amount"] = df["amount"].apply(safe_float)
    df["currency"] = df["currency"].apply(normalize_currency)
    df["status"] = df["status"].apply(normalize_order_status)

    df["ingest_date"] = ingest_date

    # Full-row duplicates
    # If two rows are identical across ALL columns, keep the last one
    df = df.drop_duplicates(keep="last")

  
    # 3) order_id dedupe policy
    
    # helper: for text columns, treat empty/whitespace/"nan"/"none"/"null" as missing
    def has_real_value(series: pd.Series) -> pd.Series:
        s = series.astype("string").str.strip()
        return s.notna() & s.ne("") & ~s.str.lower().isin(["nan", "none", "null"])

    df["has_order_time"] = df["order_time_utc"].notna().astype(int)
    df["has_customer"]   = has_real_value(df["customer_id"]).astype(int)
    df["has_amount"]     = df["amount"].notna().astype(int)
    df["has_currency"]   = has_real_value(df["currency"]).astype(int)
    df["has_status"]     = has_real_value(df["status"]).astype(int)

    # Sort so the "best" row becomes LAST for each order_id, then keep="last"
    df = df.sort_values(
        by=[
            "order_id",
            "has_order_time",
            "has_customer",
            "has_amount",
            "has_currency",
            "has_status",
            "order_time_utc",
        ],
        ascending=[True, True, True, True, True, True, True],
    )

    df = df.drop_duplicates(subset=["order_id"], keep="last")

    # cleanup helper cols
    df = df.drop(
        columns=["has_order_time", "has_customer", "has_amount", "has_currency", "has_status"]
    )

    return df
