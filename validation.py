\
from __future__ import annotations

import pandas as pd

def split_clean_quarantine_customers(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """
    Returns (clean_df, quarantine_df, stats).

    TODO: Candidate: enforce required columns/types, timestamp validity, status validity, etc.
    Starter checks are minimal; expand as required.
    """
    df = df.copy()
    reasons = []

    # example: invalid created_at
    invalid_created = df["created_at_utc"].isna()
    reason = pd.Series("", index=df.index, dtype="object")
    reason[invalid_created] += "|invalid_created_at"

    # You can add more reasons here (e.g., missing customer_id, invalid status, etc.)
    reason = reason.str.strip("|")
    df["reject_reason"] = reason.replace({"": None})

    quarantine = df[df["reject_reason"].notna()].copy()
    clean = df[df["reject_reason"].isna()].drop(columns=["reject_reason"]).copy()

    stats = {
        "total": int(len(df)),
        "clean": int(len(clean)),
        "quarantine": int(len(quarantine)),
        "by_reason": quarantine["reject_reason"].value_counts(dropna=True).to_dict(),
    }
    return clean, quarantine, stats

def split_clean_quarantine_events(df: pd.DataFrame, customers_clean: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """
    TODO: Candidate: implement referential integrity, duration rules, timestamp validity, platform validity, etc.
    """
    df = df.copy()
    reason = pd.Series("", index=df.index, dtype="object")

    invalid_ts = df["event_time_utc"].isna()
    reason[invalid_ts] += "|invalid_event_time"

    invalid_dur = df["duration_ms"].notna() & (df["duration_ms"] < 0)
    reason[invalid_dur] += "|negative_duration"

    # TODO: Candidate: referential integrity
    # orphans = ~df["customer_id"].isin(set(customers_clean["customer_id"]))
    # reason[orphans] += "|orphan_customer_id"

    reason = reason.str.strip("|")
    df["reject_reason"] = reason.replace({"": None})

    quarantine = df[df["reject_reason"].notna()].copy()
    clean = df[df["reject_reason"].isna()].drop(columns=["reject_reason"]).copy()

    stats = {
        "total": int(len(df)),
        "clean": int(len(clean)),
        "quarantine": int(len(quarantine)),
        "by_reason": quarantine["reject_reason"].value_counts(dropna=True).to_dict(),
    }
    return clean, quarantine, stats

def split_clean_quarantine_orders(df: pd.DataFrame, customers_clean: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """
    TODO: Candidate: implement referential integrity, amount/status rules, currency validity, timestamp validity.
    """
    df = df.copy()
    reason = pd.Series("", index=df.index, dtype="object")

    invalid_ts = df["order_time_utc"].isna()
    reason[invalid_ts] += "|invalid_order_time"

    unknown_currency = df["currency"].isna()
    reason[unknown_currency] += "|unknown_currency"

    # TODO: Candidate: amount/status rules (e.g., amount must be >=0 for paid, may be 0 for failed, etc.)
    neg_amount = df["amount"].notna() & (df["amount"] < 0)
    reason[neg_amount] += "|negative_amount"

    # TODO: Candidate: referential integrity
    reason = reason.str.strip("|")
    df["reject_reason"] = reason.replace({"": None})

    quarantine = df[df["reject_reason"].notna()].copy()
    clean = df[df["reject_reason"].isna()].drop(columns=["reject_reason"]).copy()

    stats = {
        "total": int(len(df)),
        "clean": int(len(clean)),
        "quarantine": int(len(quarantine)),
        "by_reason": quarantine["reject_reason"].value_counts(dropna=True).to_dict(),
    }
    return clean, quarantine, stats
