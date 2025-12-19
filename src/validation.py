\
from __future__ import annotations

import pandas as pd
import re
from datetime import datetime, timezone
from typing import Optional, Tuple


def is_missing_text(series: pd.Series) -> pd.Series:
    s = series.astype("string").str.strip()
    return s.isna() | (s == "") | s.str.lower().isin(["nan", "none", "null"])



def split_clean_quarantine_customers(
    df: pd.DataFrame,
    ingest_date: Optional[str] = None ) -> Tuple[pd.DataFrame, pd.DataFrame, dict]:
    """
    Validate customers and split into clean + quarantine with reject reasons.

    Assumes these cleaned columns exist (from your cleaning step):
      - customer_id
      - email
      - email_valid (bool)
      - created_at_utc (datetime-like or NaT)
      - status (normalized or None)
      - country (normalized or None)
      - ingest_date (optional raw column)

      - Quarantine rows keep reject_reason
      - Clean rows do NOT include reject_reason
    """

    df = df.copy()

    def year_out_of_range(dt_series: pd.Series) -> pd.Series:
        max_year = datetime.now(timezone.utc).year + 1
        years = pd.to_datetime(dt_series, errors="coerce", utc=True).dt.year
        return years.notna() & ((years < 1900) | (years > max_year))

    # customer_id format: c + 5 digits
    CUSTOMER_ID_RE = re.compile(r"^c\d{5}$", re.IGNORECASE)

    # Build reject reasons
    reject_reason = pd.Series("", index=df.index, dtype="string")

    # customer_id required + format
    missing_customer_id = is_missing_text(df["customer_id"])
    reject_reason.loc[missing_customer_id] += "|missing_customer_id"

    cust_str = df["customer_id"].astype("string").str.strip()
    invalid_customer_id = (~missing_customer_id) & (~cust_str.fillna("").str.match(CUSTOMER_ID_RE))
    reject_reason.loc[invalid_customer_id] += "|invalid_customer_id"

    # created_at required + year sanity
    missing_created_at = df["created_at_utc"].isna()
    reject_reason.loc[missing_created_at] += "|missing_created_at"

    created_at_out_of_range = year_out_of_range(df["created_at_utc"])
    reject_reason.loc[created_at_out_of_range] += "|created_at_out_of_range"

    # email required + valid
    missing_email = is_missing_text(df["email"])
    reject_reason.loc[missing_email] += "|missing_email"

    invalid_email = (~missing_email) & (~df["email_valid"].fillna(False))
    reject_reason.loc[invalid_email] += "|invalid_email"

    # status allowed
    allowed_status = {"active", "inactive", "banned"}
    invalid_status = df["status"].isna() | (~df["status"].isin(allowed_status))
    reject_reason.loc[invalid_status] += "|invalid_status"

    # country must exist after normalization
    invalid_country = df["country"].isna()
    reject_reason.loc[invalid_country] += "|invalid_country"

    # ingest_date optional checks
    if "ingest_date" in df.columns:
        missing_ingest_date = is_missing_text(df["ingest_date"])
        reject_reason.loc[missing_ingest_date] += "|missing_ingest_date"

        if ingest_date is not None:
            raw_ing = df["ingest_date"].astype("string").str.strip()
            ingest_mismatch = (~missing_ingest_date) & (raw_ing != ingest_date)
            reject_reason.loc[ingest_mismatch] += "|ingest_date_mismatch"

    # finalize reject_reason column
    reject_reason = reject_reason.str.lstrip("|")
    df["reject_reason"] = reject_reason.replace({"": pd.NA})

    # Split clean vs quarantine
    quarantine = df[df["reject_reason"].notna()].copy()
    clean = df[df["reject_reason"].isna()].drop(columns=["reject_reason"]).copy()
    
    # Stats / reporting
    full_row_dupes_rows_involved = int(df.duplicated(keep=False).sum())

    # duplicates by customer_id (ignore missing)
    non_missing = df[~missing_customer_id]
    customer_id_dupes_rows_involved = int(non_missing.duplicated(subset=["customer_id"], keep=False).sum())

    stats = {
        "total": int(len(df)),
        "clean": int(len(clean)),
        "quarantine": int(len(quarantine)),
        "by_reason": quarantine["reject_reason"].value_counts(dropna=True).to_dict(),
        "duplicates": {
            "full_row_dupes_rows_involved": full_row_dupes_rows_involved,
            "customer_id_dupes_rows_involved": customer_id_dupes_rows_involved,
        },
    }

    return clean, quarantine, stats

def split_clean_quarantine_events(
    df: pd.DataFrame,
    customers_clean: pd.DataFrame,
    ingest_date: Optional[str] = None) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """
    Returns (clean_df, quarantine_df, stats).

    Quarantine rules (events):
      - required columns present
      - timestamps parseable (event_time_utc not NaT)
      - duration_ms >= 0 (if present)
      - referential integrity: customer_id must exist in customers_clean.customer_id
      - ingest_date matches provided ingest_date
      - duplicates detected (full-row + event_id based) are reported in stats
    """
    df = df.copy()

    # required columns present
    required_cols = ["event_id", "customer_id", "event_time_utc", "event_type", "platform"]
    missing_required = [c for c in required_cols if c not in df.columns]
    if missing_required:
        # if required cols are missing from the dataframe itself, everything is invalid
        df["reject_reason"] = "missing_required_columns:" + ",".join(missing_required)
        quarantine = df.copy()
        clean = df.iloc[0:0].copy()  # empty
        stats = {
            "total": int(len(df)),
            "clean": 0,
            "quarantine": int(len(df)),
            "by_reason": quarantine["reject_reason"].value_counts(dropna=True).to_dict(),
            "duplicates": {"full_row": 0, "event_id": 0},
        }
        return clean, quarantine, stats

    reason = pd.Series("", index=df.index, dtype="object")

    # basic validity checks

    # event_id missing
    bad_event_id = is_missing_text(df["event_id"])
    reason[bad_event_id] += "|missing_event_id"

    # customer_id missing
    bad_customer = is_missing_text(df["customer_id"])
    reason[bad_customer] += "|missing_customer_id"

    # event_type missing
    bad_event_type = is_missing_text(df["event_type"])
    reason[bad_event_type] += "|missing_event_type"

    # platform missing (after your normalize_platform, unknowns should become None/NaN)
    bad_platform = df["platform"].isna()
    reason[bad_platform] += "|missing_platform"

    # timestamp invalid
    invalid_ts = df["event_time_utc"].isna()
    reason[invalid_ts] += "|invalid_event_time"
    
    # duration_ms >= 0 and <= 24 hours
    if "duration_ms" in df.columns:
        dur = pd.to_numeric(df["duration_ms"], errors="coerce")

        negative_dur = dur.notna() & (dur < 0)
        reason[negative_dur] += "|negative_duration"

        max_duration_ms = 24 * 60 * 60 * 1000  # 24 hours
        too_large_dur = dur.notna() & (dur > max_duration_ms)
        reason[too_large_dur] += "|duration_exceeds_max"

    # referential integrity
    # only check orphans when customer_id is present
    customer_set = set(
        customers_clean["customer_id"].astype("string").str.strip()
    ) if (customers_clean is not None and "customer_id" in customers_clean.columns) else set()

    has_customer = ~bad_customer
    orphan = has_customer & ~df["customer_id"].astype("string").str.strip().isin(customer_set)
    reason[orphan] += "|orphan_customer_id"

    # ingest_date matches partition date
    if ingest_date is not None and "ingest_date" in df.columns:
        df_ing = df["ingest_date"].astype("string").str.strip()
        bad_ingest = df_ing.isna() | (df_ing == "") | (df_ing != str(ingest_date))
        reason[bad_ingest] += "|ingest_date_mismatch"

    
    # finalize reject_reason column
    
    reason = reason.str.strip("|")
    df["reject_reason"] = reason.replace({"": None})

    quarantine = df[df["reject_reason"].notna()].copy()
    clean = df[df["reject_reason"].isna()].drop(columns=["reject_reason"]).copy()

    
    # stats
    # full-row duplicates in the ORIGINAL df (before split)
    full_row_dup_count = int(df.duplicated(keep=False).sum())

    # event_id duplicates
    event_id_dup_count = 0
    if "event_id" in df.columns:
        s = df["event_id"].astype("string").str.strip()
        valid_event_id = s.notna() & (s != "")
        event_id_dup_count = int(df.loc[valid_event_id, "event_id"].duplicated(keep=False).sum())

    stats = {
        "total": int(len(df)),
        "calen": int(len(clean)),
        "quarantine": int(len(quarantine)),
        "by_reason": quarantine["reject_reason"].value_counts(dropna=True).to_dict(),
        "duplicates": {
            "full_row": full_row_dup_count,
            "event_id": event_id_dup_count,
        },
    }

    return clean, quarantine, stats

def split_clean_quarantine_orders(
    df: pd.DataFrame,
    customers_clean: pd.DataFrame,
    ingest_date: Optional[str] = None ) -> Tuple[pd.DataFrame, pd.DataFrame, dict]:
    """
    Returns (clean_df, quarantine_df, stats).

    Quarantine rules (orders):
      - required columns present
      - timestamps parseable (order_time_utc not NaT)
      - currency known after normalization (currency not null)
      - amount numeric (not null) and not negative
      - referential integrity: customer_id must exist in customers_clean.customer_id
      - ingest_date matches provided ingest_date
    """
    df = df.copy()

    # required columns present
    required_cols = ["order_id", "customer_id", "order_time_utc", "amount", "currency", "status"]
    missing_required = [c for c in required_cols if c not in df.columns]
    if missing_required:
        df["reject_reason"] = "missing_required_columns:" + ",".join(missing_required)
        quarantine = df.copy()
        clean = df.iloc[0:0].copy()
        stats = {
            "total": int(len(df)),
            "clean": 0,
            "quarantine": int(len(df)),
            "by_reason": quarantine["reject_reason"].value_counts(dropna=True).to_dict(),
            "duplicates": {"full_row": 0, "order_id": 0},
        }
        return clean, quarantine, stats

    reason = pd.Series("", index=df.index, dtype="object")

    
    # basic validity checks
   
    # order_id missing
    bad_order_id = is_missing_text(df["order_id"])
    reason[bad_order_id] += "|missing_order_id"

    # customer_id missing
    bad_customer = is_missing_text(df["customer_id"])
    reason[bad_customer] += "|missing_customer_id"

    # timestamp invalid
    invalid_ts = df["order_time_utc"].isna()
    reason[invalid_ts] += "|invalid_order_time"

    # currency unknown
    unknown_currency = df["currency"].isna()
    reason[unknown_currency] += "|unknown_currency"

    # amount numeric & non-negative
    amount_num = pd.to_numeric(df["amount"], errors="coerce")
    missing_amount = amount_num.isna()
    reason[missing_amount] += "|missing_amount"

    neg_amount = amount_num.notna() & (amount_num < 0)
    reason[neg_amount] += "|negative_amount"

    # amount/status rules
   
    status_s = df["status"].astype("string").str.strip().str.lower()

    paid_like = status_s.isin(["paid"])
    failed_like = status_s.isin(["failed"])
    refund_like = status_s.isin(["refunded", "chargeback"])

    # Policy:
    # - paid: amount must be > 0
    # - failed: amount must be 0 or missing
    # - refunded/chargeback: amount must be > 0 (original purchase amount)
    
    paid_bad = paid_like & amount_num.notna() & (amount_num <= 0)
    reason[paid_bad] += "|paid_requires_positive_amount"

    failed_bad = failed_like & amount_num.notna() & (amount_num > 0)
    reason[failed_bad] += "|failed_should_not_have_positive_amount"

    refund_bad = refund_like & amount_num.notna() & (amount_num <= 0)
    reason[refund_bad] += "|refund_requires_positive_amount"

    # If status is missing/blank => quarantine
    bad_status = is_missing_text(df["status"])
    reason[bad_status] += "|missing_status"

    # referential integrity (orders -> customers)
    
    customer_set = set(
        customers_clean["customer_id"].astype("string").str.strip()
    ) if (customers_clean is not None and "customer_id" in customers_clean.columns) else set()

    has_customer = ~bad_customer
    orphan = has_customer & ~df["customer_id"].astype("string").str.strip().isin(customer_set)
    reason[orphan] += "|orphan_customer_id"

    # ingest_date matches partition date (if present)
    if ingest_date is not None and "ingest_date" in df.columns:
        df_ing = df["ingest_date"].astype("string").str.strip()
        bad_ingest = df_ing.isna() | (df_ing == "") | (df_ing != str(ingest_date))
        reason[bad_ingest] += "|ingest_date_mismatch"

  
    # finalize reject_reason
    reason = reason.str.strip("|")
    df["reject_reason"] = reason.replace({"": None})

    quarantine = df[df["reject_reason"].notna()].copy()
    clean = df[df["reject_reason"].isna()].drop(columns=["reject_reason"]).copy()

    # stats
   
    full_row_dup_count = int(df.duplicated(keep=False).sum())

    s_id = df["order_id"].astype("string").str.strip()
    valid_order_id= s_id.notna() & (s_id != "")
    order_id_dup_count = int(df.loc[valid_order_id, "order_id"].duplicated(keep=False).sum())

    stats = {
        "total": int(len(df)),
        "clean": int(len(clean)),
        "quarantine": int(len(quarantine)),
        "by_reason": quarantine["reject_reason"].value_counts(dropna=True).to_dict(),
        "duplicates": {
            "full_row": full_row_dup_count,
            "order_id": order_id_dup_count,
        },
    }

    return clean, quarantine, stats
