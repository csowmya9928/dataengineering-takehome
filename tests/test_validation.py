\
import pandas as pd
import sys, os
sys.path.append(os.path.abspath("."))

from src.cleaning import clean_customers, clean_orders
from src.validation import split_clean_quarantine_orders,split_clean_quarantine_customers

def test_orders_unknown_currency_quarantined():
    raw = pd.DataFrame([{
        "order_id":"o1","customer_id":"c1","order_time":"2025-12-10 10:00:00",
        "amount":"10.0","currency":"???","status":"paid","ingest_date":"2025-12-10"
    }])
    cust = pd.DataFrame([{
        "customer_id":"c1","email":"a@b.com","created_at":"2025-01-01","country":"US","status":"active","ingest_date":"2025-12-10"
    }])
    cust_cleaned = clean_customers(cust, "2025-12-10")
    orders_cleaned = clean_orders(raw, "2025-12-10")
    clean, quarantine, stats = split_clean_quarantine_orders(orders_cleaned, cust_cleaned)
    assert len(clean) == 0
    assert len(quarantine) == 1
    assert "unknown_currency" in quarantine.iloc[0]["reject_reason"]


def test_split_clean_quarantine_customers_one_clean_row():
    # This test ensures a fully valid customer row lands in CLEAN (no reject_reason)
    ingest_date = "2025-12-10"

    df = pd.DataFrame({
        "customer_id": ["c12345"],
        "email": ["a@b.com"],
        "email_valid": [True],
        "created_at_utc": [pd.Timestamp("2025-12-10T01:00:00Z")],
        "status": ["active"],
        "country": ["US"],
        "ingest_date": [ingest_date],
    })

    clean, quarantine, stats = split_clean_quarantine_customers(df, ingest_date=ingest_date)

    assert len(clean) == 1
    assert len(quarantine) == 0
    assert "reject_reason" not in clean.columns  # clean must NOT include reject_reason
    assert stats["total"] == 1
    assert stats["clean"] == 1
    assert stats["quarantine"] == 0


def test_split_clean_quarantine_customers_quarantine_reasons_and_stats():
    # This test ensures invalid rows land in QUARANTINE with correct reject_reason tokens,
    # and stats (total/clean/quarantine/by_reason) are consistent.
    ingest_date = "2025-12-10"

    df = pd.DataFrame({
        # Row 0: valid
        "customer_id": ["c12345",
                        # Row 1: missing_customer_id
                        pd.NA,
                        # Row 2: invalid_customer_id (wrong format)
                        "x99999",
                        # Row 3: missing_created_at
                        "c22222",
                        # Row 4: invalid_email (email_valid False)
                        "c33333",
                        # Row 5: invalid_status
                        "c44444",
                        # Row 6: invalid_country
                        "c55555",
                        # Row 7: ingest_date_mismatch
                        "c66666"],
        "email": ["ok@ex.com",
                  "m@ex.com",
                  "x@ex.com",
                  "t@ex.com",
                  "bad@ex.com",
                  "s@ex.com",
                  "u@ex.com",
                  "v@ex.com"],
        "email_valid": [True,
                        True,
                        True,
                        True,
                        False,   # triggers |invalid_email
                        True,
                        True,
                        True],
        "created_at_utc": [
            pd.Timestamp("2025-12-10T01:00:00Z"),
            pd.Timestamp("2025-12-10T02:00:00Z"),
            pd.Timestamp("2025-12-10T03:00:00Z"),
            pd.NaT,  # triggers |missing_created_at
            pd.Timestamp("2025-12-10T05:00:00Z"),
            pd.Timestamp("2025-12-10T06:00:00Z"),
            pd.Timestamp("2025-12-10T07:00:00Z"),
            pd.Timestamp("2025-12-10T08:00:00Z"),
        ],
        "status": ["active",
                   "active",
                   "active",
                   "active",
                   "active",
                   "weird",  # triggers |invalid_status
                   "active",
                   "active"],
        "country": ["US",
                    "US",
                    "US",
                    "US",
                    "US",
                    "US",
                    pd.NA,    # triggers |invalid_country
                    "US"],
        "ingest_date": [ingest_date,
                        ingest_date,
                        ingest_date,
                        ingest_date,
                        ingest_date,
                        ingest_date,
                        ingest_date,
                        "2025-12-09"],  # triggers |ingest_date_mismatch when ingest_date param is 2025-12-10
    })

    clean, quarantine, stats = split_clean_quarantine_customers(df, ingest_date=ingest_date)

    # 1 valid + 7 invalid
    assert len(clean) == 1
    assert len(quarantine) ==2

    # quarantine MUST include reject_reason
    assert "reject_reason" in quarantine.columns

    # Validate reject_reason tokens exist for specific rows (match by customer_id)
    reasons_by_id = dict(zip(quarantine["customer_id"].astype("string"), quarantine["reject_reason"].astype("string")))

    # missing_customer_id row will have customer_id = <NA>, so check via stats/by_reason instead
    assert "invalid_customer_id" in reasons_by_id["x99999"]
    assert "missing_created_at" in reasons_by_id["c22222"]
    assert "invalid_email" in reasons_by_id["c33333"]
    assert "invalid_status" in reasons_by_id["c44444"]
    assert "invalid_country" in reasons_by_id["c55555"]
    assert "ingest_date_mismatch" in reasons_by_id["c66666"]

    # Confirm missing_customer_id was captured somewhere in by_reason
    assert "missing_customer_id" in stats["by_reason"]

    # Stats sanity
    assert stats["total"] == 8
    assert stats["clean"] == 1
    assert stats["quarantine"] == 7
    assert stats["clean"] + stats["quarantine"] == stats["total"]
