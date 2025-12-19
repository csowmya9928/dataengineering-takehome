\
import pandas as pd
import pytest
import sys, os
sys.path.append(os.path.abspath("."))

from src.metrics import compute_hourly_events,compute_daily_metrics

def test_compute_hourly_events():
    df = pd.DataFrame({
        "ingest_date":["2025-12-10","2025-12-10","2025-12-10"],
        "event_time_utc": pd.to_datetime(["2025-12-10T00:10:00Z","2025-12-10T00:50:00Z","2025-12-10T01:05:00Z"], utc=True),
        "customer_id":["c1","c2","c1"]
    })
    hourly = compute_hourly_events(df)
    assert hourly["event_count"].sum() == 3
    assert len(hourly) == 2

    
def test_compute_daily_metrics_positive_path():
    """
    - All input datasets are present and valid
    - Daily metrics are computed correctly
    - Counts, rates, quality metrics, and breakdowns are populated as expected
    - No edge cases (empty data, divide-by-zero) are involved

    Why this test is important:
    This represents a normal, healthy pipeline run where data quality is good.
    """

    ingest_date = "2025-12-10"

   
    # Customers data
    customers_clean = pd.DataFrame(
        {
            "customer_id": ["c1", "c2"],
            "email": ["a@x.com", "b@x.com"],
            "country": ["US", "US"],
        }
    )

    # One quarantined customer
    customers_quarantine = pd.DataFrame({"customer_id": ["c3"]})

    # Events data
    events_clean = pd.DataFrame(
        {
            "event_id": ["e1", "e2", "e3"],
            "customer_id": ["c1", "c2", "c1"],
            "event_time_utc": pd.to_datetime(
                [
                    "2025-12-10T00:10:00Z",
                    "2025-12-10T01:10:00Z",
                    "2025-12-10T02:10:00Z",
                ],
                utc=True,
            ),
            "event_type": ["click", "click", "view"],
            "platform": ["ios", "ios", "web"],
            "duration_ms": [100, 200, 300],
        }
    )

    # No quarantined events
    events_quarantine = pd.DataFrame(columns=events_clean.columns)

    # Orders data
   
    orders_clean = pd.DataFrame(
        {
            "order_id": ["o1", "o2"],
            "customer_id": ["c1", "c2"],
            "order_time_utc": pd.to_datetime(
                ["2025-12-10T03:00:00Z", "2025-12-10T04:00:00Z"],
                utc=True,
            ),
            "amount": [10.0, 20.0],
            "currency": ["USD", "USD"],
            "status": ["paid", "paid"],
        }
    )

    # No quarantined orders
    orders_quarantine = pd.DataFrame(columns=orders_clean.columns)
    
    # Run daily metrics
    
    out = compute_daily_metrics(
        ingest_date,
        customers_clean,
        customers_quarantine,
        events_clean,
        events_quarantine,
        orders_clean,
        orders_quarantine,
    )

   
    # Assertions

    # One row per ingest_date
    assert len(out) == 1
    row = out.iloc[0]

    # ----- Volume checks -----
    assert row["customers_total"] == 3
    assert row["customers_clean"] == 2
    assert row["customers_quarantine"] == 1

    assert row["events_total"] == 3
    assert row["events_clean"] == 3
    assert row["events_quarantine"] == 0

    assert row["orders_total"] == 2
    assert row["orders_clean"] == 2
    assert row["orders_quarantine"] == 0

    # ----- Quarantine rate checks -----
    # customers: 1 quarantined out of 3 total
    assert row["quarantine_rate_customers"] == pytest.approx(1 / 3)

    # no quarantined events or orders
    assert row["quarantine_rate_events"] == pytest.approx(0.0)
    assert row["quarantine_rate_orders"] == pytest.approx(0.0)

    # ----- Quality checks -----
    # timestamps are valid → invalid rates should be 0
    assert row["invalid_event_timestamp_rate"] == pytest.approx(0.0)
    assert row["invalid_order_timestamp_rate"] == pytest.approx(0.0)

    # ----- Breakdown checks -----
    # breakdowns are stored as dictionaries
    assert isinstance(row["events_by_event_type"], dict)
    assert row["events_by_event_type"]["click"] == 2
    assert row["events_by_event_type"]["view"] == 1

    assert isinstance(row["orders_by_status"], dict)
    assert row["orders_by_status"]["paid"] == 2


def test_compute_daily_metrics_negative_all_empty_inputs():
    """
    NEGATIVE TEST CASE (Edge Case)

    What this test validates:
    - The function handles completely empty inputs safely
    - No crashes, no NaNs, no divide-by-zero errors
    - All metrics default to zero or empty structures
    """

    ingest_date = "2025-12-10"

    # Empty inputs for all datasets
    empty_customers = pd.DataFrame(columns=["customer_id", "email", "country"])
    empty_events = pd.DataFrame(
        columns=["event_id", "customer_id", "event_time_utc", "event_type", "platform", "duration_ms"]
    )
    empty_orders = pd.DataFrame(
        columns=["order_id", "customer_id", "order_time_utc", "amount", "currency", "status"]
    )

    out = compute_daily_metrics(
        ingest_date,
        empty_customers,
        empty_customers,
        empty_events,
        empty_events,
        empty_orders,
        empty_orders,
    )

    # One row should still be produced
    assert len(out) == 1
    row = out.iloc[0]

    # ----- Volume should be zero -----
    assert row["customers_total"] == 0
    assert row["events_total"] == 0
    assert row["orders_total"] == 0

    # ----- Rates should safely default to 0 -----
    assert row["quarantine_rate_customers"] == pytest.approx(0.0)
    assert row["quarantine_rate_events"] == pytest.approx(0.0)
    assert row["quarantine_rate_orders"] == pytest.approx(0.0)

    # ----- Breakdowns should be empty dicts -----
    assert row["events_by_event_type"] == {}
    assert row["events_by_platform"] == {}
    assert row["orders_by_status"] == {}

    # ----- Numeric sanity metrics default to 0 -----
    assert row["duration_ms_p50"] == pytest.approx(0.0)
    assert row["duration_ms_p95"] == pytest.approx(0.0)
    assert row["amount_p50"] == pytest.approx(0.0)
    assert row["amount_p95"] == pytest.approx(0.0)

