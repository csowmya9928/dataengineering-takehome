\
from __future__ import annotations

import argparse
import os
from datetime import datetime, timedelta
import pandas as pd
from rich.console import Console

from .io import read_csv, write_parquet, write_json
from .cleaning import clean_customers, clean_events, clean_orders
from .validation import (
    split_clean_quarantine_customers,
    split_clean_quarantine_events,
    split_clean_quarantine_orders,
)
from .metrics import compute_hourly_events, compute_daily_metrics
from .alerts import detect_partial_load

console = Console()

def date_range(start: str, end: str):
    s = datetime.strptime(start, "%Y-%m-%d").date()
    e = datetime.strptime(end, "%Y-%m-%d").date()
    cur = s
    while cur <= e:
        yield cur.strftime("%Y-%m-%d")
        cur += timedelta(days=1)

def load_day(data_dir: str, ingest_date: str):
    base = os.path.join(data_dir, f"ingest_date={ingest_date}")
    customers_path = os.path.join(base, "customers_raw.csv")
    events_path = os.path.join(base, "events_raw.csv")
    orders_path = os.path.join(base, "orders_raw.csv")
    return customers_path, events_path, orders_path

def upsert_daily_metrics(metrics_path: str, new_row: pd.DataFrame) -> None:
    os.makedirs(os.path.dirname(metrics_path), exist_ok=True)
    if os.path.exists(metrics_path):
        old = pd.read_parquet(metrics_path)
        # overwrite row for same ingest_date (idempotent)
        old = old[old["ingest_date"] != new_row.loc[0, "ingest_date"]]
        out = pd.concat([old, new_row], ignore_index=True).sort_values("ingest_date")
    else:
        out = new_row
    out.to_parquet(metrics_path, index=False)

def append_hourly(hourly_path: str, hourly_df: pd.DataFrame) -> None:
    os.makedirs(os.path.dirname(hourly_path), exist_ok=True)
    if os.path.exists(hourly_path):
        old = pd.read_parquet(hourly_path)
        # idempotent for day
        day = hourly_df["ingest_date"].iloc[0] if not hourly_df.empty else None
        if day is not None:
            old = old[old["ingest_date"] != day]
        out = pd.concat([old, hourly_df], ignore_index=True)
    else:
        out = hourly_df
    out.to_parquet(hourly_path, index=False)

def process_day(data_dir: str, out_dir: str, ingest_date: str, chunksize_events: int | None = None) -> None:
    customers_path, events_path, orders_path = load_day(data_dir, ingest_date)
    console.print(f"[bold]Processing {ingest_date}[/bold]")

    # Load raw
    customers_raw = pd.read_csv(customers_path)
    orders_raw = pd.read_csv(orders_path)

    # Events can be large; allow chunksize
    if chunksize_events:
        events_iter = pd.read_csv(events_path, chunksize=chunksize_events)
        events_raw = pd.concat(list(events_iter), ignore_index=True)
    else:
        events_raw = pd.read_csv(events_path)

    # Clean
    customers_cleaned = clean_customers(customers_raw, ingest_date)
    events_cleaned = clean_events(events_raw, ingest_date)
    orders_cleaned = clean_orders(orders_raw, ingest_date)

    # Validate + quarantine
    customers_clean, customers_quarantine, cust_stats = split_clean_quarantine_customers(customers_cleaned)
    events_clean, events_quarantine, ev_stats = split_clean_quarantine_events(events_cleaned, customers_clean)
    orders_clean, orders_quarantine, ord_stats = split_clean_quarantine_orders(orders_cleaned, customers_clean)

    validation_report = {
        "ingest_date": ingest_date,
        "customers": cust_stats,
        "events": ev_stats,
        "orders": ord_stats,
    }

    # Write outputs (overwrite partitions by date -> idempotent)
    def write_partition(kind: str, df: pd.DataFrame, root: str):
        part_dir = os.path.join(root, kind, f"ingest_date={ingest_date}")
        os.makedirs(part_dir, exist_ok=True)
        # simple single file; candidate may implement multiple parts
        write_parquet(df, os.path.join(part_dir, "part-00000.parquet"))

    write_partition("customers", customers_clean, os.path.join(out_dir, "clean"))
    write_partition("events", events_clean, os.path.join(out_dir, "clean"))
    write_partition("orders", orders_clean, os.path.join(out_dir, "clean"))

    write_partition("customers", customers_quarantine, os.path.join(out_dir, "quarantine"))
    write_partition("events", events_quarantine, os.path.join(out_dir, "quarantine"))
    write_partition("orders", orders_quarantine, os.path.join(out_dir, "quarantine"))

    # Reports
    reports_root = os.path.join("reports", f"ingest_date={ingest_date}")
    write_json(validation_report, os.path.join(reports_root, "validation_report.json"))

    # Metrics
    # Ensure datetimes are datetime dtype (pandas may keep objects if all Nones)
    if "event_time_utc" in events_clean.columns:
        events_clean["event_time_utc"] = pd.to_datetime(events_clean["event_time_utc"], utc=True, errors="coerce")
    if "order_time_utc" in orders_clean.columns:
        orders_clean["order_time_utc"] = pd.to_datetime(orders_clean["order_time_utc"], utc=True, errors="coerce")

    hourly = compute_hourly_events(events_clean)
    append_hourly(os.path.join(out_dir, "metrics", "hourly_events.parquet"), hourly)

    daily_row=compute_daily_metrics(ingest_date,customers_clean,customers_quarantine,events_clean,events_quarantine,orders_clean,orders_quarantine)
    daily_metrics_path = os.path.join(out_dir, "metrics", "daily_metrics.parquet")
    upsert_daily_metrics(daily_metrics_path, daily_row)

    # Alerts (uses history if present)
    hist = None
    if os.path.exists(daily_metrics_path):
        hist = pd.read_parquet(daily_metrics_path)
    alerts = detect_partial_load(ingest_date, hourly, hist)
    write_json(alerts, os.path.join(reports_root, "alerts.json"))

    console.print(f"  customers clean/quarantine: {len(customers_clean)}/{len(customers_quarantine)}")
    console.print(f"  events    clean/quarantine: {len(events_clean)}/{len(events_quarantine)}")
    console.print(f"  orders    clean/quarantine: {len(orders_clean)}/{len(orders_quarantine)}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data_dir", required=True)
    ap.add_argument("--out_dir", required=True)
    ap.add_argument("--date", default=None)
    ap.add_argument("--start", default=None)
    ap.add_argument("--end", default=None)
    ap.add_argument("--chunksize_events", type=int, default=None)
    args = ap.parse_args()

    if args.date:
        days = [args.date]
    else:
        if not (args.start and args.end):
            raise SystemExit("Provide either --date or --start and --end")
        days = list(date_range(args.start, args.end))

    for d in days:
        process_day(args.data_dir, args.out_dir, d, chunksize_events=args.chunksize_events)

if __name__ == "__main__":
    main()
