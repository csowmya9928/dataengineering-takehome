\
from __future__ import annotations

import re
from datetime import datetime, timezone
from dateutil import parser

CANON_CURRENCY = {
    "usd": "USD",
    "$": "USD",
    "us$": "USD",
    "eur": "EUR",
    "€": "EUR",
}

CANON_PLATFORM = {
    "ios": "ios",
    "iphone": "ios",
    "android": "android",
    "and": "android",
    "web": "web",
    "browser": "web",
}

CANON_STATUS_CUSTOMER = {
    "active": "active",
    "inactive": "inactive",
    "banned": "banned",
}

CANON_ORDER_STATUS = {
    "paid": "paid",
    "succeeded": "paid",
    "failed": "failed",
    "refunded": "refunded",
    "chargeback": "chargeback",
}

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

def is_valid_email(email: str | None) -> bool:
    if not email or not isinstance(email, str):
        return False
    return bool(EMAIL_RE.match(email.strip()))

def normalize_currency(x: str | None) -> str | None:
    """Normalize currency strings. Return canonical (e.g., USD) or None if unknown."""
    if x is None:
        return None
    s = str(x).strip().lower()
    s = s.replace(" ", "")
    return CANON_CURRENCY.get(s)

def normalize_platform(x: str | None) -> str | None:
    if x is None:
        return None
    s = str(x).strip().lower()
    return CANON_PLATFORM.get(s, s if s in {"ios","android","web"} else None)

def normalize_customer_status(x: str | None) -> str | None:
    if x is None:
        return None
    s = str(x).strip().lower()
    return CANON_STATUS_CUSTOMER.get(s)

def normalize_order_status(x: str | None) -> str | None:
    if x is None:
        return None
    s = str(x).strip().lower()
    return CANON_ORDER_STATUS.get(s)

def parse_timestamp_to_utc(value: str | None) -> datetime | None:
    """
    Parse a timestamp in mixed formats into an aware UTC datetime.

    TODO: Candidate should harden parsing, handle known bad formats, and decide how to treat
    naive timestamps (assume UTC? assume local? assume partition date timezone?).
    """
    if value is None or (isinstance(value, float) and value != value):
        return None
    try:
        dt = parser.parse(str(value))
        if dt.tzinfo is None:
            # Starter policy: assume naive timestamps are UTC (candidate may revise; document choice)
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def safe_float(x) -> float | None:
    try:
        if x is None:
            return None
        if isinstance(x, str) and x.strip() == "":
            return None
        v = float(x)
        if v != v:  # NaN
            return None
        return v
    except Exception:
        return None
