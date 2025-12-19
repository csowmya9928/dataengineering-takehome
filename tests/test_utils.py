\
import pytest

import sys, os
sys.path.append(os.path.abspath("."))

from src.utils import normalize_currency, parse_timestamp_to_utc,is_valid_email,normalize_platform,normalize_country,similarity,normalize_customer_status,normalize_order_status,normalize_customer_id,safe_float

def test_normalize_currency():
    assert normalize_currency("USD") == "USD"
    assert normalize_currency("usd") == "USD"
    assert normalize_currency("$") == "USD"
    assert normalize_currency("EUR") == "EUR"
    assert normalize_currency("???") is None

def test_parse_timestamp_to_utc_handles_invalid():
    assert parse_timestamp_to_utc("not a date") is None

def test_parse_timestamp_to_utc_timezone():
    dt = parse_timestamp_to_utc("2025-12-10T10:00:00-05:00")
    assert dt is not None
    # 10:00 EST => 15:00 UTC
    assert dt.hour == 15


# POSITIVE TEST CASE

def test_utils():
    """
    Positive test:
    Verifies that utility functions correctly normalize and parse
    valid, well-formed inputs.
    """

    # Email validation should pass for a valid email
    assert is_valid_email("user@example.com") is True

    # Currency normalization should map variants to canonical values
    assert normalize_currency("usd") == "USD"
    assert normalize_currency("$") == "USD"

    # Platform normalization should map synonyms correctly
    assert normalize_platform("iphone") == "ios"
    assert normalize_platform("web") == "web"

    # Country normalization should map known values
    assert normalize_country("United States") == "US"
    assert normalize_country("UK") == "GB"

    # Customer status normalization should fuzzy-match to canonical values
    assert normalize_customer_status("Active") == "active"

    # Order status normalization should map known statuses
    assert normalize_order_status("succeeded") == "paid"

    # Customer ID normalization should accept exact valid format
    assert normalize_customer_id("c12345") == "c12345"

    # Timestamp parsing should return a UTC-aware datetime
    ts = parse_timestamp_to_utc("2025-01-10T00:00:00Z")
    assert ts is not None
    assert ts.tzinfo is not None

    # safe_float should convert valid numeric strings
    assert safe_float("12.5") == 12.5

# NEGATIVE TEST CASE

def test_utils_invalid_inputs():
    """
    Negative test:
    Ensures utility functions safely return None / False
    for invalid, unknown, or malformed inputs.
    """

    # Invalid email should fail validation
    assert is_valid_email("not-an-email") is False

    # Unknown currency should return None
    assert normalize_currency("bitcoin") is None

    # Unknown platform should return None
    assert normalize_platform("smart_tv") is None

    # Unknown country should return None
    assert normalize_country("Mars") is None

    # Unrecognizable customer status should return None
    assert normalize_customer_status("enabled") is None

    # Unknown order status should return None
    assert normalize_order_status("processing") is None

    # Invalid customer ID formats should return None
    assert normalize_customer_id("12345") is None
    assert normalize_customer_id("customer_123") is None

    # Invalid timestamp should return None
    assert parse_timestamp_to_utc("not-a-date") is None
    assert parse_timestamp_to_utc("20251301") is None  # invalid date

    # safe_float should return None for empty or invalid values
    assert safe_float("") is None
    assert safe_float("abc") is None

