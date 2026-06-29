import pytest
from datetime import datetime, timezone, timedelta
from src.parser import _is_within_hours, _extract_from_api_page, _get_auth_token


SAMPLE_API_PAGE = [
    {
        "post": {
            "id": 12345678,
            "subject": "Vintage Sofa",
            "created_at": "2026-06-29T10:30:00Z",
            "author_details": {
                "associated_gid": "gid://nebenan/User/111222",
                "name": "Hans Mueller",
                "private_message_url": "https://nebenan.de/messages/111222",
            },
            "marketplace_details": {"price_in_cents": 5000, "price_currency": "EUR"},
        }
    },
    {
        "post": {
            "id": 99887766,
            "subject": "Tisch",
            "created_at": "2026-06-29T08:00:00Z",
            "author_details": {
                "associated_gid": "gid://nebenan/User/333444",
                "name": "Anna Schmidt",
                "private_message_url": "https://nebenan.de/messages/333444",
            },
            "marketplace_details": None,
        }
    },
]


def test_extract_returns_two_listings():
    listings = _extract_from_api_page(SAMPLE_API_PAGE)
    assert len(listings) == 2


def test_extract_listing_fields():
    listings = _extract_from_api_page(SAMPLE_API_PAGE)
    first = listings[0]
    assert first["listing_id"] == "12345678"
    assert first["seller_id"] == "111222"
    assert first["seller_name"] == "Hans Mueller"
    assert first["title"] == "Vintage Sofa"
    assert first["price"] == "50 €"
    assert "nebenan.de" in first["url"]
    assert first["published_at"] == "2026-06-29T10:30:00Z"
    assert first["message_url"] == "https://nebenan.de/messages/111222"


def test_extract_no_price():
    listings = _extract_from_api_page(SAMPLE_API_PAGE)
    assert listings[1]["price"] == ""


def test_extract_empty_page():
    listings = _extract_from_api_page([])
    assert listings == []


def test_extract_skips_malformed():
    bad_page = [{"post": {}}, {"no_post": True}]
    listings = _extract_from_api_page(bad_page)
    assert listings == []


def test_is_within_hours_recent():
    now_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    assert _is_within_hours(now_iso, 24) is True


def test_is_within_hours_old():
    old = (datetime.now(timezone.utc) - timedelta(hours=25)).isoformat().replace("+00:00", "Z")
    assert _is_within_hours(old, 24) is False


def test_is_within_hours_empty():
    assert _is_within_hours("", 24) is False


def test_get_auth_token():
    state = {"cookies": [
        {"name": "s", "value": "my-token", "domain": "nebenan.de"},
        {"name": "other", "value": "x", "domain": "nebenan.de"},
    ], "origins": []}
    assert _get_auth_token(state) == "my-token"


def test_get_auth_token_missing():
    state = {"cookies": [{"name": "session", "value": "x", "domain": "nebenan.de"}], "origins": []}
    assert _get_auth_token(state) is None
