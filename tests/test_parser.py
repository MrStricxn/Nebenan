import pytest
from src.parser import extract_listings_from_html


SAMPLE_HTML = """
<div class="marketplace-item" data-listing-id="123" data-seller-id="seller_abc">
  <a class="seller-link" href="/profile/seller_abc">Hans Mueller</a>
  <h2 class="listing-title">Vintage Sofa</h2>
  <span class="listing-price">50 €</span>
  <a class="listing-url" href="/marketplace/123">Details</a>
  <time datetime="2026-06-29T10:30:00Z">vor 2 Stunden</time>
</div>
<div class="marketplace-item" data-listing-id="456" data-seller-id="seller_xyz">
  <a class="seller-link" href="/profile/seller_xyz">Anna Schmidt</a>
  <h2 class="listing-title">Tisch</h2>
  <span class="listing-price">30 €</span>
  <a class="listing-url" href="/marketplace/456">Details</a>
  <time datetime="2026-06-29T08:00:00Z">vor 4 Stunden</time>
</div>
"""


def test_extract_returns_two_listings():
    listings = extract_listings_from_html(SAMPLE_HTML)
    assert len(listings) == 2


def test_extract_listing_fields():
    listings = extract_listings_from_html(SAMPLE_HTML)
    first = listings[0]
    assert first["listing_id"] == "123"
    assert first["seller_id"] == "seller_abc"
    assert first["seller_name"] == "Hans Mueller"
    assert first["title"] == "Vintage Sofa"
    assert first["price"] == "50 €"
    assert "nebenan.de" in first["url"] or first["url"].startswith("/marketplace/")
    assert first["published_at"] == "2026-06-29T10:30:00Z"


def test_extract_empty_html():
    listings = extract_listings_from_html("<html><body></body></html>")
    assert listings == []
