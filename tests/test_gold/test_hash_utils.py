"""Tests for canonical event hashing."""

from src.gold.hash_utils import compute_production_hash


def test_hash_deterministic():
    h1 = compute_production_hash(1000.0, 500.0, 50.0, 25.0, None, None)
    h2 = compute_production_hash(1000.0, 500.0, 50.0, 25.0, None, None)
    assert h1 == h2


def test_hash_null_vs_zero_differ():
    h_null = compute_production_hash(1000.0, 500.0, None, None, None, None)
    h_zero = compute_production_hash(1000.0, 500.0, 0.0, 0.0, 0.0, 0)
    assert h_null != h_zero


def test_hash_rounding():
    h1 = compute_production_hash(1000.0001, 500.0, None, None, None, None)
    h2 = compute_production_hash(1000.0004, 500.0, None, None, None, None)
    assert h1 == h2  # Both round to 1000.000


def test_hash_format_is_64_char_hex():
    h = compute_production_hash(100.0, 200.0, 0.0, 0.0, 0.0, 30)
    assert len(h) == 64
    assert all(c in "0123456789abcdef" for c in h)
