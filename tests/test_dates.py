from datetime import date
import pytest
from absorb.dates import (
    parse_iso_date,
    parse_compact_date,
    validate_date_range,
    parse_post_date_from_iso,
    match_post_date,
    should_stop_after_candidate,
    build_mode_label,
)


class TestParseIsoDate:
    def test_valid(self):
        assert parse_iso_date("2025-06-15") == date(2025, 6, 15)

    def test_none(self):
        assert parse_iso_date(None) is None

    def test_empty(self):
        assert parse_iso_date("") is None

    def test_invalid(self):
        assert parse_iso_date("not-a-date") is None


class TestParseCompactDate:
    def test_valid(self):
        assert parse_compact_date("150625") == date(2025, 6, 15)

    def test_none(self):
        assert parse_compact_date(None) is None

    def test_empty(self):
        assert parse_compact_date("") is None

    def test_wrong_length(self):
        assert parse_compact_date("12345") is None

    def test_invalid_day(self):
        assert parse_compact_date("320625") is None


class TestValidateDateRange:
    def test_valid(self):
        validate_date_range(date(2025, 1, 1), date(2025, 12, 31))

    def test_none_from(self):
        validate_date_range(None, date(2025, 12, 31))

    def test_none_to(self):
        validate_date_range(date(2025, 1, 1), None)

    def test_from_after_to(self):
        with pytest.raises(ValueError):
            validate_date_range(date(2025, 12, 31), date(2025, 1, 1))


class TestParsePostDateFromIso:
    def test_full_iso(self):
        assert parse_post_date_from_iso("2025-06-15T12:30:00Z") == date(2025, 6, 15)

    def test_date_only(self):
        assert parse_post_date_from_iso("2025-06-15") == date(2025, 6, 15)

    def test_none(self):
        assert parse_post_date_from_iso(None) is None

    def test_invalid(self):
        assert parse_post_date_from_iso("bad") is None


class TestMatchPostDate:
    def test_no_bounds(self):
        assert match_post_date(date(2025, 6, 15), None, None) is True

    def test_none_post_date(self):
        assert match_post_date(None, date(2025, 1, 1), date(2025, 12, 31)) is True

    def test_within_bounds(self):
        assert match_post_date(date(2025, 6, 15), date(2025, 1, 1), date(2025, 12, 31)) is True

    def test_before_from(self):
        assert match_post_date(date(2024, 1, 1), date(2025, 1, 1), None) is False

    def test_after_to(self):
        assert match_post_date(date(2026, 1, 1), None, date(2025, 12, 31)) is False

    def test_exact_from(self):
        assert match_post_date(date(2025, 1, 1), date(2025, 1, 1), None) is True

    def test_exact_to(self):
        assert match_post_date(date(2025, 12, 31), None, date(2025, 12, 31)) is True


class TestShouldStopAfterCandidate:
    def test_stop_when_older(self):
        assert should_stop_after_candidate(date(2024, 1, 1), date(2025, 1, 1)) is True

    def test_no_stop_on_same_day(self):
        assert should_stop_after_candidate(date(2025, 1, 1), date(2025, 1, 1)) is False

    def test_no_stop_when_newer(self):
        assert should_stop_after_candidate(date(2025, 6, 15), date(2025, 1, 1)) is False

    def test_no_date_from(self):
        assert should_stop_after_candidate(date(2025, 6, 15), None) is False

    def test_no_post_date(self):
        assert should_stop_after_candidate(None, date(2025, 1, 1)) is False


class TestBuildModeLabel:
    def test_both_bounds(self):
        label = build_mode_label(date(2025, 1, 1), date(2025, 12, 31))
        assert "2025-01-01" in label
        assert "2025-12-31" in label

    def test_from_only(self):
        assert "2025-01-01" in build_mode_label(date(2025, 1, 1), None)

    def test_to_only(self):
        assert "2025-12-31" in build_mode_label(None, date(2025, 12, 31))

    def test_no_bounds(self):
        assert build_mode_label(None, None) == "all available dates"
