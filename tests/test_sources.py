import pytest
from absorb.sources import (
    normalize_profile_url,
    extract_source_username,
    build_source_metadata,
    sanitize_source_dirname,
    parse_positive_limit,
    parse_content_mode,
    build_content_mode_label,
    get_profile_link_selector,
    split_raw_source_entries,
    parse_profile_sources,
    parse_source_jobs,
    parse_cli_sources_and_limit,
    parse_cli_jobs,
)


class TestNormalizeProfileUrl:
    def test_full_url(self):
        assert normalize_profile_url("https://www.instagram.com/testuser/") == "https://www.instagram.com/testuser/"

    def test_without_scheme(self):
        assert normalize_profile_url("instagram.com/testuser") == "https://www.instagram.com/testuser/"

    def test_without_www(self):
        assert normalize_profile_url("https://instagram.com/testuser/") == "https://www.instagram.com/testuser/"

    def test_with_query_params(self):
        assert normalize_profile_url("https://www.instagram.com/testuser/?hl=en") == "https://www.instagram.com/testuser/"

    def test_just_username(self):
        assert normalize_profile_url("testuser") == "https://www.instagram.com/testuser/"

    def test_username_with_path(self):
        assert normalize_profile_url("testuser/extra") == "https://www.instagram.com/testuser/"

    def test_strips_whitespace(self):
        assert normalize_profile_url("  https://www.instagram.com/testuser/  ") == "https://www.instagram.com/testuser/"


class TestExtractSourceUsername:
    def test_simple(self):
        assert extract_source_username("https://www.instagram.com/testuser/") == "testuser"

    def test_no_trailing_slash(self):
        assert extract_source_username("https://www.instagram.com/testuser") == "testuser"


class TestBuildSourceMetadata:
    def test_returns_expected_keys(self):
        meta = build_source_metadata("https://www.instagram.com/testuser/")
        assert meta["profile_url"] == "https://www.instagram.com/testuser/"
        assert meta["source_username"] == "testuser"
        assert meta["source_label"] == "@testuser"


class TestSanitizeSourceDirname:
    def test_keeps_alphanumeric(self):
        assert sanitize_source_dirname("test_user_123") == "test_user_123"

    def test_replaces_special_chars(self):
        assert sanitize_source_dirname("test@user#123") == "test_user_123"

    def test_strips_dots_underscores(self):
        assert sanitize_source_dirname("_test.") == "test"

    def test_empty_fallback(self):
        assert sanitize_source_dirname("!!!") == "source"


class TestParsePositiveLimit:
    def test_valid_int(self):
        assert parse_positive_limit("10") == 10

    def test_invalid_string(self):
        assert parse_positive_limit("abc") == 5

    def test_none(self):
        assert parse_positive_limit(None) == 5

    def test_zero_becomes_fallback(self):
        assert parse_positive_limit("0") == 5

    def test_negative_becomes_fallback(self):
        assert parse_positive_limit("-5") == 5

    def test_custom_fallback(self):
        assert parse_positive_limit("abc", 20) == 20


class TestParseContentMode:
    def test_both(self):
        assert parse_content_mode("both") == "both"

    def test_post(self):
        assert parse_content_mode("post") == "post"

    def test_reel(self):
        assert parse_content_mode("reel") == "reel"

    def test_case_insensitive(self):
        assert parse_content_mode("POST") == "post"

    def test_none_returns_fallback(self):
        assert parse_content_mode(None) == "both"

    def test_invalid_returns_fallback(self):
        assert parse_content_mode("invalid") == "both"


class TestBuildContentModeLabel:
    def test_both(self):
        assert build_content_mode_label("both") == "Posts + Reels"

    def test_post(self):
        assert build_content_mode_label("post") == "Posts only"

    def test_reel(self):
        assert build_content_mode_label("reel") == "Reels only"

    def test_unknown(self):
        assert build_content_mode_label("unknown") == "unknown"


class TestGetProfileLinkSelector:
    def test_post(self):
        assert 'href*="/p/"' in get_profile_link_selector("post")

    def test_reel(self):
        assert 'href*="/reel/"' in get_profile_link_selector("reel")

    def test_both_contains_post_and_reel(self):
        sel = get_profile_link_selector("both")
        assert 'href*="/p/"' in sel
        assert 'href*="/reel/"' in sel


class TestSplitRawSourceEntries:
    def test_single(self):
        assert split_raw_source_entries(["user1"]) == ["user1"]

    def test_comma_separated(self):
        assert split_raw_source_entries(["user1,user2"]) == ["user1", "user2"]

    def test_newline_separated(self):
        assert split_raw_source_entries(["user1\nuser2"]) == ["user1", "user2"]

    def test_semicolon_separated(self):
        assert split_raw_source_entries(["user1;user2"]) == ["user1", "user2"]

    def test_multiple_entries(self):
        assert split_raw_source_entries(["user1", "user2,user3"]) == ["user1", "user2", "user3"]

    def test_empty_entries_skipped(self):
        assert split_raw_source_entries(["user1,,user2"]) == ["user1", "user2"]

    def test_whitespace_trimmed(self):
        assert split_raw_source_entries(["  user1  ,  user2  "]) == ["user1", "user2"]


class TestParseProfileSources:
    def test_single(self):
        result = parse_profile_sources(["user1"])
        assert len(result) == 1
        assert "instagram.com" in result[0]

    def test_dedup(self):
        result = parse_profile_sources(["user1", "user1"])
        assert len(result) == 1

    def test_multiple_unique(self):
        result = parse_profile_sources(["user1", "user2"])
        assert len(result) == 2


class TestParseSourceJobs:
    def test_without_limit(self):
        jobs = parse_source_jobs(["user1"])
        assert len(jobs) == 1
        assert jobs[0]["limit"] == 5

    def test_with_limit(self):
        jobs = parse_source_jobs(["user1|10"])
        assert len(jobs) == 1
        assert jobs[0]["limit"] == 10

    def test_dedup(self):
        jobs = parse_source_jobs(["user1", "user1|10"])
        assert len(jobs) == 1


class TestParseCliSourcesAndLimit:
    def test_source_only(self):
        sources, limit = parse_cli_sources_and_limit(["user1"])
        assert len(sources) == 1
        assert limit == 5

    def test_with_limit(self):
        sources, limit = parse_cli_sources_and_limit(["10", "user1"])
        assert limit == 10
        assert len(sources) == 1

    def test_skips_flags(self):
        sources, limit = parse_cli_sources_and_limit(["--flag", "user1", "10"])
        assert sources == ["user1"]
        assert limit == 10


class TestParseCliJobs:
    def test_single_job(self):
        jobs, global_limit, mode = parse_cli_jobs(["user1"])
        assert len(jobs) == 1
        assert global_limit is None
        assert mode == "both"

    def test_with_content_mode(self):
        jobs, global_limit, mode = parse_cli_jobs(["--content-mode", "reel", "user1"])
        assert mode == "reel"

    def test_with_limit(self):
        jobs, global_limit, mode = parse_cli_jobs(["10", "user1"])
        assert global_limit == 10

    def test_skips_unknown_flags(self):
        jobs, global_limit, mode = parse_cli_jobs(["--other", "user1"])
        assert len(jobs) == 1
