import pytest
from pathlib import Path
from absorb.state import (
    load_source_state,
    save_source_state,
    get_source_state_entry,
    update_source_state,
    merge_payloads,
    estimate_max_scrolls,
)


@pytest.fixture(autouse=True)
def setup(monkeypatch, tmp_path):
    from absorb import constants
    monkeypatch.setattr(constants, "SOURCE_STATE_FILE", tmp_path / "source_state.json")
    monkeypatch.setattr(constants, "SUMMARY_FILE", tmp_path / "summary.json")
    yield


class TestSourceState:
    def test_load_empty(self):
        assert load_source_state() == {}

    def test_save_and_load(self):
        save_source_state({"url1": {"key": "val"}})
        assert load_source_state() == {"url1": {"key": "val"}}

    def test_get_source_state_entry_missing(self):
        assert get_source_state_entry("nonexistent") == {}

    def test_get_source_state_entry_existing(self):
        save_source_state({"url1": {"latest": "abc123"}})
        entry = get_source_state_entry("url1")
        assert entry["latest"] == "abc123"

    def test_update_source_state(self):
        update_source_state("url1", "abc123", "post")
        state = load_source_state()
        assert state["url1"]["latest_visible_shortcode"] == "abc123"
        assert state["url1"]["latest_visible_kind"] == "post"

    def test_update_source_state_overwrites(self):
        update_source_state("url1", "abc123", "post")
        update_source_state("url1", "def456", "reel")
        state = load_source_state()
        assert state["url1"]["latest_visible_shortcode"] == "def456"

    def test_multiple_sources(self):
        update_source_state("url1", "abc", "post")
        update_source_state("url2", "def", "reel")
        state = load_source_state()
        assert len(state) == 2


class TestMergePayloads:
    def test_empty_both(self):
        assert merge_payloads([], []) == []

    def test_merge_dedup(self):
        existing = [{"shortcode": "a"}, {"shortcode": "b"}]
        new = [{"shortcode": "b"}, {"shortcode": "c"}]
        result = merge_payloads(existing, new)
        codes = [p["shortcode"] for p in result]
        assert codes == ["a", "b", "c"]

    def test_existing_only(self):
        assert merge_payloads([{"shortcode": "a"}], []) == [{"shortcode": "a"}]

    def test_new_only(self):
        assert merge_payloads([], [{"shortcode": "a"}]) == [{"shortcode": "a"}]

    def test_items_without_shortcode_are_excluded(self):
        result = merge_payloads([{"no_shortcode": 1}], [{"no_shortcode": 2}])
        assert len(result) == 0


class TestEstimateMaxScrolls:
    def test_collect_all(self):
        assert estimate_max_scrolls(10, True) == 200

    def test_target_new_count_provided(self):
        assert estimate_max_scrolls(5, False) == 15

    def test_no_target(self):
        assert estimate_max_scrolls(None, False) == 200

    def test_large_target_capped(self):
        assert estimate_max_scrolls(100, False) == 200
