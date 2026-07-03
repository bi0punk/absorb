import json

import pytest

from absorb.registry import (
    bootstrap_registry_from_disk,
    expected_analysis_path,
    expected_post_dir,
    find_cached_payload,
    find_downloaded_payload,
    find_post_dir_in_registry,
    get_registry_connection,
    infer_payload_status,
    init_registry,
    load_processed_shortcodes,
    locate_post_dir,
    read_json_file,
    upsert_registry_record,
)


@pytest.fixture(autouse=True)
def setup(monkeypatch, tmp_path):
    from absorb import constants
    db_path = tmp_path / "registry.sqlite3"
    monkeypatch.setattr(constants, "REGISTRY_DB", db_path)
    monkeypatch.setattr(constants, "BASE_DIR", tmp_path)
    init_registry()
    yield


class TestRegistry:
    def test_init_creates_table(self):
        conn = get_registry_connection()
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        conn.close()
        assert any(r["name"] == "processed_posts" for r in tables)

    def test_upsert_and_find(self, tmp_path):
        post_dir = tmp_path / "source_user" / "-abc123"
        analysis_path = post_dir / "abc123.analysis.json"
        post_dir.mkdir(parents=True)
        analysis_path.write_text('{}')

        upsert_registry_record(
            shortcode="abc123",
            kind="post",
            profile_url="https://www.instagram.com/testuser/",
            source_username="testuser",
            source_label="@testuser",
            post_dir=str(post_dir),
            analysis_path=str(analysis_path),
            status="processed",
        )

        found = find_post_dir_in_registry("abc123")
        assert found is not None
        assert "abc123" in str(found)

    def test_upsert_replaces(self):
        upsert_registry_record("s1", "post", "url", "u", "@u", "/d1", "/a1", "processed")
        upsert_registry_record("s1", "reel", "url", "u", "@u", "/d2", "/a2", "downloaded")

        conn = get_registry_connection()
        rows = conn.execute("SELECT * FROM processed_posts").fetchall()
        conn.close()
        assert len(rows) == 1
        assert rows[0]["kind"] == "reel"

    def test_load_processed_shortcodes(self):
        upsert_registry_record("s1", "post", "url", "u", "@u", "/d1", "/a1", "processed")
        upsert_registry_record("s2", "post", "url", "u", "@u", "/d2", "/a2", "processed")
        upsert_registry_record("s3", "post", "url", "u", "@u", "/d3", "/a3", "downloaded")

        shortcodes = load_processed_shortcodes()
        assert "s1" in shortcodes
        assert "s2" in shortcodes
        assert "s3" not in shortcodes

    def test_find_cached_payload(self, tmp_path):
        post_dir = tmp_path / "testuser" / "-abc123"
        post_dir.mkdir(parents=True)
        analysis = post_dir / "abc123.analysis.json"
        analysis.write_text(json.dumps({"shortcode": "abc123", "status": "processed"}))

        upsert_registry_record(
            "abc123", "post", "url", "testuser", "@testuser",
            str(post_dir), str(analysis), "processed",
        )

        payload = find_cached_payload("abc123")
        assert payload is not None
        assert payload["shortcode"] == "abc123"

    def test_find_cached_payload_not_found(self):
        assert find_cached_payload("nonexistent") is None

    def test_find_downloaded_payload(self, tmp_path):
        post_dir = tmp_path / "testuser" / "-def456"
        post_dir.mkdir(parents=True)
        analysis = post_dir / "def456.analysis.json"
        analysis.write_text(json.dumps({"shortcode": "def456", "status": "downloaded"}))

        upsert_registry_record(
            "def456", "post", "url", "testuser", "@testuser",
            str(post_dir), str(analysis), "downloaded",
        )

        payload = find_downloaded_payload("def456")
        assert payload is not None
        assert payload["status"] == "downloaded"

    def test_expected_post_dir(self):
        p = expected_post_dir("abc123", "https://www.instagram.com/testuser/")
        assert "testuser" in str(p)
        assert "abc123" in str(p)

    def test_expected_analysis_path(self):
        p = expected_analysis_path("abc123", "https://www.instagram.com/testuser/")
        assert str(p).endswith(".analysis.json")

    def test_read_json_file_exists(self, tmp_path):
        f = tmp_path / "test.json"
        f.write_text('{"key": "value"}')
        assert read_json_file(f) == {"key": "value"}

    def test_read_json_file_not_found(self, tmp_path):
        assert read_json_file(tmp_path / "nonexistent.json") is None

    def test_read_json_file_invalid(self, tmp_path):
        f = tmp_path / "bad.json"
        f.write_text("not json")
        assert read_json_file(f) is None

    def test_infer_payload_status_processed(self):
        assert infer_payload_status({"status": "processed"}) == "processed"

    def test_infer_payload_status_downloaded(self):
        assert infer_payload_status({"status": "downloaded"}) == "downloaded"

    def test_infer_payload_status_other(self):
        assert infer_payload_status({"status": "pending"}) == ""

    def test_infer_payload_status_none(self):
        assert infer_payload_status(None) == ""

    def test_bootstrap_registry_from_disk(self, tmp_path):
        u_dir = tmp_path / "testuser"
        p1 = u_dir / "-abc123" / "abc123.analysis.json"
        p1.parent.mkdir(parents=True)
        p1.write_text(json.dumps({"shortcode": "abc123", "kind": "post", "status": "processed"}))
        p2 = u_dir / "-def456" / "def456.analysis.json"
        p2.parent.mkdir(parents=True)
        p2.write_text(json.dumps({"shortcode": "def456", "kind": "reel", "status": "processed"}))

        count = bootstrap_registry_from_disk()
        assert count == 2

        shortcodes = load_processed_shortcodes()
        assert "abc123" in shortcodes
        assert "def456" in shortcodes

    def test_locate_post_dir_from_registry(self, tmp_path):
        post_dir = tmp_path / "u" / "-abc123"
        analysis = post_dir / "abc123.analysis.json"
        post_dir.mkdir(parents=True)
        analysis.write_text('{}')
        upsert_registry_record("abc123", "post", "url", "u", "@u", str(post_dir), str(analysis), "processed")

        found = locate_post_dir("abc123")
        assert found is not None
        assert found.name == "-abc123"
