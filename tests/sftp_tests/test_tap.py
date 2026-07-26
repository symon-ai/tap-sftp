import os
from tap_sftp import tap
from unittest.mock import patch
from singer.catalog import Catalog
from tests.configuration.fixtures import sftp_client, file_handle, get_full_file_path

@patch('tap_sftp.discover.discover_streams')
def test_do_discover(mock_discover_streams):
    config = {}
    mock_discover_streams.return_value = [{"stream": "stream1", "schema": {}}, {"stream": "stream2", "schema": {}}]
    tap.do_discover(config)
    mock_discover_streams.assert_called_with(config)


@patch('tap_sftp.sync.sync_stream')
def test_do_sync(mock_sync_stream):
    config = {}
    state = {}
    catalog = Catalog.from_dict({"streams": []})
    collect_sync_stats = False
    tap.do_sync(config, catalog, state)
    mock_sync_stream.assert_called_with(config, catalog, state, collect_sync_stats)


# WP-32463 CWE-73: error_file_path path-traversal containment


def test_resolve_contained_path_allows_legitimate_relative_path(tmp_path):
    base = str(tmp_path)
    resolved = tap._resolve_contained_path("error.json", base_dir=base)
    assert resolved == os.path.join(base, "error.json")


def test_resolve_contained_path_allows_nested_relative_path(tmp_path):
    base = str(tmp_path)
    resolved = tap._resolve_contained_path("sub/error.json", base_dir=base)
    assert resolved == os.path.join(base, "sub", "error.json")


def test_resolve_contained_path_rejects_parent_traversal(tmp_path):
    base = str(tmp_path)
    assert tap._resolve_contained_path("../../etc/passwd", base_dir=base) is None


def test_resolve_contained_path_rejects_absolute_escape(tmp_path):
    base = str(tmp_path)
    assert tap._resolve_contained_path("/etc/passwd", base_dir=base) is None


def test_resolve_contained_path_rejects_none_and_empty(tmp_path):
    base = str(tmp_path)
    assert tap._resolve_contained_path(None, base_dir=base) is None
    assert tap._resolve_contained_path("", base_dir=base) is None


def test_main_does_not_write_error_file_on_traversal(tmp_path, monkeypatch):
    # A traversal error_file_path must NOT create a file outside the base dir,
    # while a legitimate in-base path IS written.
    monkeypatch.chdir(tmp_path)
    escaped = tmp_path.parent / "escaped_error.json"
    if escaped.exists():
        escaped.unlink()

    class FakeArgs:
        discover = False
        catalog = None
        properties = None
        state = None

        def __init__(self, error_file_path):
            self.config = {"error_file_path": error_file_path, "tables": []}

    def run_main(error_file_path):
        args = FakeArgs(error_file_path)
        # parse_args returns our fake args; do_discover raises so the finally
        # error-writing branch executes with a real bound `args`.
        with patch("tap_sftp.tap.utils.parse_args", return_value=args), \
                patch("tap_sftp.tap.do_discover", side_effect=RuntimeError("boom")):
            # bypass the singer top-exception decorator if present
            fn = getattr(tap.main, "__wrapped__", tap.main)
            args.discover = True
            try:
                fn()
            except BaseException:
                pass

    # traversal path must not escape the base dir
    run_main("../escaped_error.json")
    assert not escaped.exists()

    # legitimate relative path IS written within the base dir
    run_main("error.json")
    assert (tmp_path / "error.json").exists()
