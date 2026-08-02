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


# WP-33373 CWE-73: error_file_path must be constrained to the working directory.
def test_sanitize_error_file_path_allows_legitimate_relative_path(tmp_path):
    legit = 'importID/import-file-copy-execution/tapError.json'
    result = tap.sanitize_error_file_path(legit, base_dir=str(tmp_path))
    expected = os.path.join(os.path.realpath(str(tmp_path)), legit)
    assert result == expected


def test_sanitize_error_file_path_rejects_parent_traversal(tmp_path):
    malicious = '../../../../etc/cron.d/pwn'
    result = tap.sanitize_error_file_path(malicious, base_dir=str(tmp_path))
    assert result is None


def test_sanitize_error_file_path_rejects_absolute_escape(tmp_path):
    malicious = '/etc/passwd'
    result = tap.sanitize_error_file_path(malicious, base_dir=str(tmp_path))
    assert result is None


def test_sanitize_error_file_path_none_when_missing():
    assert tap.sanitize_error_file_path(None) is None
    assert tap.sanitize_error_file_path('') is None
