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
