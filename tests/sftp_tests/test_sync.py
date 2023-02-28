from tap_sftp import sync
from datetime import datetime
from unittest.mock import patch, mock_open, Mock
from tap_sftp import defaults
from singer.catalog import Catalog
from singer import metadata
import pytest
from tests.configuration.fixtures import sftp_client, file_handle, get_full_file_path

date_modified_since_oldest = datetime.fromisoformat('1970-01-01 00:00:00')
date_modified_since_old = datetime.fromisoformat('2016-01-01 00:00:00')
date_modified_since_recent = datetime.fromisoformat('2022-01-01 00:00:00')

file_smple_order_policy_xlsx = "../data/sample_orders_policy.xlsx"
streams_csv = [
    {
        "stream": "/test_tmp/bin/test1.csv",
        "tap_stream_id": "test1",
        "schema": {
            "type": "object",
            "properties": {}
        },
        "metadata": [
            {
                "breadcrumb": [],
                "metadata": {
                    "table-key-properties": [],
                    "forced-replication-method": "INCREMENTAL",
                    "inclusion": "available",
                    "src_file_type": "csv/text",
                    "compressed_file_name": "",
                    "selected": True,
                    "file_source": "/test_tmp/bin/test1.csv"
                }
            }
        ]
    }]

streams_excel = [
    {
        "stream": "/test_tmp/bin/test2.xlsx",
        "tap_stream_id": "Sheet1",
        "schema": {
            "type": "object",
            "properties": {}
        },
        "metadata": [
            {
                "breadcrumb": [],
                "metadata": {
                    "table-key-properties": [],
                    "forced-replication-method": "INCREMENTAL",
                    "inclusion": "available",
                    "src_file_type": "excel",
                    "compressed_file_name": "",
                    "selected": False,
                    "file_source": "/test_tmp/bin/test2.xlsx"
                }
            }
        ]
    },
    {
        "stream": "/test_tmp/bin/test2.xlsx",
        "tap_stream_id": "Sheet2",
        "schema": {
            "type": "object",
            "properties": {}
        },
        "metadata": [
            {
                "breadcrumb": [],
                "metadata": {
                    "table-key-properties": [],
                    "forced-replication-method": "INCREMENTAL",
                    "inclusion": "available",
                    "src_file_type": "excel",
                    "compressed_file_name": "",
                    "selected": False,
                    "file_source": "/test_tmp/bin/test2.xlsx"
                }
            }
        ]
    }
]


@patch('tap_sftp.sync.sync_file')
@patch('tap_sftp.client.SFTPConnection')
@patch('tap_sftp.client.connection')
def test_sync_stream(mock_connection, mock_sftp_client, mock_sync_file):
    table_specs = [{
        "table_name": "test1",
        "file_type": "csv",
        "search_prefix": "/test_tmp/bin",
        "search_pattern": "test1.csv",
        "key_properties": []
    },
        {
            "table_name": "sheet1",
            "file_type": "excel",
            "search_prefix": "/test_tmp/bin",
            "search_pattern": "test2.xlsx",
            "key_properties": []
    }
    ]
    decryption_configs = {
        "key_name": "key",
        "gnupghome": "home",
        "passphrase": "passphrase"
    }
    config = {
        "host": "host",
        "port": 22,
        "username": "user",
        "password": "password",
        "search_subdirectories": True,
        "start_date": "1800-01-01",
        "private_key_file": "",
        "tables": table_specs,
        "decryption_configs": decryption_configs
    }
    files = [{"id": 1, "filepath": "/test_tmp/bin/test1.csv", "last_modified": date_modified_since_oldest,
              "file_size": 12404},
             {"id": 2, "filepath": "/test_tmp/bin/test2.xlsx", "last_modified": date_modified_since_oldest,
              "file_size": 12404}]
    state = {}
    catalog = Catalog.from_dict({"streams": streams_csv + streams_excel})
    collect_sync_stats = False
    mock_connection.return_value = mock_sftp_client
    mock_sftp_client.get_files.side_effect = lambda pre, pat, ms, ss: [files[0]] if pat == table_specs[0][
        "search_pattern"] else [files[1]]
    sync.stream_is_selected = Mock(return_value=True)
    sync.sync_stream(config, catalog, state, collect_sync_stats)
    assert mock_sync_file.call_count == 2
    mock_sync_file.assert_called()


@patch('tap_sftp.sync.sync_file')
@patch('tap_sftp.client.SFTPConnection')
@patch('tap_sftp.client.connection')
def test_sync_stream_no_stream_selected(mock_connection, mock_sftp_client, mock_sync_file):
    table_specs = [{
        "table_name": "test1",
        "file_type": "csv",
        "search_prefix": "/test_tmp/bin",
        "search_pattern": "test1.csv",
        "key_properties": []
    },
        {
            "table_name": "sheet1",
            "file_type": "excel",
            "search_prefix": "/test_tmp/bin",
            "search_pattern": "test2.xlsx",
            "key_properties": []
    }
    ]
    decryption_configs = {
        "key_name": "key",
        "gnupghome": "home",
        "passphrase": "passphrase"
    }
    config = {
        "host": "host",
        "port": 22,
        "username": "user",
        "password": "password",
        "search_subdirectories": True,
        "start_date": "1800-01-01",
        "private_key_file": "",
        "tables": table_specs,
        "decryption_configs": decryption_configs
    }
    state = {}
    catalog = Catalog.from_dict({"streams": streams_csv + streams_excel})
    collect_sync_stats = False
    mock_connection.return_value = mock_sftp_client
    sync.stream_is_selected = Mock(return_value=False)
    sync.sync_stream(config, catalog, state, collect_sync_stats)
    assert mock_sync_file.call_count == 0
    mock_sync_file.assert_not_called()


@patch('tap_sftp.sync.sync_file')
@patch('tap_sftp.client.SFTPConnection')
@patch('tap_sftp.client.connection')
def test_sync_stream_for_large_files(mock_connection, mock_sftp_client, mock_sync_file):
    table_specs = [{
        "table_name": "test1",
        "file_type": "csv",
        "search_prefix": "/test_tmp/bin",
        "search_pattern": "test1.csv",
        "key_properties": []
    }
    ]
    decryption_configs = {
        "key_name": "key",
        "gnupghome": "home",
        "passphrase": "passphrase"
    }
    config = {
        "host": "host",
        "port": 22,
        "username": "user",
        "password": "password",
        "search_subdirectories": True,
        "start_date": "1800-01-01",
        "private_key_file": "",
        "tables": table_specs,
        "decryption_configs": decryption_configs
    }
    files = [{"id": 1, "filepath": "/test_tmp/bin/test1.csv", "last_modified": date_modified_since_oldest,
              "file_size": defaults.MAX_FILE_SIZE * 1024 + 1}]
    state = {}
    catalog = Catalog.from_dict({"streams": streams_csv})
    collect_sync_stats = False
    mock_connection.return_value = mock_sftp_client
    mock_sftp_client.get_files.return_value = files
    sync.stream_is_selected = Mock(return_value=True)
    with pytest.raises(BaseException):
        sync.sync_stream(config, catalog, state, collect_sync_stats)


@patch('tap_sftp.sync.sync_file')
@patch('tap_sftp.client.SFTPConnection')
@patch('tap_sftp.client.connection')
def test_sync_stream_no_file_found(mock_connection, mock_sftp_client, mock_sync_file):
    table_specs = [{
        "table_name": "test1",
        "file_type": "csv",
        "search_prefix": "/test_tmp/bin",
        "search_pattern": "test1.csv",
        "key_properties": []
    }
    ]
    decryption_configs = {
        "key_name": "key",
        "gnupghome": "home",
        "passphrase": "passphrase"
    }
    config = {
        "host": "host",
        "port": 22,
        "username": "user",
        "password": "password",
        "search_subdirectories": True,
        "start_date": "1800-01-01",
        "private_key_file": "",
        "tables": table_specs,
        "decryption_configs": decryption_configs
    }
    state = {}
    catalog = Catalog.from_dict({"streams": streams_csv})
    collect_sync_stats = False
    mock_connection.return_value = mock_sftp_client
    mock_sftp_client.get_files.return_value = []
    sync.stream_is_selected = Mock(return_value=True)
    sync.sync_stream(config, catalog, state, collect_sync_stats)
    assert mock_sync_file.call_count == 0
    mock_sync_file.assert_not_called()


@patch('tap_sftp.sync.sync_file')
@patch('tap_sftp.client.SFTPConnection')
@patch('tap_sftp.client.connection')
def test_sync_stream_with_duplicate_table_specs(mock_connection, mock_sftp_client, mock_sync_file):
    table_specs = [{
        "table_name": "test1",
        "file_type": "csv",
        "search_prefix": "/test_tmp/bin",
        "search_pattern": "test1.csv",
        "key_properties": []
    },
        {
            "table_name": "test1",
            "file_type": "csv",
            "search_prefix": "/test_tmp/bin",
            "search_pattern": "test1.csv",
            "key_properties": []
    }
    ]
    decryption_configs = {
        "key_name": "key",
        "gnupghome": "home",
        "passphrase": "passphrase"
    }
    config = {
        "host": "host",
        "port": 22,
        "username": "user",
        "password": "password",
        "search_subdirectories": True,
        "start_date": "1800-01-01",
        "private_key_file": "",
        "tables": table_specs,
        "decryption_configs": decryption_configs
    }
    state = {}
    catalog = Catalog.from_dict({"streams": streams_csv})
    collect_sync_stats = False
    mock_connection.return_value = mock_sftp_client
    sync.stream_is_selected = Mock(return_value=True)
    sync.sync_stream(config, catalog, state, collect_sync_stats)
    assert mock_sync_file.call_count == 0
    mock_sync_file.assert_not_called()


@patch('tap_sftp.sync.sync_file')
@patch('tap_sftp.client.SFTPConnection')
@patch('tap_sftp.client.connection')
def test_sync_stream_with_missing_table_specs(mock_connection, mock_sftp_client, mock_sync_file):
    table_specs = []
    decryption_configs = {
        "key_name": "key",
        "gnupghome": "home",
        "passphrase": "passphrase"
    }
    config = {
        "host": "host",
        "port": 22,
        "username": "user",
        "password": "password",
        "search_subdirectories": True,
        "start_date": "1800-01-01",
        "private_key_file": "",
        "tables": table_specs,
        "decryption_configs": decryption_configs
    }
    state = {}
    catalog = Catalog.from_dict({"streams": streams_csv})
    collect_sync_stats = False
    mock_connection.return_value = mock_sftp_client
    sync.stream_is_selected = Mock(return_value=True)
    sync.sync_stream(config, catalog, state, collect_sync_stats)
    assert mock_sync_file.call_count == 0
    mock_sync_file.assert_not_called()


@patch('tap_sftp.client.SFTPConnection')
@patch('tap_sftp.client.connection')
@patch('tap_sftp.helper.update_decryption_key')
@patch('file_processors.clients.csv_client.CSVClient.sync')
def test_sync_file_for_csv(mock_sync, mock_update_decryption_key, mock_connection, mock_sftp_client):
    table_spec = {
        "table_name": "test1",
        "file_type": "csv",
        "search_prefix": "/test_tmp/bin",
        "search_pattern": "test1.csv",
        "key_properties": [],
        "has_header": True
    }
    decryption_configs = {
        "key_name": "key",
        "gnupghome": "home",
        "passphrase": "passphrase"
    }
    config = {
        "host": "host",
        "port": 22,
        "username": "user",
        "password": "password",
        "search_subdirectories": True,
        "start_date": "1800-01-01",
        "private_key_file": "",
        "tables": table_spec,
        "decryption_configs": decryption_configs
    }
    state = {}
    file = {"id": 1, "filepath": "/test_tmp/bin/test1.csv", "last_modified": date_modified_since_oldest,
            "file_size": 12404}
    collect_sync_stats = False
    catalog = Catalog.from_dict({"streams": streams_csv})
    mock_connection.return_value = mock_sftp_client
    mock_sftp_client.get_file_handle.return_value.__enter__.return_value = mock_open
    sync.sync_file(config, file, catalog.streams, table_spec,
                   state, date_modified_since_oldest, collect_sync_stats, True)
    mock_update_decryption_key.assert_called_with(decryption_configs)
    mock_sync.assert_called_with(mock_open, [stream.to_dict() for stream in catalog.streams], state,
                                 date_modified_since_oldest, columns_to_update=None)


@patch('tap_sftp.client.SFTPConnection')
@patch('tap_sftp.client.connection')
@patch('tap_sftp.helper.update_decryption_key')
@patch('file_processors.clients.excel_client.ExcelClient.sync')
def test_sync_file_for_excel(mock_sync, mock_update_decryption_key, mock_connection, mock_sftp_client):
    table_spec = {
        "table_name": "test1",
        "file_type": "excel",
        "search_prefix": "/test_tmp/bin",
        "search_pattern": "test2.xlsx",
        "key_properties": [],
        "has_header": True
    }
    config = {
        "host": "host",
        "port": 22,
        "username": "user",
        "password": "password",
        "search_subdirectories": True,
        "start_date": "1800-01-01",
        "private_key_file": "",
        "tables": table_spec
    }
    state = {}
    file = {"id": 1, "filepath": "/test_tmp/bin/test2.xlsx", "last_modified": date_modified_since_oldest,
            "file_size": 12404}
    collect_sync_stats = False
    catalog = Catalog.from_dict({"streams": streams_excel})
    mock_connection.return_value = mock_sftp_client
    mock_sftp_client.get_file_handle.return_value.__enter__.return_value = mock_open
    sync.sync_file(config, file, catalog.streams, table_spec,
                   state, date_modified_since_oldest, collect_sync_stats, True)
    mock_update_decryption_key.assert_not_called()
    mock_sync.assert_called_with(mock_open, [stream.to_dict() for stream in catalog.streams], state,
                                 date_modified_since_oldest)


def test_stream_is_selected():
    catalog = Catalog.from_dict({"streams": streams_csv})
    stream = catalog.streams[0]
    mdata = metadata.to_map(stream.metadata)
    result = sync.stream_is_selected(mdata)
    assert result is True
