from datetime import datetime
from unittest.mock import patch, mock_open
from tap_sftp import defaults
from tap_sftp.discover import discover_streams
import pytest
from tests.configuration.fixtures import sftp_client, file_handle

date_modified_since_oldest = datetime.fromisoformat('1970-01-01 00:00:00')
date_modified_since_old = datetime.fromisoformat('2016-01-01 00:00:00')
date_modified_since_recent = datetime.fromisoformat('2022-01-01 00:00:00')


@patch('tap_sftp.client.SFTPConnection')
@patch('tap_sftp.client.connection')
@patch('tap_sftp.helper.update_decryption_key')
@patch('file_processors.clients.csv_client.CSVClient.build_streams')
def test_discover_streams_encrypted_csv_file(mock_build_streams, mock_update_decryption_key, mock_connection, mock_sftp_client):
    tap_stream_id = "test1"
    table_specs = [{
            "table_name": tap_stream_id,
            "file_type": "csv",
            "search_prefix": "/test_tmp/bin",
            "search_pattern": "test1.csv",
            "key_properties": []
        }]
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
    streams = [{'tap_stream_id': tap_stream_id, 'schema': {"type": "object", "properties": {}}}]
    files = [{"id": 1, "filepath": "/test_tmp/bin/test1.csv", "last_modified": date_modified_since_oldest,
              "file_size": 12404}]
    sample_size = defaults.SAMPLE_SIZE
    mock_connection.return_value = mock_sftp_client
    mock_sftp_client.get_files.return_value = files
    mock_sftp_client.get_file_handle_for_sample.return_value.__enter__.return_value = mock_open
    mock_build_streams.return_value = streams
    result_streams = discover_streams(config)
    mock_update_decryption_key.assert_called_with(decryption_configs)
    mock_build_streams.assert_called_with(mock_open, sample_size, tap_stream_id=tap_stream_id)
    assert result_streams == streams


@patch('tap_sftp.client.SFTPConnection')
@patch('tap_sftp.client.connection')
@patch('tap_sftp.helper.update_decryption_key')
@patch('file_processors.clients.excel_client.ExcelClient.build_streams')
def test_discover_streams_encrypted_excel_file(mock_build_streams, mock_update_decryption_key, mock_connection, mock_sftp_client):
    worksheets = ["sheet1"]
    table_specs = [{
            "table_name": "test1",
            "file_type": "excel",
            "search_prefix": "/test_tmp/bin",
            "search_pattern": "test1.xlsx",
            "key_properties": [],
            "has_header": True,
            "worksheets": worksheets
        }]
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
    streams = [{'tap_stream_id': 'test1', 'schema': {"type": "object", "properties": {}}}]
    files = [{"id": 1, "filepath": "/test_tmp/bin/test1.xlsx", "last_modified": date_modified_since_oldest,
              "file_size": 12404}]
    sample_size = defaults.SAMPLE_SIZE
    mock_connection.return_value = mock_sftp_client
    mock_sftp_client.get_files.return_value = files
    mock_sftp_client.get_file_handle.return_value.__enter__.return_value = mock_open
    mock_build_streams.return_value = streams
    result_streams = discover_streams(config)
    mock_update_decryption_key.assert_called_with(decryption_configs)
    mock_build_streams.assert_called_with(mock_open, sample_size, worksheets=worksheets)
    assert result_streams == streams


@patch('tap_sftp.client.SFTPConnection')
@patch('tap_sftp.client.connection')
def test_discover_streams_unsupported_file(mock_connection, mock_sftp_client):
    table_specs = [{
            "table_name": "test1",
            "file_type": "unknown",
            "search_prefix": "/test_tmp/bin",
            "search_pattern": "test1",
        }]
    config = {
        "host": "host",
        "port": 22,
        "username": "user",
        "password": "password",
        "search_subdirectories": True,
        "start_date": "1800-01-01",
        "tables": table_specs
    }
    files = [{"id": 1, "filepath": "/test_tmp/bin/test1", "last_modified": date_modified_since_oldest,
              "file_size": 12404}]
    mock_connection.return_value = mock_sftp_client
    mock_sftp_client.get_files.return_value = files
    with pytest.raises(BaseException):
        discover_streams(config)


@patch('tap_sftp.client.SFTPConnection')
@patch('tap_sftp.client.connection')
def test_discover_streams_with_no_matching_file_found(mock_connection, mock_sftp_client):
    table_specs = [{
            "table_name": "test1",
            "file_type": "unknown",
            "search_prefix": "/test_tmp/bin",
            "search_pattern": "test1",
        }]
    config = {
        "host": "host",
        "port": 22,
        "username": "user",
        "password": "password",
        "search_subdirectories": True,
        "start_date": "1800-01-01",
        "tables": table_specs
    }
    files = []
    mock_connection.return_value = mock_sftp_client
    mock_sftp_client.get_files.return_value = files
    result_streams = discover_streams(config)
    assert result_streams == {}


@patch('tap_sftp.client.SFTPConnection')
@patch('tap_sftp.client.connection')
def test_discover_streams_with_large_file(mock_connection, mock_sftp_client):
    table_specs = [{
            "table_name": "test1.xlsx",
            "file_type": "excel",
            "search_prefix": "/test_tmp/bin",
            "search_pattern": "test1",
        }]
    config = {
        "host": "host",
        "port": 22,
        "username": "user",
        "password": "password",
        "search_subdirectories": True,
        "start_date": "1800-01-01",
        "tables": table_specs
    }
    files = [{"id": 1, "filepath": "/test_tmp/bin/test1.xlsx", "last_modified": date_modified_since_oldest,
              "file_size": defaults.MAX_FILE_SIZE*1024+1}]
    mock_connection.return_value = mock_sftp_client
    mock_sftp_client.get_files.return_value = files
    with pytest.raises(BaseException):
        discover_streams(config)
