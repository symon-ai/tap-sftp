import os
from pathlib import Path
from unittest.mock import patch

from pytest import fixture
from singer.catalog import Catalog
from tap_sftp.client import connection


@fixture
def sftp_client(monkeypatch):
    # overwrite the client so we never actually try to connect to an sftp
    with patch('paramiko.SFTPClient.from_transport'), patch('paramiko.Transport'):
        yield connection({'host': '', 'username': ''})

def get_sample_file_path(file_name):
    path = Path(__file__).parent.absolute()
    return f'{path}/{file_name}'

def get_catalog():
    return Catalog.from_dict(
        {
            "streams": [
                    {
                        'tap_stream_id': 'fake_file',
                        'stream': 'fake_file',
                        'schema': {
                            "type": "object",
                            "properties": {
                                "Col1": {
                                    "type": [
                                        "null",
                                        "string"
                                    ]
                                },
                                "Col2": {
                                    "type": [
                                        "null",
                                        "string"
                                    ]
                                }
                            }
                        },
                        "metadata": [
                            {
                                "breadcrumb": [],
                                "metadata": {
                                    "table-key-properties": [],
                                    "forced-replication-method": "INCREMENTAL",
                                    "selected": True,
                                    "inclusion": "available"
                                }
                            }
                        ]
                    }
            ]
        }
    )


def get_table_spec():
    return {
        'table_name': 'fake_file',
        'search_prefix': '/Export',
        'search_pattern': 'fake_file*',
        'key_properties': [],
        'delimiter': ','
    }


@fixture(params=["file_path"])
def file_handle(request):
    with get_file_handle(request.param) as file_handle:
        yield file_handle


@fixture(params=["file_path"])
def file_handle_unscoped(request):
    return get_file_handle(request.param)


@fixture(params=["file_path"])
def file_handle_second_unscoped(request):
    return get_file_handle(request.param)


def get_file_handle(file_path):
    return open(get_full_file_path(file_path), 'rb')


def get_full_file_path(file_name):
    return os.path.join(os.path.dirname(__file__), file_name)