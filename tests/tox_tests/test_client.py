import os.path
from datetime import datetime
import time
from unittest.mock import patch, mock_open
import pytest
import stat
from paramiko.sftp_attr import SFTPAttributes
from tests.configuration.fixtures import get_sample_file_path, sftp_client, get_full_file_path, file_handle_unscoped, \
    file_handle_second_unscoped

date_modified_since_oldest = datetime.fromisoformat('1970-01-01 00:00:00')
date_modified_since_old = datetime.fromisoformat('2016-01-01 00:00:00')
date_modified_since_recent = datetime.fromisoformat('2022-01-01 00:00:00')

files = [
    {"id": 1, "filepath": "/test_tmp/bin/test1.csv", "last_modified": date_modified_since_oldest, "file_size": 12404},
    {"id": 2, "filepath": "/test_tmp/bin/test2.csv", "last_modified": date_modified_since_oldest, "file_size": 50458},
    {"id": 3, "filepath": "/test_tmp/bin/test2.csv", "last_modified": date_modified_since_old, "file_size": 50458},
    {"id": 4, "filepath": "/test_tmp/bin/test2_10.csv", "last_modified": date_modified_since_old, "file_size": 50458},
    {"id": 5, "filepath": "/test_tmp/bin/test2.csv", "last_modified": date_modified_since_recent, "file_size": 25874},
    {"id": 6, "filepath": "/test_tmp/bin/test2.csv.zip", "last_modified": date_modified_since_recent,
     "file_size": 25874},
    {"id": 7, "filepath": "/test_tmp/bin/test3.csv", "last_modified": date_modified_since_recent, "file_size": 25874},
    {"id": 8, "filepath": "/test_tmp/bin/test3.csv.zip", "last_modified": date_modified_since_recent,
     "file_size": 25874},
    {"id": 9, "filepath": "/test_home/bin/orders.csv", "last_modified": date_modified_since_recent, "file_size": 25874},
    {"id": 10, "filepath": "/test_tmp/bin/orders.csv.zip", "last_modified": date_modified_since_recent,
     "file_size": 25874}]


@patch('tap_sftp.client.SFTPConnection.get_files_by_prefix')
def test_get_files(mock_get_files_by_prefix, sftp_client):
    """Testing scenario -
            Testing get_files function to verify getting files by prefix and search pattern and SUT should
            return the correct files."""
    prefix = "/test_tmp/bin"
    search_pattern = "test2.csv"
    mock_get_files_by_prefix.return_value = files
    matched_files = sftp_client.get_files(prefix, search_pattern, date_modified_since_old)
    assert len(matched_files) == 1
    assert len([file for file in matched_files if file["id"] in [5]]) == 1


@patch('tap_sftp.client.SFTPConnection.get_files_by_prefix')
def test_get_files_with_wildcard_in_search_pattern(mock_get_files_by_prefix, sftp_client):
    """Testing scenario -
            Testing get_files function to verify getting files by prefix and wildcard in search pattern and SUT should
            return the correct files."""
    prefix = "/test_tmp/bin"
    search_pattern = "test2(.*)"
    mock_get_files_by_prefix.return_value = files
    matched_files = sftp_client.get_files(prefix, search_pattern, date_modified_since_oldest)
    assert len(matched_files) == 4
    assert len([file for file in matched_files if file["id"] in [3, 4, 5, 6]]) == 4


@patch('tap_sftp.client.SFTPConnection.get_files_by_prefix')
def test_get_files_without_modified_date_provided(mock_get_files_by_prefix, sftp_client):
    """Testing scenario -
            Testing get_files function to verify getting files by prefix and search pattern but no modified date
            and SUT should return the correct files."""
    prefix = "/test_tmp/bin"
    search_pattern = "(test2.*)"
    mock_get_files_by_prefix.return_value = files
    matched_files = sftp_client.get_files(prefix, search_pattern, None)
    assert len(matched_files) == 5
    assert len([file for file in matched_files if file["id"] in [2, 3, 4, 5, 6]]) == 5


def test_get_files_by_prefix(sftp_client):
    """Testing scenario -
            Testing get_files_by_prefix function to verify getting files by prefix and SUT should return the
            correct files."""
    prefix = "/Data"
    file_result1: SFTPAttributes = SFTPAttributes()
    file_result1.filename = "test1.csv"
    file_result1.st_size = 12404
    file_result1.st_mode = stat.S_IFREG
    file_result1.st_mtime = time.mktime(date_modified_since_old.timetuple())

    file_result2: SFTPAttributes = SFTPAttributes()
    file_result2.filename = "test2.csv"
    file_result2.st_size = 2048
    file_result2.st_mode = stat.S_IFREG
    file_result1.st_mtime = time.mktime(date_modified_since_oldest.timetuple())

    file_result3: SFTPAttributes = SFTPAttributes()
    file_result3.filename = "test1.jpg"
    file_result3.st_size = 4096
    file_result3.st_mode = stat.S_IFREG

    file_result4: SFTPAttributes = SFTPAttributes()
    file_result4.filename = "empty_file"
    file_result4.st_size = 0
    file_result4.st_mode = stat.S_IFREG

    file_result5: SFTPAttributes = SFTPAttributes()
    file_result5.filename = "test_dir"
    file_result5.st_size = 10
    file_result5.st_mode = stat.S_IFDIR

    sftp_result = [file_result1, file_result2, file_result3, file_result4, file_result5]
    sftp_client.sftp.listdir_attr.side_effect = lambda \
        p: [] if p == f'{prefix}/{file_result5.filename}' else sftp_result
    matched_files = sftp_client.get_files_by_prefix(prefix)
    assert len(matched_files) == 3
    assert len([file for file in matched_files if
                file['file_size'] == 0 or file['filepath'] == f'{prefix}/{file_result4.filename}']) == 0


@patch('tap_sftp.helper.load_file_encrypted')
@patch('paramiko.sftp_file.SFTPFile')
@patch('tempfile.TemporaryDirectory.__enter__')
def test_get_file_handle_with_remote_decryption_config(mock_tempfile, mock_sftp_file, mock_load_file_encrypted,
                                                       sftp_client):
    """Testing scenario -
            Testing get_file_handle function to verify getting file handle with decryption config with remote decryption
             for the sftp file and SUT should download the decrypted file locally and return correct file handle."""
    prefix = "/sftp_path"
    tmp_dir_name = get_full_file_path("../data")
    file_name = "fake_file.txt.pgp"
    original_file_name = os.path.splitext(file_name)[0]
    sftp_path = f'{prefix}/{file_name}'
    decrypt_path = f'{tmp_dir_name}/{original_file_name}'
    mock_tempfile.return_value = tmp_dir_name
    file = {"id": 1, "filepath": sftp_path, "last_modified": date_modified_since_oldest, "file_size": 12404}
    decryption_config = {
        "key": "key",
        "gnupghome": "home",
        "passphrase": "passphrase",
        "decrypt_remote": True
    }
    sftp_client.sftp.open.return_value.__enter__.return_value = mock_sftp_file
    mock_load_file_encrypted.return_value = decrypt_path
    with sftp_client.get_file_handle(file, decryption_config) as file_handle:
        mock_load_file_encrypted.assert_called_with(mock_sftp_file, decryption_config.get("key"),
                                                    decryption_config.get("gnupghome"),
                                                    decryption_config.get("passphrase"), decrypt_path)
        assert file_handle.name == decrypt_path


@pytest.mark.parametrize("file_handle_second_unscoped", ["../data/fake_file.txt"], indirect=True)
@pytest.mark.parametrize("file_handle_unscoped", ["../data/fake_file.txt.pgp"], indirect=True)
@patch('builtins.open')
@patch('file_processors.utils.decrypt.gpg_decrypt_to_file')
@patch('tempfile.TemporaryDirectory.__enter__')
def test_get_file_handle_with_local_decryption_config(mock_tempfile, mock_decrypt_to_file, mock_open,
                                                      file_handle_unscoped, file_handle_second_unscoped, sftp_client):
    """Testing scenario -
            Testing get_file_handle function to verify getting file handle with decryption config with local decryption
             for the sftp file and SUT should download the encrypted file decrypt locally and return correct file handle."""
    prefix = "/sftp_path"
    tmp_dir_name = get_full_file_path("../data")
    file_name = "fake_file.txt.pgp"
    original_file_name = os.path.splitext(file_name)[0]
    sftp_path = f'{prefix}/{file_name}'
    encrypt_path = f'{tmp_dir_name}/{file_name}'
    decrypt_path = f'{tmp_dir_name}/{original_file_name}'
    mock_tempfile.return_value = tmp_dir_name
    file = {"id": 1, "filepath": sftp_path, "last_modified": date_modified_since_oldest, "file_size": 12404}
    decryption_config = {
        "key": "key",
        "gnupghome": "home",
        "passphrase": "passphrase",
        "decrypt_remote": False
    }

    mock_decrypt_to_file.return_value = decrypt_path
    mock_open.side_effect = lambda p, b, encoding=None: file_handle_unscoped if p == encrypt_path else file_handle_second_unscoped
    returned_file_handle = sftp_client.get_file_handle(file, decryption_config)
    mock_decrypt_to_file.assert_called_with(file_handle_unscoped, decryption_config.get("key"),
                                            decryption_config.get("gnupghome"),
                                            decryption_config.get("passphrase"), decrypt_path)
    assert returned_file_handle.name == decrypt_path



# TODO
#
# # @pytest.mark.parametrize("file_handle_second", ["../data/fake_file.txt"], indirect=True)
# # @pytest.mark.parametrize("file_handle", ["../data/fake_file.txt.pgp"], indirect=True)
# # @patch('builtins.open')
# @patch('file_processors.utils.decrypt.gpg_decrypt_to_file')
# @patch('tempfile.TemporaryDirectory.__enter__')
# def test_get_file_handle_with_local_decryption_config2(mock_tempfile,  mock_decrypt_to_file, sftp_client):
#     """Testing scenario -
#             Testing get_file_handle function to verify getting file handle with decryption config with local decryption
#              for the sftp file and SUT should download the encrypted file decrypt locally and return correct file handle."""
#     prefix = "/sftp_path"
#     tmp_dir_name = get_full_file_path("../data")
#     file_name = "fake_file.txt.pgp"
#     original_file_name = os.path.splitext(file_name)[0]
#     sftp_path = f'{prefix}/{file_name}'
#     encrypt_path = f'{tmp_dir_name}/{file_name}'
#     decrypt_path = f'{tmp_dir_name}/{original_file_name}'
#     mock_tempfile.return_value = tmp_dir_name
#     file = {"id": 1, "filepath": sftp_path, "last_modified": date_modified_since_oldest, "file_size": 12404}
#     decryption_config = {
#         "key": "key",
#         "gnupghome": "home",
#         "passphrase": "passphrase",
#         "decrypt_remote": False
#     }
#
#     with open(encrypt_path, 'rb') as enc_file_handle:
#         with open(decrypt_path, 'rb') as dec_file_handle:
#             moc = mock_open()
#     # sftp_client.sftp.get.return_value.__enter__.return_value = mock_sftp_file
#             mock_decrypt_to_file.return_value = decrypt_path
#             # sftp_client.sftp.get.return_value =
#             # mock_open.return_value = file_handle
#             # mock_open.side_effect = lambda p: file_handle if p == encrypt_path else file_handle_second
#             mock_open.side_effect = lambda p, b, encoding=None: enc_file_handle if p == encrypt_path else dec_file_handle
#             # mock_open.__enter__.side_effect = file_handle_side_effect
#             returned_file_handle = sftp_client.get_file_handle(file, decryption_config)
#             mock_decrypt_to_file.assert_called_with(enc_file_handle, decryption_config.get("key"),
#                                                         decryption_config.get("gnupghome"),
#                                                         decryption_config.get("passphrase"), decrypt_path)
