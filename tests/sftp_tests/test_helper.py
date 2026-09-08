import os
import tempfile
from unittest.mock import patch, mock_open, Mock
from tap_sftp import helper
import pytest
from tests.configuration.fixtures import sftp_client, file_handle, file_handle_second
import singer  # type: ignore
import json
import base64
from file_processors.utils import compression  # type: ignore
from file_processors.utils.aws_secrets_manager import AWSSecretsManager  # type: ignore
from file_processors.utils.aws_ssm import AWS_SSM  # type: ignore
from paramiko.sftp_file import SFTPFile  # type: ignore
from file_processors.utils.capturer import GPGDataCapturer  # type: ignore
from file_processors.utils.symon_exception import SymonException  # type: ignore


@patch('file_processors.utils.aws_ssm.AWS_SSM.get_parameter_value')
def test_update_decryption_key_for_AWS_SSM(mock_get_parameter_value):
    key = 'PRIVATE_KEY'
    decryption_configs = {
        "key_name": "",
        "key_storage_type": "AWS_SSM"
    }

    mock_get_parameter_value.return_value = key
    helper.update_decryption_key(decryption_configs)
    mock_get_parameter_value.assert_called_with(decryption_configs['key_name'])
    assert decryption_configs['key'] == key


@patch('os.environ')
@patch('boto3.session')
@patch('file_processors.utils.aws_secrets_manager.AWSSecretsManager.get_secret')
def test_update_decryption_key_for_AWS_Secrets_Manager(mock_get_secret, mock_boto3, mock_os_environ):
    key = 'PRIVATE_KEY'
    bytes_key = key.encode('ascii')
    passphrase = 'pass'
    secret = {'privateKeyEncoded': base64.b64encode(
        bytes_key).decode('ascii'), 'passphrase': passphrase}
    secure_string = json.dumps(secret)
    decryption_configs = {
        "key_name": "",
        "key_storage_type": "AWS_Secrets_Manager"
    }
    mock_boto3.return_value = Mock()
    mock_os_environ.get.return_value = 'region-1'
    mock_get_secret.return_value = secure_string
    helper.update_decryption_key(decryption_configs)
    mock_get_secret.assert_called_with(decryption_configs['key_name'])
    assert decryption_configs['key'] == bytes_key
    assert decryption_configs['passphrase'] == passphrase


@pytest.mark.parametrize("file_handle", ["../data/fake_file.txt"], indirect=True)
@patch('builtins.open')
@patch('file_processors.utils.compression.infer')
def test_sample_file(mock_compression_infer, mock_open_file, file_handle):
    compressed_file = ''
    src_file_object = None
    mock_compression_infer.return_value = [(compressed_file, file_handle)]
    src_file_name = "test1.csv"
    out_dir = "/test_tmp/bin"
    file_path = helper._safe_local_path(out_dir, src_file_name)
    max_records = 1
    result_file = helper.sample_file(
        src_file_object, src_file_name, out_dir, max_records)
    assert result_file == file_path
    assert mock_open_file.return_value.__enter__().write.call_count == 2


@pytest.mark.parametrize("file_handle_second", ["../data/fake_file.txt"], indirect=True)
@pytest.mark.parametrize("file_handle", ["../data/fake_file.txt"], indirect=True)
@patch('zipfile.ZipFile.__new__')
@patch('builtins.open')
@patch('file_processors.utils.compression.infer')
def test_sample_file_for_compressed_file(mock_compression_infer, mock_open_file, mock_ZipFile, file_handle, file_handle_second):
    compressed_file1 = 'test1.csv'
    compressed_file2 = 'test2.csv'
    src_file_object = None
    mock_compression_infer.return_value = [
        (compressed_file1, file_handle), (compressed_file2, file_handle_second)]
    src_file_name = "Archive.csv.zip"
    out_dir = "/test_tmp/bin"
    file_path = helper._safe_local_path(out_dir, src_file_name)
    max_records = 1
    mock_ZipFile.return_value.__enter__.return_value = Mock()
    result_file = helper.sample_file(
        src_file_object, src_file_name, out_dir, max_records)
    assert result_file == file_path
    assert mock_open_file.return_value.__enter__().write.call_count == 4
    assert mock_ZipFile.return_value.__enter__().writestr.call_count == 2
    mock_ZipFile.return_value.__enter__().writestr.assert_any_call(
        'test1.csv', mock_open_file.return_value.__enter__().read())
    mock_ZipFile.return_value.__enter__().writestr.assert_any_call(
        'test2.csv', mock_open_file.return_value.__enter__().read())


@pytest.mark.parametrize("file_handle_second", ["../data/fake_file.txt"], indirect=True)
@pytest.mark.parametrize("file_handle", ["../data/fake_file.txt"], indirect=True)
@patch('zipfile.ZipFile.__new__')
@patch('builtins.open')
@patch('file_processors.utils.compression.infer')
def test_sample_file_strips_member_path_traversal(mock_compression_infer, mock_open_file, mock_ZipFile, file_handle, file_handle_second):
    mock_compression_infer.return_value = [
        ('../../etc/passwd', file_handle),
        ('nested/dir/data.csv', file_handle_second)]
    out_dir = "/test_tmp/bin"
    mock_ZipFile.return_value.__enter__.return_value = Mock()
    result_file = helper.sample_file(None, "Archive.csv.zip", out_dir, 1)
    assert result_file == helper._safe_local_path(out_dir, "Archive.csv.zip")
    opened_paths = [call.args[0] for call in mock_open_file.call_args_list]
    assert helper._safe_local_path(out_dir, 'passwd') in opened_paths
    assert helper._safe_local_path(out_dir, 'data.csv') in opened_paths
    mock_ZipFile.return_value.__enter__().writestr.assert_any_call(
        'passwd', mock_open_file.return_value.__enter__().read())
    mock_ZipFile.return_value.__enter__().writestr.assert_any_call(
        'data.csv', mock_open_file.return_value.__enter__().read())


def test_safe_filename_uses_basename_only():
    assert helper._safe_filename('../../foo"bar.csv') == 'foo"bar.csv'
    assert helper._safe_filename('nested/dir/data.csv') == 'data.csv'


def test_validate_path_in_directory_allows_normal_filename():
    """WP-33428: a legitimate filename resolving inside the base dir is accepted
    and its real path is returned unchanged in behavior."""
    with tempfile.TemporaryDirectory() as tmp_dir_name:
        candidate = f'{tmp_dir_name}/orders.csv'
        result = helper.validate_path_in_directory(tmp_dir_name, candidate)
        assert result == os.path.realpath(candidate)


def test_validate_path_in_directory_rejects_traversal():
    """WP-33428 (CWE-73): a crafted filename with ../ traversal that escapes the
    intended temp dir is rejected before it can reach open()."""
    with tempfile.TemporaryDirectory() as tmp_dir_name:
        malicious = f'{tmp_dir_name}/../../etc/passwd'
        with pytest.raises(SymonException):
            helper.validate_path_in_directory(tmp_dir_name, malicious)


def test_get_inner_file_extension_for_pgp_file():
    file_path = '/test_tmp/bin/test1.csv.pgp'
    extension = helper.get_inner_file_extension_for_pgp_file(file_path)
    assert extension == '.csv'


@patch('file_processors.utils.decrypt.gpg_decrypt_to_file')
@patch('file_processors.utils.capturer.GPGDataCapturer.__new__')
def test_load_file_decrypted(mock_GPGDataCapturer, mock_gpg_decrypt_to_file):
    src_file_object = mock_open()
    key = 'key'
    sign_key = 'sign_key'
    gnupghome = 'home'
    passphrase = 'pass'
    decrypt_path = '/test_dir/test1.csv'
    max_records = 5
    mocked_capturer = Mock()
    mock_GPGDataCapturer.return_value = mocked_capturer
    helper.load_file_decrypted(
        src_file_object, key, gnupghome, passphrase, decrypt_path, max_records, sign_key)
    mock_gpg_decrypt_to_file.assert_called_with(
        src_file_object, key, gnupghome, passphrase, decrypt_path, mocked_capturer, sign_key)
