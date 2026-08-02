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
import os


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
    file_path = f'{out_dir}/{src_file_name}'
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
    file_path = f'{out_dir}/{src_file_name}'
    max_records = 1
    mock_ZipFile.return_value.__enter__.return_value = Mock()
    result_file = helper.sample_file(
        src_file_object, src_file_name, out_dir, max_records)
    assert result_file == file_path
    assert mock_open_file.return_value.__enter__().write.call_count == 4
    assert mock_ZipFile.return_value.__enter__().write.call_count == 2


@pytest.mark.parametrize("malicious_name", [
    "../evil.csv",
    "../../etc/passwd",
    "/etc/passwd",
    "subdir/../../evil.csv",
])
@patch('builtins.open')
@patch('file_processors.utils.compression.infer')
def test_sample_file_contains_path_traversal(mock_compression_infer, mock_open_file, malicious_name):
    # WP-33376: a malicious src_file_name containing ../ or an absolute path
    # must not allow writing outside out_dir (CWE-73 path traversal). The
    # untrusted name is reduced to its basename and confined to out_dir.
    src_file_object = None
    mock_compression_infer.return_value = [('', [b"row"])]
    out_dir = "/test_tmp/bin"
    out_dir_real = os.path.realpath(out_dir)
    result_file = helper.sample_file(src_file_object, malicious_name, out_dir, 1)
    # The write path must stay inside out_dir, never escape it.
    assert os.path.commonpath([out_dir_real, result_file]) == out_dir_real
    written_path = mock_open_file.call_args[0][0]
    assert os.path.commonpath([out_dir_real, written_path]) == out_dir_real
    assert ".." not in os.path.relpath(written_path, out_dir_real)


@pytest.mark.parametrize("malicious_compressed_name", [
    "../evil.csv",
    "/etc/passwd",
])
@patch('builtins.open')
@patch('file_processors.utils.compression.infer')
def test_sample_file_contains_path_traversal_in_archive_member(mock_compression_infer, mock_open_file, malicious_compressed_name):
    # WP-33376: a malicious archive member (compressed_name) containing ../ or
    # an absolute path must also be confined to out_dir (CWE-73 path traversal).
    src_file_object = None
    mock_compression_infer.return_value = [
        (malicious_compressed_name, [b"row"])]
    out_dir = "/test_tmp/bin"
    out_dir_real = os.path.realpath(out_dir)
    result_file = helper.sample_file(src_file_object, "Archive.zip", out_dir, 1)
    assert os.path.commonpath([out_dir_real, result_file]) == out_dir_real
    written_path = mock_open_file.call_args[0][0]
    assert os.path.commonpath([out_dir_real, written_path]) == out_dir_real


def test_contain_path_within_dir_keeps_legitimate_name():
    # WP-33376: a legitimate file name still lands inside out_dir under a safe name.
    out_dir = "/test_tmp/bin"
    result = helper.contain_path_within_dir(out_dir, "test1.csv")
    assert result == os.path.join(os.path.realpath(out_dir), "test1.csv")


def test_contain_path_within_dir_neutralizes_traversal():
    # WP-33376: escaping names are reduced to a safe basename inside out_dir.
    out_dir = "/test_tmp/bin"
    out_dir_real = os.path.realpath(out_dir)
    assert helper.contain_path_within_dir(
        out_dir, "../../evil.csv") == os.path.join(out_dir_real, "evil.csv")
    assert helper.contain_path_within_dir(
        out_dir, "/etc/passwd") == os.path.join(out_dir_real, "passwd")


def test_contain_path_within_dir_rejects_degenerate_name():
    # WP-33376: a name that reduces to no basename (e.g. bare '..') is rejected.
    with pytest.raises(SymonException):
        helper.contain_path_within_dir("/test_tmp/bin", "..")
    with pytest.raises(SymonException):
        helper.contain_path_within_dir("/test_tmp/bin", "../")


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
