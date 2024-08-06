import singer  # type: ignore
import json
import os
import base64
from file_processors.utils import compression  # type: ignore
from zipfile import ZipFile
from file_processors.utils.aws_secrets_manager import AWSSecretsManager  # type: ignore
from file_processors.utils.aws_ssm import AWS_SSM  # type: ignore
from paramiko.sftp_file import SFTPFile  # type: ignore
from file_processors.utils import decrypt  # type: ignore
from file_processors.utils.capturer import GPGDataCapturer  # type: ignore
from file_processors.utils.symon_exception import SymonException  # type: ignore
from tap_sftp import defaults  # type: ignore

LOGGER = singer.get_logger()


def update_decryption_key(decryption_configs):
    storage_type = decryption_configs.get(
        'key_storage_type', 'AWS_Secrets_Manager')
    LOGGER.info(f'Using key storage type "{storage_type}"')
    if storage_type == "AWS_SSM":
        decryption_configs['key'] = AWS_SSM.get_parameter_value(
            decryption_configs.get('key_name'))

    elif storage_type == "AWS_Secrets_Manager":
        secret_manager = AWSSecretsManager(os.environ.get('AWS_REGION'))
        secret = secret_manager.get_secret(decryption_configs.get('key_name'))
        secret_json = json.loads(secret)
        decryption_configs['key'] = base64.b64decode(
            secret_json['privateKeyEncoded'])
        decryption_configs['passphrase'] = secret_json['passphrase']


def get_inner_file_extension_for_pgp_file(file_path):
    file_extension = os.path.splitext(file_path)[1]
    if file_extension in ['.gpg', '.pgp']:
        file_extension = os.path.splitext(os.path.splitext(file_path)[0])[1]
    return file_extension

def get_file(src_file_object, src_file_name, out_dir):
    compressed_iterables = compression.infer(src_file_object, src_file_name)
    generated_files = []
    for compressed_name, compressed_iterators in compressed_iterables:
        local_path = f'{out_dir}/{src_file_name if (isinstance(compressed_iterators, SFTPFile) or not compressed_name) else compressed_name}'
        with open(local_path, "wb") as out_file:
            for line in compressed_iterators:
                out_file.write(line)
            generated_files.append(local_path)

        return local_path
    return ""

def sample_file(src_file_object, src_file_name, out_dir, max_records):
    compressed_iterables = compression.infer(src_file_object, src_file_name)
    generated_files = []
    for compressed_name, compressed_iterators in compressed_iterables:
        local_path = f'{out_dir}/{src_file_name if (isinstance(compressed_iterators, SFTPFile) or not compressed_name) else compressed_name}'
        with open(local_path, "wb") as out_file:
            record_number = 0
            for line in compressed_iterators:
                out_file.write(line)
                record_number += 1
                if record_number > max_records:
                    break
            generated_files.append(local_path)

    if len(generated_files) == 1:
        return generated_files[0]

    final_file = f'{out_dir}/{src_file_name}'
    with ZipFile(final_file, "w") as out_file:
        for path in generated_files:
            out_file.write(path)
    return final_file


def get_custom_metadata(mdata, attribute_name, default_value=''):
    return mdata.get((), {}).get(attribute_name, default_value)


def load_file_decrypted(src_file_object, key, gnupghome, passphrase, decrypt_path, max_records=None):
    capturer = GPGDataCapturer(decrypt_path, max_records)
    return decrypt.gpg_decrypt_to_file(src_file_object, key, gnupghome, passphrase, decrypt_path, capturer)


def validate_file_size(config, decryption_configs, table_spec, files):
    enable_import_file_copy = config.get('enable_import_file_copy', False)

    if (enable_import_file_copy and table_spec.get('file_type').lower() in ["csv", "text"] and decryption_configs is None):
        return

    max_file_size = config.get(
        "max_file_size", defaults.MAX_FILE_SIZE_KB if decryption_configs is None else defaults.MAX_ENCRYPTED_FILE_SIZE_KB)

    if any(f['file_size'] / 1024 > max_file_size for f in files):
        raise SymonException(
            f'Oops! The file size exceeds the current limit of {max_file_size / 1024 / 1024} GB.', 'sftp.MaxFilesizeError')
