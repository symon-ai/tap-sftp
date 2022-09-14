import singer  # type: ignore
import json
import os
import base64
from file_processors.utils import compression  # type: ignore
from zipfile import ZipFile
from file_processors.utils.aws_secrets_manager import AWSSecretsManager  # type: ignore
from file_processors.utils.aws_ssm import AWS_SSM  # type: ignore
from paramiko.sftp_file import SFTPFile  # type: ignore
from file_processors.utils import decrypt
from file_processors.utils.capturer import GPGDataCapturer  # type: ignore

LOGGER = singer.get_logger()


def update_decryption_key(decryption_configs):
    storage_type = decryption_configs.get('key_storage_type', 'AWS_Secrets_Manager')
    LOGGER.info(f'Using key storage type "{storage_type}"')
    if storage_type == "AWS_SSM":
        decryption_configs['key'] = AWS_SSM.get_decryption_key(decryption_configs.get('key_name'))

    elif storage_type == "AWS_Secrets_Manager":
        secret_manager = AWSSecretsManager(os.environ.get('AWS_REGION'))
        secret = secret_manager.get_secret(decryption_configs.get('key_name'))
        secret_json = json.loads(secret)
        decryption_configs['key'] = base64.b64decode(secret_json['privateKeyEncoded'])
        decryption_configs['passphrase'] = secret_json['passphrase']


def get_inner_file_extension_for_pgp_file(file_path):
    file_extension = os.path.splitext(file_path)[1]
    if file_extension in ['.gpg', '.pgp']:
        file_extension = os.path.splitext(os.path.splitext(file_path)[0])[1]
    return file_extension


def sample_file(src_file_object, src_file_name, out_dir, max_records):
    compressed_iterables = compression.infer(src_file_object, src_file_name)
    generated_files = []
    for compressed_name, compressed_iterators in compressed_iterables:
        local_path = f'{out_dir}/{src_file_name if isinstance(compressed_iterators, SFTPFile) else compressed_name}'
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


def load_file_decrypted(src_file_object, key, gnupghome, passphrase, decrypt_path, max_records=None):
    capturer = GPGDataCapturer(decrypt_path, max_records)
    return decrypt.gpg_decrypt_to_file(src_file_object, key, gnupghome, passphrase, decrypt_path, capturer)
