import singer
from tap_sftp.aws_secrets_manager import AWSSecretsManager
from tap_sftp.aws_ssm import AWS_SSM
import json
import os
import base64

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
