import singer
from tap_sftp.aws_secrets_manager import AWSSecretsManager
from tap_sftp.aws_ssm import AWS_SSM
import json
import os
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
        decryption_configs['key'] = secret_json['privatekey'][:-800]
        decryption_configs['passphrase'] = secret_json['passphrase']
    else:
        raise Exception(f'Storage type "{storage_type}" not supported')
