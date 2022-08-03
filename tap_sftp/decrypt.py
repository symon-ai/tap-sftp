import singer
import os
import gnupg
from tap_sftp.capturer import Capturer
LOGGER = singer.get_logger()


def gpg_decrypt_to_file(gpg, src_file_path, decrypted_path, passphrase):
    with open(src_file_path, 'rb') as file_obj:
        decryption_result = gpg.decrypt_file(file_obj, output=decrypted_path, passphrase=passphrase)
        if decryption_result.returncode in [1, 2]:
            raise Exception(f'There was an error while decrypting file. Please verify decryption settings are correct.')
    return decrypted_path


def initialize_gpg(key, gnupghome):
    if not gnupghome:
        gnupghome = f'{os.getcwd()}/gnupg'
        LOGGER.info(f'GPG home folder not provided. using current folder at "{gnupghome}"')
    if not os.path.exists(gnupghome):
        try:
            LOGGER.info(f'GPG home folder does not exist. Creating home folder at "{gnupghome}"')
            os.makedirs(gnupghome)
        except OSError:
            raise Exception(f'Unable to create GNU home directory at "{gnupghome}"')
    gpg = gnupg.GPG(gnupghome=gnupghome)
    import_key_result = gpg.import_keys(key)
    if import_key_result.returncode != 0:
        raise Exception(f"Unable to import decryption key. Please verify the provided key is correct.")
    return gpg


def gpg_decrypt(src_file_path, output_path, key, gnupghome, passphrase):
    gpg_filename = os.path.basename(src_file_path)
    decrypted_filename = os.path.splitext(gpg_filename)[0]
    decrypted_path = f'{output_path}/{decrypted_filename}'

    gpg = initialize_gpg(key, gnupghome)
    return gpg_decrypt_to_file(gpg, src_file_path, decrypted_path, passphrase)


def gpg_decrypt_from_remote(src_file_object, source_file_path, out_dir, key, gnupghome, passphrase, max_records=None):
    gpg_filename = os.path.basename(source_file_path)
    decrypted_filename = os.path.splitext(gpg_filename)[0]
    decrypted_path = f'{out_dir}/{decrypted_filename}'
    capturer = Capturer(decrypted_path, max_records)
    return gpg_decrypt_with_capturer(src_file_object, key, gnupghome, passphrase, capturer)


def gpg_decrypt_with_capturer(src_file_object, key, gnupghome, passphrase, capturer):
    gpg = initialize_gpg(key, gnupghome)
    gpg.on_data = capturer
    decryption_result = gpg.decrypt_file(src_file_object, always_trust=True, passphrase=passphrase)
    if decryption_result.returncode in [1, 2]:
        raise Exception(f'There was an error while decrypting file. Please verify decryption settings are correct.')

    return capturer.out_file_path
