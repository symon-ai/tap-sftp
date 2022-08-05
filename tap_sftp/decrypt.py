import singer
import os
import gnupg
from tap_sftp.capturer import Capturer
import tempfile
LOGGER = singer.get_logger()


def gpg_decrypt_to_file(src_file_object, key, gnupghome, passphrase, decrypted_path, capturer=None):
    with tempfile.TemporaryDirectory() as tmp_gnupghome:
        gpg = initialize_gpg(key, tmp_gnupghome if not gnupghome else gnupghome)
        if capturer:
            gpg.on_data = capturer
            decryption_result = gpg.decrypt_file(src_file_object, always_trust=True, passphrase=passphrase)
            decrypted_path = capturer.out_file_path
        else:
            decryption_result = gpg.decrypt_file(src_file_object, output=decrypted_path, passphrase=passphrase)
        if decryption_result.returncode in [1, 2]:
            raise Exception(
                f'{"tap_sftp.decryption_key_invalid_error:" if "decryption failed: No secret key" in decryption_result.stderr else "tap_sftp.decryption_failed_error"}: {decryption_result.stderr}')
        return decrypted_path


def initialize_gpg(key, gnupghome):
    if not gnupghome:
        gnupghome = f'{os.getcwd()}/gnupg'
        LOGGER.info(f'GPG home folder not provided. Using current folder at "{gnupghome}"')
    else:
        LOGGER.info(f'GPG home folder: "{gnupghome}"')
    if not os.path.exists(gnupghome):
        try:
            LOGGER.info(f'GPG home folder does not exist. Creating home folder at "{gnupghome}"')
            os.makedirs(gnupghome)
        except OSError:
            raise Exception(f'Unable to create GNU home directory at "{gnupghome}"')
    gpg = gnupg.GPG(gnupghome=gnupghome)
    import_key_result = gpg.import_keys(key)
    if import_key_result.returncode != 0 and not import_key_result.fingerprints:
        raise Exception(f"tap_sftp.decryption_key_invalid_error: {import_key_result.stderr}")
    return gpg


def gpg_decrypt(src_file_path, output_path, key, gnupghome, passphrase):
    gpg_filename = os.path.basename(src_file_path)
    decrypted_filename = os.path.splitext(gpg_filename)[0]
    decrypted_path = f'{output_path}/{decrypted_filename}'
    with open(src_file_path, 'rb') as src_file_object:
        return gpg_decrypt_to_file(src_file_object, key, gnupghome, passphrase, decrypted_path)


def gpg_decrypt_from_remote(src_file_object, source_file_path, out_dir, key, gnupghome, passphrase, max_records=None):
    gpg_filename = os.path.basename(source_file_path)
    decrypted_filename = os.path.splitext(gpg_filename)[0]
    decrypted_path = f'{out_dir}/{decrypted_filename}'
    capturer = Capturer(decrypted_path, max_records)
    return gpg_decrypt_to_file(src_file_object, key, gnupghome, passphrase, capturer.out_file_path, capturer)




