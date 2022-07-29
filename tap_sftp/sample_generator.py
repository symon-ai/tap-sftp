from tap_sftp.singer_encodings import compression
from zipfile import ZipFile
from paramiko.sftp_file import SFTPFile
from tap_sftp import decrypt


def generate_sample(src_file_object, src_file_name, out_dir, max_records):
    compressed_iterables = compression.infer(src_file_object, src_file_name)
    generated_files = []
    for item in compressed_iterables:
        local_path = f'{out_dir}/{src_file_name if isinstance(item, SFTPFile) else item.name}'
        with open(local_path, "wb") as out_file:
            record_number = 0
            for line in item:
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


def generate_sample_for_encrypted(src_file_object, src_file_path, decryption_configs, out_dir, max_records):
    return decrypt.gpg_decrypt_from_remote(src_file_object, src_file_path, out_dir,
                                           decryption_configs.get('key'),
                                           decryption_configs.get('gnupghome'),
                                           decryption_configs.get('passphrase'),
                                           max_records)
