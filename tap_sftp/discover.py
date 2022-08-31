import os.path
import singer
from tap_sftp import client
from tap_sftp import defaults, helper
from data_utils.v1.utils import file_format_handler
from data_utils.v1.objects.file_spec import FileSpec

LOGGER = singer.get_logger()


def discover_streams(config):
    streams = []

    conn = client.connection(config)

    tables = config['tables']
    for table_spec in tables:
        LOGGER.info('Sampling records to determine table JSON schema "%s".', table_spec['table_name'])
        files = conn.get_files(table_spec['search_prefix'], table_spec['search_pattern'],
                               search_subdirectories=False)
        max_file_size = config.get("max_file_size", defaults.MAX_FILE_SIZE)
        if not files:
            return {}
        if any(f['file_size'] / 1024 > max_file_size for f in files):
            raise BaseException(
                f'tap_sftp.max_filesize_error: File size limit exceeded the current limit of{max_file_size / 1024 / 1024} GB.')
        else:
            sorted_files = sorted(files, key=lambda f: f['last_modified'], reverse=True)
            for f in sorted_files:
                file_path = f['filepath']
                file_name = os.path.basename(file_path)
                file_extension = helper.get_inner_file_extension_for_pgp_file(file_path)
                file_type = table_spec.get('file_type')
                decryption_configs = config.get('decryption_configs')
                if decryption_configs:
                    helper.update_decryption_key(decryption_configs)
                file_spec = FileSpec(file_name, file_extension, file_path, file_type)
                with get_file_handle(conn, f, file_extension, decryption_configs, defaults.SAMPLE_SIZE) as file_handle:
                    streams += file_format_handler.generate_file_stream(file_handle, file_spec, table_spec)

    return streams


def get_file_handle(conn, f, file_type, decryption_configs, max_records):
    if file_type.lower() in ["csv", "text"]:
        return conn.get_file_handle_for_sample(f, decryption_configs=decryption_configs,
                                               max_records=max_records)
    else:
        LOGGER.info(f'Downloading entire file to sample file type "{file_type}".')
        return conn.get_file_handle(f, decryption_configs)
