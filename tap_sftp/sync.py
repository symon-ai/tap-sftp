import singer
from singer import utils, metadata
import itertools
import os
from data_utils.v1.utils import file_format_handler
from data_utils.v1.objects.file_spec import FileSpec
from tap_sftp import client
from tap_sftp import defaults
from concurrent.futures import ThreadPoolExecutor, as_completed
from tap_sftp import helper


LOGGER = singer.get_logger()


def stream_is_selected(mdata):
    return mdata.get((), {}).get('selected', False)


def sync_stream(config, catalog, state):
    sftp_client = client.connection(config)
    stream_groups = itertools.groupby(catalog.streams, key=lambda x: x.stream)
    for key, group in stream_groups:
        streams = list(group)

        ''' Skipping file read if no stream is selected'''
        streams_to_read = next((
            stream for stream in streams if stream_is_selected(metadata.to_map(stream.metadata))), None)
        if not streams_to_read:
            for stream in streams:
                LOGGER.info(f"{stream.tap_stream_id}: Skipping - not selected")
                continue
            return 0

        table_specs = [table_config for table_config in config["tables"] if
                       f"{table_config['search_prefix']}/{table_config['search_pattern']}" == key]
        if len(table_specs) == 0:
            LOGGER.info("No table configuration found for '%s', skipping stream", key)
            return 0
        if len(table_specs) > 1:
            LOGGER.info("Multiple table configurations found for '%s', skipping stream", key)
            return 0
        table_spec = table_specs[0]
        modified_since = utils.strptime_to_utc(config['start_date'])
        search_subdir = config.get("search_subdirectories", True)
        files = sftp_client.get_files(
            table_spec["search_prefix"],
            table_spec["search_pattern"],
            modified_since,
            search_subdir
        )

        if not files:
            sftp_client.close()
            return 0

        max_file_size = config.get("max_file_size", defaults.MAX_FILE_SIZE)
        if any(f['file_size'] / 1024 > max_file_size for f in files):
            raise BaseException(
                f'tap_sftp.max_filesize_error: File size limit exceeded the current limit of{max_file_size / 1024 / 1024} GB.')

        for file in files:
            sync_file(config, file, streams, table_spec, state, modified_since)


def sync_file(config, file, streams, table_spec, state, modified_since):
    file_path = file["filepath"]
    LOGGER.info('Syncing file "%s".', file_path)
    sftp_client = client.connection(config)
    decryption_configs = config.get('decryption_configs')
    file_type = table_spec.get('file_type')
    file_name = os.path.basename(file_path)
    file_extension = helper.get_inner_file_extension_for_pgp_file(file_path)

    if decryption_configs:
        helper.update_decryption_key(decryption_configs)
    file_spec = FileSpec(file_name, file_extension, file_path, file_type)
    with sftp_client.get_file_handle(file, decryption_configs) as file_handle:
        file_format_handler.sync_file_data(file_handle, streams, file_spec, table_spec, state, modified_since)
