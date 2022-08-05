import singer
from singer import Transformer, metadata, utils
from tap_sftp import client, stats
from tap_sftp.singer_encodings import csv_handler
from tap_sftp import defaults
from concurrent.futures import ThreadPoolExecutor, as_completed
from tap_sftp import helper


LOGGER = singer.get_logger()


def sync_ftp(sftp_file, stream, table_spec, config, state, table_name):
    records_streamed = sync_file(sftp_file, stream, table_spec, config)
    state = singer.write_bookmark(state, table_name, 'modified_since', sftp_file['last_modified'].isoformat())
    singer.write_state(state)
    return records_streamed


def sync_stream(config, state, stream):
    table_name = stream.tap_stream_id
    modified_since = utils.strptime_to_utc(singer.get_bookmark(state, table_name, 'modified_since') or
                                           config['start_date'])
    search_subdir = config.get("search_subdirectories", True)

    LOGGER.info('Syncing table "%s".', table_name)
    LOGGER.info('Getting files modified since %s.', modified_since)

    sftp_client = client.connection(config)
    table_spec = [table_config for table_config in config["tables"] if table_config["table_name"] == table_name]
    if len(table_spec) == 0:
        LOGGER.info("No table configuration found for '%s', skipping stream", table_name)
        return 0
    if len(table_spec) > 1:
        LOGGER.info("Multiple table configurations found for '%s', skipping stream", table_name)
        return 0
    table_spec = table_spec[0]

    files = sftp_client.get_files(
        table_spec["search_prefix"],
        table_spec["search_pattern"],
        modified_since,
        search_subdir
    )
    sftp_client.close()

    LOGGER.info('Found %s files to be synced.', len(files))

    records_streamed = 0
    if not files:
        return records_streamed

    max_file_size = config.get("max_file_size", defaults.MAX_FILE_SIZE)
    if any(f['file_size']/1024 > max_file_size for f in files):
        raise BaseException(f'tap_sftp.max_filesize_error: File size limit exceeded the current limit of{max_file_size/1024/1024} GB.')

    with ThreadPoolExecutor(max_workers=8) as executor:
        future_sftp = {executor.submit(sync_ftp, sftp_file, stream, table_spec, config, state, table_name): sftp_file for sftp_file in files}
        for future in as_completed(future_sftp):
            records_streamed += future.result()

    LOGGER.info('Wrote %s records for table "%s".', records_streamed, table_name)

    return records_streamed


def sync_file(sftp_file_spec, stream, table_spec, config):
    LOGGER.info('Syncing file "%s".', sftp_file_spec["filepath"])
    sftp_client = client.connection(config)
    decryption_configs = config.get('decryption_configs')
    if decryption_configs:
        helper.update_decryption_key(decryption_configs)

    with sftp_client.get_file_handle(sftp_file_spec, decryption_configs) as file_handle:
        if decryption_configs:
            sftp_file_spec['filepath'] = file_handle.name

        # Add file_name to opts and flag infer_compression to support gzipped files
        opts = {'key_properties': table_spec.get('key_properties', []),
                'delimiter': table_spec.get('delimiter', ','),
                'quotechar': table_spec.get('quotechar', '"'),
                'file_name': sftp_file_spec['filepath'],
                'encoding': table_spec.get('encoding', 'utf-8')}

        readers = csv_handler.get_row_iterators(file_handle, options=opts, infer_compression=True)

        records_synced = 0

        for reader in readers:
            LOGGER.info('Synced Record Count: 0')
            with Transformer() as transformer:
                for row in reader:
                    custom_columns = {
                        # Uncomment if required custom fields
                        # '_sdc_source_file': sftp_file_spec["filepath"],
                        #
                        # # index zero, +1 for header row
                        # '_sdc_source_lineno': records_synced + 2
                    }
                    rec = {**row, **custom_columns}

                    to_write = transformer.transform(rec, stream.schema.to_dict(), metadata.to_map(stream.metadata))

                    singer.write_record(stream.tap_stream_id, to_write)
                    records_synced += 1
                    if records_synced % 100000 == 0:
                        LOGGER.info(f'Synced Record Count: {records_synced}')
            LOGGER.info(f'Sync Complete - Records Synced: {records_synced}')

    stats.add_file_data(table_spec, sftp_file_spec['filepath'], sftp_file_spec['last_modified'], records_synced)

    if config.get('delete_after_sync'):
        sftp_client.sftp.remove(sftp_file_spec["filepath"])
        LOGGER.info(f"Deleting remote file: {sftp_file_spec['filepath']}")
    sftp_client.close()
    del sftp_client

    return records_synced
