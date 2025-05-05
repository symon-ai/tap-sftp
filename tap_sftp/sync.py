import singer  # type: ignore
from singer import utils, metadata
import itertools
from tap_sftp import client
from tap_sftp import defaults
from contextlib import ExitStack
from concurrent.futures import ThreadPoolExecutor, as_completed
from tap_sftp import helper
from file_processors.clients.csv_client import CSVClient  # type: ignore
from file_processors.clients.excel_client import ExcelClient  # type: ignore
from file_processors.clients.fwf_client import FWFClient  # type: ignore
from file_processors.utils.symon_exception import SymonException  # type: ignore
import re

LOGGER = singer.get_logger()


def stream_is_selected(mdata):
    return mdata.get((), {}).get('selected', False)


def sync_stream(config, catalog, state, collect_sync_stats=False):
    files_arr = []

    sftp_client = client.connection(config)
    stream_groups = itertools.groupby(catalog.streams, key=lambda stream: helper.get_custom_metadata(
        singer.metadata.to_map(stream.metadata), 'file_source'))
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

        dynamic = config.get('dynamic')
        # regex match instead of direct equality as search_pattern could get escaped regex chars
        table_specs = [table_config for table_config in config.get('tables') if
                       matches_key(table_config, key, dynamic)]
        if len(table_specs) == 0:
            LOGGER.info(
                "No table configuration found for '%s', skipping stream", key)
            return 0
        if len(table_specs) > 1:
            LOGGER.info(
                "Multiple table configurations found for '%s', skipping stream", key)
            return 0
        table_spec = table_specs[0]
        modified_since = utils.strptime_to_utc(config.get('start_date'))
        search_subdir = config.get("search_subdirectories", True)

        files = sftp_client.get_files(
            table_spec.get("search_prefix"),
            table_spec.get("search_pattern"),
            modified_since,
            search_subdir
        )

        if not files:
            sftp_client.close()
            return 0
        
        helper.validate_file_size(
            config, config.get('decryption_configs'), table_spec, files)

        has_header = table_spec.get('has_header')
        if(config.get('merge')):
            files_arr.append({
                "files": files,
                "streams": streams,
                "table_spec": table_spec,
                "has_header": has_header,
                "modified_since": modified_since
            })
        else:
            for file in files:
                sync_file(config, file, streams, table_spec, state, modified_since, collect_sync_stats, has_header)
    
    if(config.get('merge')):
        print(files_arr)
        sync_multi_file(config, files_arr, state, collect_sync_stats)

def matches_key(table_config, key, dynamic):
    if dynamic is None:
        search_pattern = f"{re.escape(table_config.get('search_prefix'))}/{table_config.get('search_pattern')}"
        matcher = re.compile(search_pattern)
        result = matcher.search(key) != None
    else:
        # file name is predetermined for dynamic import - we only check here to see if file still exist at this point
        search_pattern = f"{re.escape(table_config.get('search_prefix'))}/{table_config.get('table_name')}"
        matcher = re.compile(search_pattern, re.IGNORECASE)
        result = matcher.search(key) != None
    return result


def sync_multi_file(config, files_arr, state, collect_sync_stats):
    file_path = files_arr[0]["files"]["filepath"]
    LOGGER.info('Syncing file "%s".', file_path)
    sftp_client = client.connection(config)
    decryption_configs = config.get('decryption_configs')
    file_type = files_arr[0]["table_spec"].get('file_type').lower()
    log_sync_update = config.get('log_sync_update')
    log_sync_update_interval = config.get('log_sync_update_interval')
    columns_to_update = config.get('columns_to_update')
    columns_to_rename = config.get('columns_to_rename')

    if decryption_configs:
        helper.update_decryption_key(decryption_configs)

    with ExitStack() as stack:
        file_handles = [
            (stack.enter_context(
                sftp_client.get_file_handle(f, files["table_spec"].get('file_type').lower(), files["table_spec"].get('encoding'), decryption_configs)
            ) for f in files["files"]
            )for files in files_arr
        ]
        if file_type in ["csv", "text"]:
            skip_header_row = table_spec.get('skip_header_row', 0)
            skip_footer_row = table_spec.get('skip_footer_row', 0)
            csv_client = CSVClient(file_path, files_arr[0]["table_spec"].get('table_name'), files_arr[0]["table_spec"].get('key_properties', []),
                                   files_arr[0]["has_header"], collect_stats=collect_sync_stats, log_sync_update=log_sync_update,
                                   log_sync_update_interval=log_sync_update_interval, skip_header_row=skip_header_row, skip_footer_row=skip_footer_row)
            csv_client.delimiter = files_arr[0]["table_spec"].get('delimiter') or ","
            csv_client.quotechar = files_arr[0]["table_spec"].get('quotechar') or "\""
            csv_client.encoding = files_arr[0]["table_spec"].get('encoding')
            csv_client.escapechar = files_arr[0]["table_spec"].get('escapechar', '\\')

            csv_client.sync_multi_process(file_handles, [(stream.to_dict() for stream in streams["streams"]) for streams in files_arr], state, files_arr[0]["modified_since"],
                            columns_to_update=columns_to_update)


def sync_file(config, file, streams, table_spec, state, modified_since, collect_sync_stats, has_header):
    file_path = file["filepath"]
    LOGGER.info('Syncing file "%s".', file_path)
    sftp_client = client.connection(config)
    decryption_configs = config.get('decryption_configs')
    file_type = table_spec.get('file_type').lower()
    log_sync_update = config.get('log_sync_update')
    log_sync_update_interval = config.get('log_sync_update_interval')
    columns_to_update = config.get('columns_to_update')
    columns_to_rename = config.get('columns_to_rename')

    if decryption_configs:
        helper.update_decryption_key(decryption_configs)

    with sftp_client.get_file_handle(file, file_type, table_spec.get('encoding'), decryption_configs) as file_handle:
        if file_type in ["csv", "text"]:
            skip_header_row = table_spec.get('skip_header_row', 0)
            skip_footer_row = table_spec.get('skip_footer_row', 0)
            csv_client = CSVClient(file_path, table_spec.get('table_name'), table_spec.get('key_properties', []),
                                   has_header, collect_stats=collect_sync_stats, log_sync_update=log_sync_update,
                                   log_sync_update_interval=log_sync_update_interval, skip_header_row=skip_header_row, skip_footer_row=skip_footer_row)
            csv_client.delimiter = table_spec.get('delimiter') or ","
            csv_client.quotechar = table_spec.get('quotechar') or "\""
            csv_client.encoding = table_spec.get('encoding')
            csv_client.escapechar = table_spec.get('escapechar', '\\')
            csv_client.sync(file_handle, [stream.to_dict() for stream in streams], state, modified_since,
                            columns_to_update=columns_to_update)
        elif file_type in ["excel"]:
            excel_client = ExcelClient(file_path, '', table_spec.get('key_properties', []), has_header,
                                       collect_stats=collect_sync_stats, log_sync_update=log_sync_update,
                                       log_sync_update_interval=log_sync_update_interval)
            excel_client.sync(file_handle, [stream.to_dict()
                              for stream in streams], state, modified_since)
        elif file_type in ["fwf"]:
            skip_header_row = table_spec.get('skip_header_row', 0)
            skip_footer_row = table_spec.get('skip_footer_row', 0)
            column_specs = table_spec.get('column_specs')
            # we require column specs in config since discovery in sftp connector is mandatory
            if column_specs is None or len(column_specs) == 0:
                raise Exception('No column specs found in config.')
            fwf_client = FWFClient(file_path, table_spec.get('table_name'), table_spec.get(
                'key_properties', []), has_header, column_specs=column_specs, skip_header_row=skip_header_row, skip_footer_row=skip_footer_row)
            fwf_client.delimiter = table_spec.get('delimiter', ' ')
            fwf_client.encoding = table_spec.get('encoding')
            fwf_client.sync(
                file_handle,
                [stream.to_dict() for stream in streams],
                state,
                modified_since,
                columns_to_update=columns_to_update,
                columns_to_rename=columns_to_rename
            )
