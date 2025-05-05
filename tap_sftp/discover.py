import singer  # type: ignore
from tap_sftp import client
from tap_sftp import defaults, helper
from file_processors.clients.csv_client import CSVClient  # type: ignore
from file_processors.clients.excel_client import ExcelClient  # type: ignore
from file_processors.clients.fwf_client import FWFClient  # type: ignore

LOGGER = singer.get_logger()


def discover_streams(config):
    streams = []
    conn = client.connection(config)
    decryption_configs = config.get('decryption_configs')
    if decryption_configs:
        helper.update_decryption_key(decryption_configs)
    merged_opt = config.get('merge')
    check_schema = None

    tables = config.get('tables')
    for table_spec in tables:
        LOGGER.info('Sampling records to determine table JSON schema "%s".',
                    table_spec.get('table_name'))
        has_header = table_spec.get('has_header')
        files = conn.get_files(table_spec.get('search_prefix'), table_spec.get('search_pattern'),
                               search_subdirectories=False)

        helper.validate_file_size(
            config, decryption_configs, table_spec, files)

        sorted_files = sorted(
            files, key=lambda f: f['last_modified'], reverse=True)
        for f in sorted_files:
            file_path = f['filepath']
            file_type = table_spec.get('file_type').lower()
            if file_type in ["csv", "text"]:
                table_name = table_spec.get('table_name')
                skip_header_row = table_spec.get('skip_header_row', 0)
                skip_footer_row = table_spec.get('skip_footer_row', 0)
                # update sample size for get_file_handle_for_sample to write SAMPLE_SIZE rows excluding skipped rows
                sample_size = defaults.SAMPLE_SIZE + skip_header_row + skip_footer_row
                with conn.get_file_handle_for_sample(f, file_type, table_spec.get('encoding'), decryption_configs, sample_size) as file_handle:
                    csv_client = CSVClient(file_path, '',
                                           table_spec.get('key_properties', []), has_header, skip_header_row=skip_header_row, skip_footer_row=skip_footer_row)
                    csv_client.delimiter = table_spec.get('delimiter', ',')
                    csv_client.quotechar = table_spec.get('quotechar', '"')
                    csv_client.encoding = table_spec.get('encoding')
                    streams += csv_client.build_streams(
                        file_handle, defaults.SAMPLE_SIZE, tap_stream_id=table_name)
            elif file_type in ["excel"]:
                with conn.get_file_handle(f, file_type, table_spec.get('encoding'), decryption_configs) as file_handle:
                    excel_client = ExcelClient(file_path, '', table_spec.get(
                        'key_properties', []), has_header)
                    streams += excel_client.build_streams(file_handle, defaults.SAMPLE_SIZE,
                                                          worksheets=table_spec.get('worksheets', []))
            elif file_type in ["fwf"]:
                table_name = table_spec.get('table_name')
                with conn.get_file_handle_for_sample(f, file_type, table_spec.get('encoding'), None, defaults.SAMPLE_SIZE) as file_handle:
                    skip_header_row = table_spec.get('skip_header_row', 0)
                    skip_footer_row = table_spec.get('skip_footer_row', 0)
                    fwf_client = FWFClient(file_path, '', table_spec.get(
                'key_properties', []), has_header, skip_header_row=skip_header_row, skip_footer_row=skip_footer_row)
                    fwf_client.delimiter = table_spec.get('delimiter', ' ')
                    fwf_client.encoding = table_spec.get('encoding')
                    streams += fwf_client.build_streams(
                        file_handle, defaults.SAMPLE_SIZE, tap_stream_id=table_name)
            else:
                raise BaseException(
                    f'file_type_error: Unsupported file type "{file_type}"')

        if merged_opt:
            if check_schema == None:
                check_schema = streams[-1]["schema"]
            else:
                merged_opt = helper.check_merge(check_schema, streams[-1]["schema"])
                if merged_opt:
                    check_schema = streams[-1]
                else:
                    raise BaseException(
                        f'file_type_error: Merge requires same schema and columns')

    return streams
