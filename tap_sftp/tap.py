import json
import sys
import singer  # type: ignore
import traceback
from singer import utils
from terminaltables import AsciiTable  # type: ignore
from file_processors.utils.stat_collector import FILE_SYNC_STATS  # type: ignore
from file_processors.utils.np_encoder import NpEncoder  # type: ignore
from file_processors.utils.symon_exception import SymonException # type: ignore
from tap_sftp import discover
from tap_sftp import sync

REQUIRED_CONFIG_KEYS = ["username", "port", "host", "tables", "start_date"]
REQUIRED_DECRYPT_CONFIG_KEYS = ['key_name']
REQUIRED_COMMON_TABLE_SPEC_CONFIG_KEYS = [
    "file_type", "search_prefix", "search_pattern"]
REQUIRED_CSV_TABLE_SPEC_CONFIG_KEYS = ["table_name"]

LOGGER = singer.get_logger()


def do_discover(config):
    LOGGER.info("Starting discover")
    streams = discover.discover_streams(config)
    if not streams:
        raise SymonException('File is empty.', 'EmptyFile')
    catalog = {"streams": streams}
    json.dump(catalog, sys.stdout, indent=2, cls=NpEncoder)
    LOGGER.info("Finished discover")


def stream_is_selected(mdata):
    return mdata.get((), {}).get('selected', False)


def do_sync(config, catalog, state):
    collect_sync_stats = config.get("show_stats", False)
    sync.sync_stream(config, catalog, state, collect_sync_stats)

    if collect_sync_stats:
        headers = [['table_name',
                    'file path',
                    'row count',
                    'last_modified']]

        rows = []

        for table_name, table_data in FILE_SYNC_STATS.items():
            for filepath, file_data in table_data['files'].items():
                rows.append([table_name,
                             filepath,
                             file_data['row_count'],
                             file_data['last_modified']])

        data = headers + rows
        table = AsciiTable(data, title='Extraction Summary')
        LOGGER.info("\n\n%s", table.table)
        LOGGER.info('Done syncing.')


@singer.utils.handle_top_exception(LOGGER)
def main():
    try:
        # used for storing error info to write if error occurs
        error_info = None
        args = utils.parse_args(REQUIRED_CONFIG_KEYS)
        # validate tables config
        for table in args.config.get('tables'):
            utils.check_config(table, REQUIRED_COMMON_TABLE_SPEC_CONFIG_KEYS)
            file_type = table.get("file_type")
            if file_type in ["csv", "text", "fwf"]:
                utils.check_config(table, REQUIRED_CSV_TABLE_SPEC_CONFIG_KEYS)

        decrypt_configs = args.config.get('decryption_configs')
        if decrypt_configs:
            # validate decryption configs
            utils.check_config(decrypt_configs, REQUIRED_DECRYPT_CONFIG_KEYS)

        if args.discover:
            do_discover(args.config)
        elif args.catalog or args.properties:
            do_sync(args.config, args.catalog, args.state)
    except SymonException as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        error_info = {
            'message': traceback.format_exception_only(exc_type, exc_value)[-1],
            'code': e.code,
            'traceback': "".join(traceback.format_tb(exc_traceback))
        }

        if e.details is not None:
            error_info['details'] = e.details
        raise
    except BaseException as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        error_info = {
            'message': traceback.format_exception_only(exc_type, exc_value)[-1],
            'traceback': "".join(traceback.format_tb(exc_traceback))
        }
        raise
    finally:
        if error_info is not None:
            error_file_path = args.config.get('error_file_path', None)
            if error_file_path is not None:
                try:
                    with open(error_file_path, 'w', encoding='utf-8') as fp:
                        json.dump(error_info, fp)
                except:
                    pass
            # log error info as well in case file is corrupted
            error_info_json = json.dumps(error_info)
            error_start_marker = args.config.get('error_start_marker', '[tap_error_start]')
            error_end_marker = args.config.get('error_end_marker', '[tap_error_end]')
            LOGGER.info(f'{error_start_marker}{error_info_json}{error_end_marker}')


if __name__ == "__main__":
    main()
