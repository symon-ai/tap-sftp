import json
import sys
import singer  # type: ignore
from singer import utils
from terminaltables import AsciiTable  # type: ignore
from file_processors.utils.stat_collector import FILE_SYNC_STATS  # type: ignore
from tap_sftp.discover import discover_streams
from tap_sftp.sync import sync_stream

REQUIRED_CONFIG_KEYS = ["username", "port", "host", "tables", "start_date"]
REQUIRED_DECRYPT_CONFIG_KEYS = ['key_name']
REQUIRED_TABLE_SPEC_CONFIG_KEYS = ["table_name", "file_type", "search_prefix", "search_pattern"]

LOGGER = singer.get_logger()


def do_discover(config):
    LOGGER.info("Starting discover")
    streams = discover_streams(config)
    if not streams:
        raise Exception("No streams found")
    catalog = {"streams": streams}
    json.dump(catalog, sys.stdout, indent=2)
    LOGGER.info("Finished discover")


def stream_is_selected(mdata):
    return mdata.get((), {}).get('selected', False)


def do_sync(config, catalog, state):
    collect_sync_stats = config.get("show_stats", False)
    sync_stream(config, catalog, state, collect_sync_stats)

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
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    # validate tables config
    for table in args.config.get('tables'):
        utils.check_config(table, REQUIRED_TABLE_SPEC_CONFIG_KEYS)

    decrypt_configs = args.config.get('decryption_configs')
    if decrypt_configs:
        # validate decryption configs
        utils.check_config(decrypt_configs, REQUIRED_DECRYPT_CONFIG_KEYS)

    if args.discover:
        do_discover(args.config)
    elif args.catalog or args.properties:
        do_sync(args.config, args.catalog, args.state)


if __name__ == "__main__":
    main()
