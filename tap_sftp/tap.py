import json
import sys
import singer  # type: ignore
from singer import utils
from terminaltables import AsciiTable  # type: ignore
from file_processors.utils.stat_collector import FILE_SYNC_STATS  # type: ignore
from tap_sftp import discover
from tap_sftp import sync

REQUIRED_CONFIG_KEYS = ["username", "port", "host", "tables", "start_date"]
REQUIRED_DECRYPT_CONFIG_KEYS = ['key_name']
REQUIRED_COMMON_TABLE_SPEC_CONFIG_KEYS = ["file_type", "search_prefix", "search_pattern"]
REQUIRED_CSV_TABLE_SPEC_CONFIG_KEYS = ["table_name"]

LOGGER = singer.get_logger()


def do_discover(config):
    LOGGER.info("Starting discover")
    streams = discover.discover_streams(config)
    if not streams:
        raise Exception("No streams found")
    catalog = {"streams": streams}
    json.dump(catalog, sys.stdout, indent=2)
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


# Function that updates schema for handling leading zeros being lost when casting column from 
# integer to string. Singer transformer casts the column to first matching type (non null) 
# in type definition array in catalog
# e.g type: ['null', 'integer', 'string] --> casts to integer, and leading zeros lost
# Update the type definition to have null and target type only for columns that are being updated from 
# integer type to string type
def update_schema_for_column_update(config, catalog):
    for stream in catalog.streams:
        stream_name = stream.tap_stream_id
        schema = stream.schema
        columns_to_update = config.get('columns_to_update', {}).get(stream_name, [])

        for column_update_info in columns_to_update:
            # check if we are updating column from number to string (number could be float or integer)
            if (column_update_info['columnUpdateType'] != 'modify' or 
                column_update_info['type'] != 'number' or 
                column_update_info['targetType'] != 'string'):
                continue
            
            column_name = column_update_info['column']
            column_schema = schema.properties.get(column_name, None)

            if column_schema:
                # type definition for the column
                initial_column_type = column_schema.type
                if not isinstance(initial_column_type, list):
                    initial_column_type = [initial_column_type]

                # Check if initally inferred type for the column is integer (to distinguish integer and float)
                # Since singer transformer casts the column to first matching type other than null, 
                # we could only have 'null' before 'integer' in type definition array
                if 'integer' not in initial_column_type or initial_column_type.index('integer') > 1:
                    continue

                # We know 'integer' is in the type definition array with index <= 1. Verify that 'integer' is the
                # first element in the type definition array other than 'null'
                if initial_column_type[0] not in ['null', 'integer']:
                    continue

                target_column_type = ['null', column_update_info['targetType']]
                setattr(column_schema, 'type', target_column_type)


@singer.utils.handle_top_exception(LOGGER)
def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    # validate tables config
    for table in args.config.get('tables'):
        utils.check_config(table, REQUIRED_COMMON_TABLE_SPEC_CONFIG_KEYS)
        file_type = table.get("file_type")
        if file_type in ["csv", "text"]:
            utils.check_config(table, REQUIRED_CSV_TABLE_SPEC_CONFIG_KEYS)

    decrypt_configs = args.config.get('decryption_configs')
    if decrypt_configs:
        # validate decryption configs
        utils.check_config(decrypt_configs, REQUIRED_DECRYPT_CONFIG_KEYS)

    if args.discover:
        do_discover(args.config)
    elif args.catalog or args.properties:
        update_schema_for_column_update(args.config, args.catalog)
        do_sync(args.config, args.catalog, args.state)


if __name__ == "__main__":
    main()
