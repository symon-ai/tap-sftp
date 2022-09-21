import json
import sys

import singer
from singer import metadata, utils
from terminaltables import AsciiTable

from tap_sftp.discover import discover_streams
from tap_sftp.stats import STATS
from tap_sftp.sync import sync_stream

REQUIRED_CONFIG_KEYS = ["username", "port", "host", "tables", "start_date"]
REQUIRED_DECRYPT_CONFIG_KEYS = ['key_name']
REQUIRED_TABLE_SPEC_CONFIG_KEYS = ["table_name", "search_prefix", "search_pattern"]

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
    LOGGER.info('Starting sync.')

    for stream in catalog.streams:
        stream_name = stream.tap_stream_id
        mdata = metadata.to_map(stream.metadata)

        if not stream_is_selected(mdata):
            LOGGER.info("%s: Skipping - not selected", stream_name)
            continue

        singer.write_state(state)
        key_properties = metadata.get(metadata.to_map(stream.metadata), (), "table-key-properties")
        if key_properties is None: 
            key_properties = []
        singer.write_schema(stream_name, stream.schema.to_dict(), key_properties)

        LOGGER.info("%s: Starting sync", stream_name)
        counter_value = sync_stream(config, state, stream)
        LOGGER.info("%s: Completed sync (%s rows)", stream_name, counter_value)

    headers = [['table_name',
                'search prefix',
                'search pattern',
                'file path',
                'row count',
                'last_modified']]

    rows = []

    for table_name, table_data in STATS.items():
        for filepath, file_data in table_data['files'].items():
            rows.append([table_name,
                         table_data['search_prefix'],
                         table_data['search_pattern'],
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
        utils.check_config(table, REQUIRED_TABLE_SPEC_CONFIG_KEYS)

    decrypt_configs = args.config.get('decryption_configs')
    if decrypt_configs:
        # validate decryption configs
        utils.check_config(decrypt_configs, REQUIRED_DECRYPT_CONFIG_KEYS)

    if args.discover:
        do_discover(args.config)
    elif args.catalog or args.properties:
        update_schema_for_column_update(args.config, args.catalog)
        do_sync(args.config, args.catalog, args.state)

if __name__=="__main__":
    main()
