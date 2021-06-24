import singer
from singer import metrics, metadata, Transformer, utils, UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING
from datetime import datetime, timezone

LOGGER = singer.get_logger()


def write_schema(catalog, stream_name):
    stream = catalog.get_stream(stream_name)
    schema = stream.schema.to_dict()
    try:
        singer.write_schema(stream_name, schema, stream.key_properties)
    except OSError as err:
        LOGGER.info('OS Error writing schema for: %s', stream_name)
        raise err


def write_record(stream_name, record, time_extracted):
    try:
        singer.write_record(stream_name, record, time_extracted=time_extracted)
    except OSError as err:
        LOGGER.info('OS Error writing record for: %s', stream_name)
        LOGGER.info('record: %s', record)
        raise err


def get_bookmark(state, stream, default):
    if (state is None) or ('bookmarks' not in state):
        return default
    return (
        state
        .get('bookmarks', {})
        .get(stream, default)
    )


def write_bookmark(state, stream, value):
    if 'bookmarks' not in state:
        state['bookmarks'] = {}
    state['bookmarks'][stream] = value
    LOGGER.info('Write state for stream: %s, value: %s', stream, value)
    singer.write_state(state)


def process_records(catalog,
                    stream_name,
                    records,
                    time_extracted,
                    bookmark_field=None,
                    max_bookmark_value=None,
                    last_datetime=None,
                    parent=None,
                    parent_id=None):
    stream = catalog.get_stream(stream_name)
    schema = stream.schema.to_dict()
    stream_metadata = metadata.to_map(stream.metadata)

    max_bookmark_value = datetime.strptime(max_bookmark_value, "%Y-%m-%dT%H:%M:%SZ")
    last_datetime = datetime.strptime(last_datetime, "%Y-%m-%dT%H:%M:%SZ")

    with metrics.record_counter(stream_name) as counter:
        for record in records:
            # If child object, add parent_id to record
            if parent_id and parent:
                record[parent + '_id'] = parent_id

            # Transform record for Singer.io
            with Transformer(integer_datetime_fmt=UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING) as transformer:
                transformed_record = transformer.transform(
                    record,
                    schema,
                    stream_metadata)

                bookmark_dttm = datetime.utcfromtimestamp(int(transformed_record[bookmark_field]))

                # Reset max_bookmark_value to new value if higher
                if (bookmark_field and (bookmark_field in transformed_record)) and (
                        max_bookmark_value is None or
                        bookmark_dttm > max_bookmark_value
                ):
                    max_bookmark_value = bookmark_dttm

                if bookmark_field and (bookmark_field in transformed_record):
                    last_dttm = last_datetime
                    # Keep only records whose bookmark is after the last_datetime
                    if (bookmark_dttm >= last_dttm):
                        write_record(stream_name, transformed_record, time_extracted=time_extracted)
                        counter.increment()
                else:
                    write_record(stream_name, transformed_record, time_extracted=time_extracted)
                    counter.increment()

        return max_bookmark_value, counter.value


# Sync a specific parent or child endpoint.
def sync_endpoint(client,  # pylint: disable=too-many-branches,too-many-statements
                  catalog,
                  state,
                  start_date,
                  stream_name,
                  path,
                  endpoint_config,
                  data_key,
                  static_params,
                  bookmark_query_field=None,
                  bookmark_field=None,
                  id_fields=None,
                  parent=None,
                  parent_id=None):

    # Get the latest bookmark for the stream and set the last_datetime
    last_datetime = get_bookmark(state, stream_name, start_date)
    max_bookmark_value = last_datetime
    LOGGER.info('%s: bookmark last_datetime = %s', stream_name, max_bookmark_value)

    # Initialize child_max_bookmarks
    child_max_bookmarks = {}
    children = endpoint_config.get('children')
    if children:
        for child_stream_name, child_endpoint_config in children.items():
            should_stream, _ = should_sync_stream(get_selected_streams(catalog),
                                                  None,
                                                  child_stream_name)

            if should_stream:
                child_bookmark_field = child_endpoint_config.get('bookmark_field')
                if child_bookmark_field:
                    child_last_datetime = get_bookmark(state, stream_name, start_date)
                    child_max_bookmarks[child_stream_name] = child_last_datetime

    page_size = endpoint_config.get('count', 100)  # Batch size; Number of records per API call, default = 100
    total_records = 0
    page = 1
    params = {
        'page_size': page_size,
        **static_params  # adds in endpoint specific, sort, filter params
    }
    if bookmark_query_field:
        params[bookmark_query_field] = last_datetime
    url = f'https://api.pinterest.com/ads/v3/{path}'
    pagination = True

    while pagination:
        LOGGER.info(f'URL for {stream_name}: {url} -> params: {params.items()}')

        # Get data, API request
        data = client.get(url=url, endpoint=stream_name, params=params)
        # time_extracted: datetime when the data was extracted from the API
        time_extracted = utils.now()

        # Pagination reference:
        # https://developers.pinterest.com/docs/redoc/#tag/Pagination
        # Pagination is done via a cursor that is returned every time we make a request.
        if data.get('bookmark'):
            params.update(dict(bookmark=data['bookmark']))
        else:
            pagination = False

        # Process records and get the max_bookmark_value and record_count for the set of records
        max_bookmark_value, record_count = process_records(
            catalog=catalog,
            stream_name=stream_name,
            records=data['data'],
            time_extracted=time_extracted,
            bookmark_field=bookmark_field,
            max_bookmark_value=max_bookmark_value,
            last_datetime=last_datetime,
            parent=parent,
            parent_id=parent_id)
        LOGGER.info('%s, records processed: %s', stream_name, record_count)
        total_records = total_records + record_count

        # Loop thru parent batch records for each children objects (if should stream)
        if children:
            for child_stream_name, child_endpoint_config in children.items():
                should_stream, _ = should_sync_stream(get_selected_streams(catalog),
                                                      None,
                                                      child_stream_name)
                if should_stream:
                    # For each parent record
                    for record in data['data']:
                        i = 0
                        # Set parent_id
                        for id_field in id_fields:
                            if i == 0:
                                parent_id_field = id_field
                            if id_field == 'id':
                                parent_id_field = id_field
                            i = i + 1
                        parent_id = record.get(parent_id_field)

                        # TODO: debug why the child stream isn't syncing.

                        LOGGER.info('Syncing: %s, parent_stream: %s, parent_id: %s',
                                    child_stream_name,
                                    stream_name,
                                    parent_id)
                        child_path = child_endpoint_config.get('path').format(id=parent_id)
                        child_total_records, child_batch_bookmark_value = sync_endpoint(
                            client=client,
                            catalog=catalog,
                            state=state,
                            start_date=start_date,
                            stream_name=child_stream_name,
                            path=child_path,
                            endpoint_config=child_endpoint_config,
                            data_key=child_endpoint_config.get('data_key', 'elements'),
                            static_params=child_endpoint_config.get('params', {}),
                            bookmark_query_field=child_endpoint_config.get('bookmark_query_field'),
                            bookmark_field=child_endpoint_config.get('bookmark_field'),
                            id_fields=child_endpoint_config.get('id_fields'),
                            parent=child_endpoint_config.get('parent'),
                            parent_id=parent_id)

                        child_batch_bookmark_dttm = datetime.strptime(child_batch_bookmark_value, "%Y-%m-%dT%H:%M:%SZ")
                        child_max_bookmark = child_max_bookmarks.get(child_stream_name)

                        # Handle case where bookmark comes from a unix timestamp OR a datetime string
                        if type(child_max_bookmark) is str and child_max_bookmark.isdigit():
                            child_max_bookmark_dttm = datetime.utcfromtimestamp(child_max_bookmark)
                        else:
                            child_max_bookmark_dttm = datetime.strptime(child_max_bookmark, "%Y-%m-%dT%H:%M:%SZ")

                        if child_batch_bookmark_dttm > child_max_bookmark_dttm:
                            child_batch_bookmark_dttm = child_batch_bookmark_dttm.replace(tzinfo=timezone.utc)
                            child_max_bookmarks[child_stream_name] = datetime.strftime(child_batch_bookmark_dttm, "%Y-%m-%dT%H:%M:%SZ")

                        LOGGER.info('Synced: %s, parent_id: %s, total_records: %s',
                                    child_stream_name,
                                    parent_id,
                                    child_total_records)

        LOGGER.info('%s: Synced page %s, this page: %s. Total records processed: %s',
                    stream_name,
                    page,
                    record_count,
                    total_records)
        page = page + 1

    # Write child bookmarks
    for stream, timestamp in list(child_max_bookmarks.items()):
        write_bookmark(state, stream, timestamp)

    return total_records, datetime.strftime(max_bookmark_value, "%Y-%m-%dT%H:%M:%SZ")


# Review catalog and make a list of selected streams
def get_selected_streams(catalog):
    return [
        stream.tap_stream_id for stream in catalog.streams
        if stream.schema.selected
    ]


# Currently syncing sets the stream currently being delivered in the state.
# If the integration is interrupted, this state property is used to identify
#  the starting point to continue from.
# Reference: https://github.com/singer-io/singer-python/blob/master/singer/bookmarks.py#L41-L46
def update_currently_syncing(state, stream_name):
    if (stream_name is None) and ('currently_syncing' in state):
        del state['currently_syncing']
    else:
        singer.set_currently_syncing(state, stream_name)
    singer.write_state(state)


# Review last_stream (last currently syncing stream), if any,
#  and continue where it left off in the selected streams.
# Or begin from the beginning, if no last_stream, and sync
#  all selected steams.
# Returns should_sync_stream (true/false) and last_stream.
def should_sync_stream(selected_streams, last_stream, stream_name):
    if last_stream == stream_name or last_stream is None:
        if last_stream is not None:
            last_stream = None
        if stream_name in selected_streams:
            return True, last_stream
    return False, last_stream


def sync(client, config, catalog, state):
    if 'start_date' in config:
        start_date = config['start_date']

    selected_streams = get_selected_streams(catalog)
    LOGGER.info('selected_streams: %s', selected_streams)

    if not selected_streams:
        return

    # last_stream = Previous currently synced stream, if the load was interrupted
    last_stream = singer.get_currently_syncing(state)
    LOGGER.info('last/currently syncing stream: %s', last_stream)

    # TODO: Add all endpoints
    # endpoints: API URL endpoints to be called
    endpoints = {
        'advertisers': {
            'path': 'advertisers',
            'account_filter': 'id',
            'params': {
                'owner_user_id': config['owner_user_id'],
                'include_acl': True
            },
            'data_key': 'data',
            'bookmark_field': 'updated_time',
            'id_fields': ['id'],
            'children': {
                'advertisers_campaigns': {
                    'path': 'advertisers/{id}/campaigns',
                    'account_filter': None,
                    'params': {
                        'campaign_status': 'ALL',
                        'managed_status': 'UNMANAGED'  # TODO: Check if param is right.
                    },
                    'data_key': 'data',
                    'bookmark_field': 'updated_time',
                    'id_fields': ['id', 'advertiser_id']
                }
            }
        },
    }

    # For each endpoint (above), determine if the stream should be streamed
    #   (based on the catalog and last_stream), then sync those streams.
    for stream_name, endpoint_config in endpoints.items():
        should_stream, last_stream = should_sync_stream(selected_streams,
                                                        last_stream,
                                                        stream_name)
        if should_stream:
            LOGGER.info('START Syncing: %s', stream_name)
            update_currently_syncing(state, stream_name)
            path = endpoint_config.get('path')
            bookmark_field = endpoint_config.get('bookmark_field')
            write_schema(catalog, stream_name)
            # prevent children schema to be rewrite after each iteration, unstead write it when writing the parent
            children = endpoint_config.get('children')
            if children:
                for child_stream_name, _ in children.items():
                    should_sync, _ = should_sync_stream(selected_streams, None, child_stream_name)
                    if should_sync:
                        write_schema(catalog, child_stream_name)
            total_records, max_bookmark_value = sync_endpoint(
                client=client,
                catalog=catalog,
                state=state,
                start_date=start_date,
                stream_name=stream_name,
                path=path,
                endpoint_config=endpoint_config,
                data_key=endpoint_config.get('data_key', 'elements'),
                static_params=endpoint_config.get('params', {}),
                bookmark_query_field=endpoint_config.get('bookmark_query_field'),
                bookmark_field=bookmark_field,
                id_fields=endpoint_config.get('id_fields'))

            # Write parent bookmarks
            if bookmark_field:
                write_bookmark(state, stream_name, max_bookmark_value)

            update_currently_syncing(state, None)
            LOGGER.info('Synced: %s, total_records: %s',
                        stream_name,
                        total_records)
            LOGGER.info('FINISHED Syncing: %s', stream_name)
