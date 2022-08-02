''' This is a file with geenric helper functions for working with catalog, state and custom report objects'''
import singer

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
    return state.get('bookmarks', {}).get(stream, default)


def write_bookmark(state, stream, value):
    if 'bookmarks' not in state:
        state['bookmarks'] = {}
    state['bookmarks'][stream] = value
    LOGGER.info('Write state for stream: %s, value: %s', stream, value)
    singer.write_state(state)


def get_selected_columns(custom_reports, catalog, stream_name):
    '''Get list of columns selected for a stream from custom reports or return complete list of availible columns from catalog entry'''

    res = []
    if custom_reports:
        custom_report = [c for c in custom_reports if c['stream'] == stream_name]
        if custom_report:
            res=list(set(custom_report[0]['columns']))
    else:
        stream = [s for s in catalog.streams if s.stream == stream_name][0]
        res=list(set(stream.schema.to_dict()['properties'].keys()))

    return res


def get_selected_streams(catalog, custom_reports=[]):
    '''Review catalog and make a list of selected streams'''
    return [
        stream.tap_stream_id for stream in catalog.streams
        if stream.schema.selected or stream.tap_stream_id in [custom_report['stream'] for custom_report in custom_reports]
    ]


def update_currently_syncing(state, stream_name):
    ''' Currently syncing sets the stream currently being delivered in the state.
        If the integration is interrupted, this state property is used to identify the starting point to continue from.
        Reference: https://github.com/singer-io/singer-python/blob/master/singer/bookmarks.py#L41-L46
    '''
    if (stream_name is None) and ('currently_syncing' in state):
        del state['currently_syncing']
    else:
        singer.set_currently_syncing(state, stream_name)
    singer.write_state(state)


def should_sync_stream(selected_streams, last_stream, stream_name):
    ''' Review last_stream (last currently syncing stream), if any, and continue where it left off in the selected streams.
        Or begin from the beginning, if no last_stream, and sync all selected steams. Returns should_sync_stream (true/false) and last_stream. 
    '''
    if last_stream == stream_name or last_stream is None:
        if last_stream is not None:
            last_stream = None
        if stream_name in selected_streams:
            return True, last_stream
    return False, last_stream
