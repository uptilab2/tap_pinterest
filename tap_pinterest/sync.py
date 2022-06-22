import logging
from datetime import date, datetime, timedelta, timezone

import backoff
import singer
from singer import metrics, utils

logger = logging.getLogger(__name__)

LOGGER = singer.get_logger()
API_VERSION = 'v4'
BASE_URL = f'https://api.pinterest.com/ads/{API_VERSION}'


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


def process_records(catalog, stream_name, records, time_extracted,
                    bookmark_field=None,
                    max_bookmark_value=None,
                    last_datetime=None,
                    parent=None,
                    parent_id=None):
    stream = catalog.get_stream(stream_name)
    schema = stream.schema.to_dict()

    with metrics.record_counter(stream_name) as counter:
        for record in records:
            for key in schema['properties']:
                if key not in record:
                    record[key] = None
                elif isinstance(record[key], int) or (isinstance(record[key], str) and record[key].isdigit()):
                    # Cast ints to floats to never have schema issues.
                    record[key] = float(record[key])

            # Remove all entries that are not in the schema. This is used for custom reports.
            record = {key: value for key, value in record.items() if key in schema['properties']}

            # If child object, add parent_id to record
            if parent_id and parent:
                record[parent + '_id'] = parent_id

            if bookmark_field in record:
                bookmark_dttm = datetime.strptime(record[bookmark_field], '%Y-%m-%d')
            else:
                bookmark_dttm = datetime.now()

            if isinstance(max_bookmark_value, str):
                max_bookmark_value = datetime.strptime(max_bookmark_value, "%Y-%m-%dT%H:%M:%SZ")

            # Reset max_bookmark_value to new value if higher
            if (bookmark_field and (bookmark_field in record)) and (max_bookmark_value is None or bookmark_dttm > max_bookmark_value):
                max_bookmark_value = bookmark_dttm

            if bookmark_field and (bookmark_field in record):
                last_dttm = last_datetime
                # Keep only records whose bookmark is after the last_datetime
                if (bookmark_dttm.date() >= last_dttm):
                    write_record(stream_name, record, time_extracted=time_extracted)
                    counter.increment()
            else:
                write_record(stream_name, record, time_extracted=time_extracted)
                counter.increment()

        return max_bookmark_value, counter.value


def get_advertiser_ids(client, url, owner_user_id=None):

    if not owner_user_id:
        res = client.get(f'https://api.pinterest.com/{API_VERSION}/users/me/')
        if res['data']:
            owner_user_id = res['data']['id']
        else:
            return {''}

    res = client.get(url=url, endpoint='advertisers', params=dict(owner_user_id=owner_user_id, include_acl=True))
    return [adveriser['id'] for adveriser in res['data']]


# Sync a specific parent or child endpoint.
def sync_endpoint(client, catalog, state, start_date, stream_name, path, endpoint_config, parent_id=None, custom_reports=None, window_size=0):

    url = f'{BASE_URL}/{path}'

    # Two types of endpoints exist, each with their own logic.
    if (endpoint_config.get('async_report', False)):
        total_records, max_bookmark_value = sync_async_endpoint(
            client,
            catalog,
            state,
            url,
            stream_name,
            start_date,
            endpoint_config,
            parent_id,
            custom_reports,
            window_size
        )
    else:
        total_records, max_bookmark_value = sync_rest_endpoint(
            client,
            catalog,
            state,
            url,
            stream_name,
            start_date,
            endpoint_config,
            parent_id
        )

    return total_records, max_bookmark_value


def sync_rest_endpoint(client, catalog, state, url, stream_name, start_date, endpoint_config, parent_id=None):
    """ Sync endpoints using the traditional REST API method.
    This handles getting the endpoint and pagination.
    """

    # Pagination params
    page_size = endpoint_config.get('count', 100)  # Batch size; Number of records per API call, default = 100
    page = 1

    # Request params
    params = {
        'page_size': page_size,
        **endpoint_config.get('params')  # adds in endpoint specific, sort, filter params
    }

    # Get the latest bookmark for the stream and set the last_datetime
    last_datetime = get_bookmark(state, stream_name, start_date)
    max_bookmark_value = last_datetime
    LOGGER.info(f'{stream_name}: bookmark last_datetime = {max_bookmark_value}')

    bookmark_query_field = endpoint_config.get('bookmark_query_field')
    if bookmark_query_field:
        params[bookmark_query_field] = datetime.strptime(last_datetime, "%Y-%m-%d")

    # Initialize child_max_bookmarks
    child_max_bookmarks = {}
    children = endpoint_config.get('children')
    if children:
        for child_stream_name, child_endpoint_config in children.items():
            should_stream, _ = should_sync_stream(get_selected_streams(catalog), None, child_stream_name)
            if should_stream:
                child_bookmark_field = child_endpoint_config.get('bookmark_field')
                if child_bookmark_field:
                    child_last_datetime = get_bookmark(state, stream_name, start_date)
                    child_max_bookmarks[child_stream_name] = child_last_datetime

    path = f'{BASE_URL}/advertisers'
    if endpoint_config.get('advertiser_ids'):
        advertiser_ids = [a_id.strip() for a_id in endpoint_config['advertiser_ids'].split(',')]
    else:
        advertiser_ids = get_advertiser_ids(client, path, endpoint_config.get('owner_user_id'))

    total_records = 0

    original_url = url
    for advertiser_id in advertiser_ids:

        # Set advertiser ID, if present in string.
        url = original_url.format(advertiser_id=advertiser_id)

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

            data = data[endpoint_config.get('data_key', 'data')]

            # Process records and get the max_bookmark_value and record_count for the set of records
            max_bookmark_value, record_count = process_records(
                catalog=catalog,
                stream_name=stream_name,
                records=data,
                time_extracted=time_extracted,
                bookmark_field=endpoint_config.get('bookmark_field'),
                max_bookmark_value=max_bookmark_value,
                last_datetime=last_datetime,
                parent=endpoint_config.get('parent'),
                parent_id=parent_id)
            LOGGER.info(f'{stream_name}, records processed: {record_count}')
            total_records = total_records + record_count

            # Loop thru parent batch records for each children objects (if should stream)
            if children:
                for child_stream_name, child_endpoint_config in children.items():
                    should_stream, _ = should_sync_stream(get_selected_streams(catalog), None, child_stream_name)
                    if should_stream:
                        # For each parent record
                        for record in data:
                            i = 0
                            # Set parent_id
                            for id_field in endpoint_config.get('id_fields'):
                                if i == 0:
                                    parent_id_field = id_field
                                if id_field == 'id':
                                    parent_id_field = id_field
                                i = i + 1
                            parent_id = record.get(parent_id_field)

                            child_path = child_endpoint_config.get('path').format(id=parent_id)
                            child_total_records, child_batch_bookmark_value = sync_endpoint(
                                client=client,
                                catalog=catalog,
                                state=state,
                                start_date=start_date,
                                stream_name=child_stream_name,
                                path=child_path,
                                endpoint_config=child_endpoint_config,
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

                            LOGGER.info(f'Synced: {child_stream_name}, parent_id: {parent_id}, total_records: {child_total_records}')

            LOGGER.info(f'{stream_name}: Synced page {page}, this page: {record_count}. Total records processed: {total_records}')
            page = page + 1

    # Write child bookmarks
    for stream, timestamp in list(child_max_bookmarks.items()):
        write_bookmark(state, stream, timestamp)

    return total_records, max_bookmark_value


def sync_async_endpoint(client, catalog, state, url, stream_name, start_date, endpoint_config, parent_id=None, custom_reports=None, window_size=0):
    """ Sync endpoints using the fancy ansyc report method.
    https://developers.pinterest.com/docs/redoc/combined_reporting/#operation/ads_v3_create_advertiser_delivery_metrics_report_POST
    https://developers.pinterest.com/docs/redoc/combined_reporting/#tag/reports
    """

    # Request params
    # start_date and end_date are already defined in the endpoints dict
    body = endpoint_config.get('params')

    if custom_reports:
        for custom_report in custom_reports:
            if custom_report['stream'] == stream_name:
                body.update(dict(
                    columns=list(set(custom_report['columns']))
                ))
    else:
        for stream in catalog.streams:
            if stream.name == stream_name:
                body.update(dict(
                    columns=list(set(stream.schema.get('properties', dict()).keys()))
                ))
                break

    # Get the latest bookmark for the stream and set the last_datetime
    last_datetime = get_bookmark(state, stream_name, start_date)
    max_bookmark_value = last_datetime
    LOGGER.info(f'{stream_name}: bookmark last_datetime = {max_bookmark_value}')

    # NOTE: Documentation specifies start_date and end_date cannot be more than 30 days appart.
    segments = []
    now = date.today()

    if type(last_datetime) is str:
        last_datetime = datetime.strptime(last_datetime, "%Y-%m-%dT%H:%M:%SZ")

    last_datetime -= timedelta(days=window_size)

    segment_start = last_datetime.date()
    segment_end = segment_start + timedelta(days=30)
    while segment_end <= now:
        segments.append((segment_start, segment_end))
        segment_start = segment_end + timedelta(days=1)
        segment_end = segment_end + timedelta(days=30)
    if segment_end > now:
        segments.append((segment_start, now))

    path = f'{BASE_URL}/advertisers'
    if endpoint_config.get('advertiser_ids'):
        advertiser_ids = [a_id.strip() for a_id in endpoint_config['advertiser_ids'].split(',')]
    else:
        advertiser_ids = get_advertiser_ids(client, path, endpoint_config.get('owner_user_id'))

    LOGGER.info(f' -- advertiser_ids = {advertiser_ids}')

    total_records = 0

    original_url = url
    for advertiser_id in advertiser_ids:

        # Set advertiser ID, if present in string.
        url = original_url.format(advertiser_id=advertiser_id)

        for start, end in segments:

            LOGGER.info(f' -- Looking up data for advertiser: {advertiser_id} ---- segment: {start} --TO--> {end}')

            body.update(dict(
                start_date=start.strftime("%Y-%m-%d"),
                end_date=end.strftime("%Y-%m-%d")
            ))

            # Create request to generate report
            LOGGER.info(f'URL for {stream_name}: {url} -> body: {body.items()}')
            res = client.post(url=url, endpoint=stream_name, json=body)

            # If the report generates instantly
            if res['data'].get('report_status') == 'FINISHED':
                token = res['data'].get('token')
            else:
                token = retry_report(client, 'post', url, stream_name, json=body, key='token')

            # GET the report data using the token
            LOGGER.info(f'Getting report with token: {token}')
            res = client.get(url=url, endpoint=stream_name, params=dict(token=token))

            # Here we do the same retry.
            # Normally this should never be used as we wait in the first step, but lets be safe.
            if res['data'].get('report_status') == 'FINISHED':
                report_url = res['data'].get('url')
            else:
                report_url = retry_report(client, 'get', url, stream_name, params=dict(token=token), key='url')

            LOGGER.info(f'REPORT URL -> {report_url}')

            # Now that we have the report, we need to dowload the link to the file.
            data = client.download_report(report_url)
            total_records = 0

            if not data:
                LOGGER.info(f' -- No data for report at : {report_url}')

            # time_extracted: datetime when the data was extracted from the API
            time_extracted = utils.now()

            for line in data.values():

                # Process records and get the max_bookmark_value and record_count for the set of records
                max_bookmark_value, record_count = process_records(
                    catalog=catalog,
                    stream_name=stream_name,
                    records=line,
                    time_extracted=time_extracted,
                    bookmark_field=endpoint_config.get('bookmark_field'),
                    max_bookmark_value=max_bookmark_value,
                    last_datetime=start,
                    parent=endpoint_config.get('parent'),
                    parent_id=parent_id)
                LOGGER.info(f'{stream_name}: Synced report. Total records processed: {record_count}')
                total_records = total_records + record_count

    return total_records, max_bookmark_value


class TokenNotReadyException(Exception):
    pass


@backoff.on_exception(backoff.expo, TokenNotReadyException, max_time=120, factor=2)
def retry_report(client, method, url, stream_name, key='token', **kwargs):
    # Get status of report generating proccess
    LOGGER.info(f' -- REPORT NOT READY -> retrying: {url} -> {kwargs.items()}')
    if method == 'post':
        res = client.post(url=url, endpoint=stream_name, **kwargs)
    else:
        res = client.get(url=url, endpoint=stream_name, **kwargs)

    # If the report generates instantly
    if res['data'].get('report_status') == 'FINISHED':
        return res['data'].get(key)
    else:
        LOGGER.info(f' -- -- REPORT STATUS: {res["data"].get("report_status")}')
        raise TokenNotReadyException


# Review catalog and make a list of selected streams
def get_selected_streams(catalog, custom_reports=[]):
    return [
        stream.tap_stream_id for stream in catalog.streams
        if stream.schema.selected or stream.tap_stream_id in [custom_report['stream'] for custom_report in custom_reports]
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
    date = datetime.strptime(config['start_date'], "%Y-%m-%dT%H:%M:%SZ")
    date_string = datetime.strftime(date, "%Y-%m-%d")

    selected_streams = get_selected_streams(catalog, config.get('custom_report'))
    LOGGER.info(f'selected_streams: {selected_streams}')

    if not selected_streams:
        return

    # last_stream = Previous currently synced stream, if the load was interrupted
    last_stream = singer.get_currently_syncing(state)
    LOGGER.info(f'last/currently syncing stream: {last_stream}')

    # endpoints: API URL endpoints to be called
    endpoints = {
        'advertisers': {
            'path': 'advertisers',
            'params': {
                'owner_user_id': config.get('owner_user_id'),
                'include_acl': True
            },
            'bookmark_field': 'updated_time',
        },
        'advertisers_campaigns': {
            'path': 'advertisers/{advertiser_id}/campaigns',
            'params': {
                'campaign_status': 'ALL',
                'managed_status': 'ALL'
            },
            'data_key': 'data',
            'bookmark_field': 'updated_time',
            'id_fields': ['id'],
            'advertiser_ids': config.get('advertiser_ids'),
            'owner_user_id': config.get('owner_user_id'),
            'children': {
                # 'campaign_ad_groups': {  # TODO: Replace this with advertiser_ad_groups, if possible.
                #    'path': 'campaigns/{id}/ad_groups',
                #    'data_key': 'data',
                #    'bookmark_field': 'updated_time',
                #    'id_fields': ['id']
                # }
            }
        },
        'advertiser_delivery_metrics': {
            # https://developers.pinterest.com/docs/redoc/combined_reporting/#operation/ads_v3_create_advertiser_delivery_metrics_report_POST
            'path': 'advertisers/{advertiser_id}/delivery_metrics/async',
            'account_filter': None,
            'advertiser_ids': config.get('advertiser_ids'),
            'owner_user_id': config.get('owner_user_id'),
            'params': {
                'granularity': 'DAY',  # This returns one record per day, no need to iterate on days like some other taps
                'level': 'ADVERTISER',
            },
            'bookmark_field': 'DATE',
            'id_fields': ['ADVERTISER_ID'],
            'async_report': True
        },
        'campaign_delivery_metrics': {
            # https://developers.pinterest.com/docs/redoc/combined_reporting/#operation/ads_v3_create_advertiser_delivery_metrics_report_POST
            'path': 'advertisers/{advertiser_id}/delivery_metrics/async',
            'advertiser_ids': config.get('advertiser_ids'),
            'owner_user_id': config.get('owner_user_id'),
            'params': {
                'granularity': 'DAY',  # This returns one record per day, no need to iterate on days like some other taps
                'level': 'CAMPAIGN',
            },
            'bookmark_field': 'DATE',
            'id_fields': ['CAMPAIGN_ID'],
            'async_report': True
        },
        'ad_group_delivery_metrics': {
            # https://developers.pinterest.com/docs/redoc/combined_reporting/#operation/ads_v3_create_advertiser_delivery_metrics_report_POST
            'path': 'advertisers/{advertiser_id}/delivery_metrics/async',
            'advertiser_ids': config.get('advertiser_ids'),
            'owner_user_id': config.get('owner_user_id'),
            'params': {
                'granularity': 'DAY',  # This returns one record per day, no need to iterate on days like some other taps
                'level': 'AD_GROUP',
            },
            'bookmark_field': 'DATE',
            'id_fields': ['CAMPAIGN_ID'],
            'async_report': True
        },
        'pin_promotion_delivery_metrics': {
            # https://developers.pinterest.com/docs/redoc/combined_reporting/#operation/ads_v3_create_advertiser_delivery_metrics_report_POST
            'path': 'advertisers/{advertiser_id}/delivery_metrics/async',
            'advertiser_ids': config.get('advertiser_ids'),
            'owner_user_id': config.get('owner_user_id'),
            'params': {
                'granularity': 'DAY',  # This returns one record per day, no need to iterate on days like some other taps
                'level': 'PIN_PROMOTION',
            },
            'bookmark_field': 'DATE',
            'id_fields': ['CAMPAIGN_ID'],
            'async_report': True
        },
    }

    # For each endpoint (above), determine if the stream should be streamed
    #   (based on the catalog and last_stream), then sync those streams.
    for stream_name, endpoint_config in endpoints.items():
        should_stream, last_stream = should_sync_stream(selected_streams, last_stream, stream_name)
        if should_stream:
            LOGGER.info(f'START Syncing: {stream_name} for date {date_string}')
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

            if endpoint_config.get('async_report'):
                if config.get('attribution_types'):
                    endpoint_config['params']['attribution_types'] = [string.strip() for string in config['attribution_types'].split(',')]

                if config.get('conversion_report_time'):
                    endpoint_config['params']['conversion_report_time'] = config['conversion_report_time']

                if config.get('click_window_days'):
                    endpoint_config['params']['click_window_days'] = f"DAYS_{config['click_window_days']}"

                if config.get('engagement_window_days'):
                    endpoint_config['params']['engagement_window_days'] = f"DAYS_{config['engagement_window_days']}"

                if config.get('view_window_days'):
                    endpoint_config['params']['view_window_days'] = f"DAYS_{config['view_window_days']}"

            if config.get('window_size').isnumeric():
                window_size = int(config['window_size'])
            else:
                window_size = 0

            total_records, max_bookmark_value = sync_endpoint(
                client=client,
                catalog=catalog,
                state=state,
                start_date=datetime.strptime(config.get('start_date'), "%Y-%m-%dT%H:%M:%SZ"),
                stream_name=stream_name,
                path=path,
                endpoint_config=endpoint_config,
                custom_reports=config.get('custom_report'),
                window_size=window_size
            )

            # Write parent bookmarks
            if bookmark_field:
                if type(max_bookmark_value) is datetime:
                    max_bookmark_value = max_bookmark_value.strftime("%Y-%m-%dT%H:%M:%SZ")
                write_bookmark(state, stream_name, max_bookmark_value)

            update_currently_syncing(state, None)
            LOGGER.info(f'Synced: {stream_name}, total_records: {total_records}')
            LOGGER.info(f'FINISHED Syncing: {stream_name}')
