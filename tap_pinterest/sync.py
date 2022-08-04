
from datetime import date, datetime, timedelta
import singer
from singer import metrics, utils
from tap_pinterest.schema import STREAMS, BASE_URL
import tap_pinterest.helpers as helpers


LOGGER = singer.get_logger()


def decode_bookmark_field(bookmark_field):
    '''Return a datetime object from either a yyyy-dd-mm string or unix timestamp'''

    if isinstance(bookmark_field, str): # TODO a lot of type-checking in this file, will be nice to refactor later
        return datetime.strptime(bookmark_field, '%Y-%m-%d')
    elif isinstance(bookmark_field, float):
        return datetime.fromtimestamp(bookmark_field).replace(hour=0,minute=0,second=0) 
    else:
        return None


def sync(client, config, catalog, state):
    date = datetime.strptime(config['start_date'], "%Y-%m-%dT%H:%M:%SZ")
    date_string = datetime.strftime(date, "%Y-%m-%d")

    selected_streams = helpers.get_selected_streams(catalog, config.get('custom_report'))
    LOGGER.info(f'selected_streams: {selected_streams}')

    if not selected_streams:
        return

    # last_stream = Previous currently synced stream, if the load was interrupted
    last_stream = singer.get_currently_syncing(state)
    LOGGER.info(f'last/currently syncing stream: {last_stream}')

    # For each endpoint (above), determine if the stream should be streamed
    #   (based on the catalog and last_stream), then sync those streams.
    for stream in STREAMS:
        should_stream, last_stream = helpers.should_sync_stream(selected_streams, last_stream, stream.name)
        if should_stream:
            LOGGER.info(f'START Syncing: {stream.name} for date {date_string}')
            helpers.update_currently_syncing(state, stream.name)
            path = stream.path
            bookmark_field = stream.bookmark_field
            helpers.write_schema(catalog, stream.name)

            if stream.async_report:
                if config.get('attribution_types'):
                    stream.params['attribution_types'] = [string.strip() for string in config['attribution_types'].split(',')]

                if config.get('conversion_report_time'):
                    stream.params['conversion_report_time'] = config['conversion_report_time']

                if config.get('click_window_days'):
                    stream.params['click_window_days'] = int(config['click_window_days'])

                if config.get('engagement_window_days'):
                    stream.params['engagement_window_days'] = int(config['engagement_window_days'])

                if config.get('view_window_days'):
                    stream.params['view_window_days'] = int(config['view_window_days'])

            if config.get('window_size').isnumeric():
                window_size = int(config['window_size'])
            else:
                window_size = 0
            
            if config.get('advertiser_ids'):
                advertiser_ids = [a_id.strip() for a_id in config.get('advertiser_ids').split(',')]
            else:
                advertiser_ids = client.get_advertiser_ids()
            LOGGER.info(f'Working with  advertiser_ids: {advertiser_ids}')

            total_records, max_bookmark_value = sync_endpoint(
                client=client,
                catalog=catalog,
                state=state,
                start_date=datetime.strptime(config.get('start_date'), "%Y-%m-%dT%H:%M:%SZ"),
                path=path,
                stream = stream,
                custom_reports=config.get('custom_report'),
                window_size=window_size,
                advertiser_ids = advertiser_ids
            )

            # Write bookmarks
            if bookmark_field:
                if type(max_bookmark_value) is datetime:
                    max_bookmark_value = max_bookmark_value.strftime("%Y-%m-%dT%H:%M:%SZ")
                helpers.write_bookmark(state, stream.name, max_bookmark_value)

            helpers.update_currently_syncing(state, None)
            LOGGER.info(f'Synced: {stream.name}, total_records: {total_records}')
            LOGGER.info(f'FINISHED Syncing: {stream.name}')


def sync_endpoint(client, catalog, state, start_date, path, stream, custom_reports, window_size, advertiser_ids):
    '''Sync a specific endpoint.'''

    url = f'{BASE_URL}/{path}'

    # Two types of endpoints exist, each with their own logic.
    if stream.async_report:
        total_records, max_bookmark_value = sync_async_endpoint(
            client,
            catalog,
            state,
            url,
            start_date,
            stream,
            custom_reports,
            window_size,
            advertiser_ids
        )
    else:
        total_records, max_bookmark_value = sync_rest_endpoint(
            client,
            catalog,
            state,
            url,
            start_date,
            stream,
            advertiser_ids
        )

    return total_records, max_bookmark_value


def sync_rest_endpoint(client, catalog, state, url, start_date, stream, advertiser_ids):
    """ Sync endpoints using the traditional REST API method.
    This handles getting the endpoint and pagination.
    """

    # Request params
    params = stream.params
    if 'page_size' not in params:
        params['page_size'] = 100

    # Get the latest bookmark for the stream and set the last_datetime
    last_datetime = helpers.get_bookmark(state, stream.name, start_date)
    max_bookmark_value = last_datetime
    LOGGER.info(f'{stream.name}: bookmark last_datetime = {max_bookmark_value}')

    # Customise the url with ad_account ids
    custom_urls = set([url.format(advertiser_id=advertiser_id) for advertiser_id in advertiser_ids ])
    total_records, current_page = 0, 0

    for url in custom_urls:
        pagination = True
        while pagination:
            current_page+=1
            LOGGER.info(f'URL for {stream.name}: {url} -> params: {params.items()}')

            # Get data, API request
            response = client.get(url=url, endpoint=stream.name, params=params)
            # time_extracted: datetime when the data was extracted from the API
            time_extracted = utils.now()

            # Pagination reference:
            # https://developers.pinterest.com/docs/redoc/#tag/Pagination
            # Pagination is done via a cursor that is returned every time we make a request.
            if response.get('bookmark'):
                params.update(dict(bookmark=response['bookmark']))
            else:
                pagination = False

            data = response[stream.data_key]

            # Process records and get the max_bookmark_value and record_count for the set of records
            max_bookmark_value, record_count = process_records(
                catalog=catalog,
                stream_name=stream.name,
                records=data,
                time_extracted=time_extracted,
                bookmark_field=stream.bookmark_field,
                max_bookmark_value=max_bookmark_value,
                last_datetime=last_datetime)

            total_records += record_count
            LOGGER.info(f'{stream.name}: Synced page number {current_page}, this page contains: {record_count} records. Total records processed: {total_records}')

    return total_records, max_bookmark_value


def sync_async_endpoint(client, catalog, state, url, start_date, stream, custom_reports, window_size, advertiser_ids):
    """ Sync endpoints using the fancy ansyc report method.
    https://developers.pinterest.com/docs/redoc/combined_reporting/#operation/ads_v3_create_advertiser_delivery_metrics_report_POST
    https://developers.pinterest.com/docs/redoc/combined_reporting/#tag/reports
    """

    # Get the latest bookmark for the stream and set the last_datetime
    last_datetime = helpers.get_bookmark(state, stream.name, start_date)
    LOGGER.info(f'{stream.name}: bookmark last_datetime = {last_datetime}')
    
    max_bookmark_value = last_datetime
    last_datetime -= timedelta(days=window_size)

    # NOTE: Documentation specifies start_date and end_date cannot be more than 30 days appart.
    segments = []
    now = date.today()
    segment_start = last_datetime.date()
    segment_end = segment_start + timedelta(days=30)
    while segment_end <= now:
        segments.append((segment_start, segment_end))
        segment_start = segment_end + timedelta(days=1)
        segment_end = segment_end + timedelta(days=30)
    if segment_end > now:
        segments.append((segment_start, now))

    # Request params
    # start_date and end_date are already defined in the endpoints dict
    body = stream.params
    body['columns']= helpers.get_selected_columns(custom_reports, catalog, stream.name)

    # in case the 'date' fields was among the selected ones, do not include it in the list of metrics
    if 'DATE' in body['columns']: 
        body['columns'].remove('DATE') 

    total_records = 0
    for advertiser_id in advertiser_ids:
        # Set advertiser ID, if present in string.
        custom_url = url.format(advertiser_id=advertiser_id)

        for start, end in segments:
            LOGGER.info(f' -- Looking up data for advertiser: {advertiser_id} ---- segment: {start} --TO--> {end}')

            body.update(dict(
                start_date=start.strftime("%Y-%m-%d"),
                end_date=end.strftime("%Y-%m-%d")
            ))

            # Create request to generate report
            LOGGER.info(f'URL for {stream.name}: {custom_url} -> body: {body.items()}')
            res = client.post(url=custom_url, endpoint=stream.name, json=body)

            # If the report generates instantly
            if res.get('report_status') == 'FINISHED':
                token = res.get('token')
            else:
                token = client.retry_report('post', custom_url, stream.name, json=body, key='token')

            # GET the report data using the token
            LOGGER.info(f'Getting report with token: {token}')
            res = client.get(url=custom_url, endpoint=stream.name, params=dict(token=token))

            # Here we do the same retry.
            # Normally this should never be used as we wait in the first step, but lets be safe.
            if res.get('report_status') == 'FINISHED':
                report_url = res.get('url')
            else:
                report_url = client.retry_report('get', url, stream.name, params=dict(token=token), key='url')
            LOGGER.info(f'REPORT URL -> {report_url}')

            # Now that we have the report, we need to dowload the link to the file.
            data = client.download_report(report_url)
            if not data:
                LOGGER.info(f' -- No data for report at : {report_url}')

            # time_extracted: datetime when the data was extracted from the API
            time_extracted = utils.now()

            for line in data.values():
                # Process records and get the max_bookmark_value and record_count for the set of records
                max_bookmark_value, record_count = process_records(
                    catalog=catalog,
                    stream_name=stream.name,
                    records=line,
                    time_extracted=time_extracted,
                    bookmark_field=stream.bookmark_field,
                    max_bookmark_value=max_bookmark_value,
                    last_datetime=start)
                LOGGER.info(f'{stream.name}: Synced report. Total records processed: {record_count}')
                total_records += record_count

    return total_records, max_bookmark_value


def process_records(catalog, stream_name, records, time_extracted,
                    bookmark_field=None,
                    max_bookmark_value=None,
                    last_datetime=None):
    stream = catalog.get_stream(stream_name)
    schema = stream.schema.to_dict()

    with metrics.record_counter(stream_name) as counter:
        for record in records:
            for key in schema['properties']:
                if key not in record:
                    record[key] = None
                elif isinstance(record[key], int) or (isinstance(record[key], str) and record[key].isdigit()):
                    # Cast ints to floats to never have schema issues. 
                    # !!! this also converts strings with digits and booleans
                    record[key] = float(record[key])

            # Remove all entries that are not in the schema. This is used for custom reports.
            record = {key: value for key, value in record.items() if key in schema['properties']}

            if bookmark_field in record:
                bookmark_dttm = decode_bookmark_field(record[bookmark_field])
            else:
                bookmark_dttm = datetime.now()

            if isinstance(max_bookmark_value, str):
                max_bookmark_value = datetime.strptime(max_bookmark_value, "%Y-%m-%dT%H:%M:%SZ")

            # Reset max_bookmark_value to new value if higher
            if (bookmark_field and (bookmark_field in record)) and (max_bookmark_value is None or bookmark_dttm > max_bookmark_value):
                max_bookmark_value = bookmark_dttm

            if bookmark_field and (bookmark_field in record):
                last_dttm = date(year = last_datetime.year, month = last_datetime.month, day = last_datetime.day)
                # Keep only records whose bookmark is after the last_datetime
                if (bookmark_dttm.date() >= last_dttm):
                    helpers.write_record(stream_name, record, time_extracted=time_extracted)
                    counter.increment()
            else:
                helpers.write_record(stream_name, record, time_extracted=time_extracted)
                counter.increment()

        return max_bookmark_value, counter.value

