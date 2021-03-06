import os
import json
from singer import metadata

# Reference:
#   https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#Metadata

STREAMS = {
    'advertisers': {
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
        'replication_keys': ['updated_time']
    },
    'advertisers_campaigns': {
        'key_properties': ['advertiser_id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['updated_time']
    },
    'advertiser_delivery_metrics': {
        'key_properties': ['ADVERTISER_ID'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['DATE'],
    },
    'campaign_delivery_metrics': {
        'key_properties': ['CAMPAIGN_ID'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['DATE'],
    },
    'ad_group_delivery_metrics': {
        'key_properties': ['AD_GROUP_ID'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['DATE'],
    },
    'pin_promotion_delivery_metrics': {
        'key_properties': ['PIN_PROMOTION_ID'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['DATE'],
    },
    'campaign_ad_groups': {
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['updated_time']
    }
}


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def get_schemas(custom_reports=None):
    schemas = {}
    field_metadata = {}

    for stream_name, stream_metadata in STREAMS.items():

        filtered_custom_reports = [custom_report for custom_report in custom_reports if stream_name == custom_report['stream']]

        schema_path = get_abs_path(f'schemas/{stream_name}.json')
        with open(schema_path) as file:
            schema = json.load(file)

        # If there are custom reports for this stream name, define a custom schema for this report.
        for custom_report in filtered_custom_reports:
            custom_schema = dict(type='object', properties={})
            for key, value in schema['properties'].items():
                if key in custom_report['columns']:
                    custom_schema['properties'][key] = value

            if custom_schema['properties']:
                custom_schema['properties']['DATE'] = schema['properties'].get('DATE', None)
                schema = custom_schema

        schemas[stream_name] = schema

        mdata = metadata.new()

        # Documentation:
        #   https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md
        # Reference:
        #   https://github.com/singer-io/singer-python/blob/master/singer/metadata.py#L25-L44
        mdata = metadata.get_standard_metadata(
            schema=schema,
            key_properties=stream_metadata.get('key_properties', None),
            valid_replication_keys=stream_metadata.get('replication_keys', None),
            replication_method=stream_metadata.get('replication_method', None)
        )
        field_metadata[stream_name] = mdata

    return schemas, field_metadata
