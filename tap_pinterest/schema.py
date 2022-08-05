import os
import json
from singer import metadata
from dataclasses import dataclass

# Reference:
#   https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#Metadata

API_VERSION = 'v5'
BASE_URL = f'https://api.pinterest.com/{API_VERSION}'

@dataclass
class stream:
    name: str  
    key_properties : list
    replication_method : str
    replication_keys : list
    path : str
    data_key : str 
    params: dict
    async_report : bool
    
    @property
    def bookmark_field(self): 
        return self.replication_keys[0] if self.replication_keys else ''
    

STREAMS = [
    stream (
        name = 'ad_accounts',
        key_properties = ['id'] ,
        replication_method = 'FULL_TABLE',
        replication_keys = [],
        path = 'ad_accounts',
        data_key = 'items' ,
        params = {},
        async_report = False 
    ),
    stream (
        name = 'campaigns',
        key_properties = ['id'] ,
        replication_method = 'INCREMENTAL',
        replication_keys = ['updated_time'],
        path = 'ad_accounts/{advertiser_id}/campaigns',
        data_key = 'items' ,
        params = {},
        async_report = False 
    ),
    stream (
        name = 'ad_groups',
        key_properties = ['id'] ,
        replication_method = 'INCREMENTAL',
        replication_keys = ['updated_time'],
        path = 'ad_accounts/{advertiser_id}/ad_groups',
        data_key = 'items' ,
        params = {},
        async_report = False 
    ),
    stream (
        name = 'advertiser_delivery_metrics',
        key_properties = ['ADVERTISER_ID'],
        replication_method = 'INCREMENTAL',
        replication_keys = ['DATE'],
        path = 'ad_accounts/{advertiser_id}/reports', 
        data_key = 'items' ,
        params = {
            'granularity': 'DAY',
            'level': 'ADVERTISER',
        },
        async_report = True 
    ),
    stream (
        name = 'campaign_delivery_metrics',
        key_properties = ['CAMPAIGN_ID'],
        replication_method = 'INCREMENTAL',
        replication_keys = ['DATE'],
        path = 'ad_accounts/{advertiser_id}/reports', 
        data_key = 'items' ,
        params = {
            'granularity': 'DAY',
            'level': 'CAMPAIGN',
        },
        async_report = True 
    ),
    stream (
        name = 'ad_group_delivery_metrics',
        key_properties = ['AD_GROUP_ID'],
        replication_method = 'INCREMENTAL',
        replication_keys = ['DATE'],
        path = 'ad_accounts/{advertiser_id}/reports', 
        data_key = 'items' ,
        params = {
            'granularity': 'DAY',
            'level': 'AD_GROUP',
        },
        async_report = True 
    ),
    stream (
        name = 'pin_promotion_delivery_metrics',
        key_properties = ['PIN_PROMOTION_ID'],
        replication_method = 'INCREMENTAL',
        replication_keys = ['DATE'],
        path = 'ad_accounts/{advertiser_id}/reports', 
        data_key = 'items' ,
        params = {
            'granularity': 'DAY',
            'level': 'PIN_PROMOTION',
        },
        async_report = True 
    )
]


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def get_schemas(custom_reports=None):
    schemas = {}
    field_metadata = {}

    for stream in STREAMS:

        filtered_custom_reports = [custom_report for custom_report in custom_reports if stream.name == custom_report['stream']]

        schema_path = get_abs_path(f'schemas/{stream.name}.json')
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

        schemas[stream.name] = schema

        mdata = metadata.new()

        # Documentation:
        #   https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md
        # Reference:
        #   https://github.com/singer-io/singer-python/blob/master/singer/metadata.py#L25-L44
        mdata = metadata.get_standard_metadata(
            schema=schema,
            key_properties=stream.key_properties,
            valid_replication_keys=stream.replication_keys,
            replication_method=stream.replication_method
        )
        field_metadata[stream.name] = mdata

    return schemas, field_metadata
