from singer.catalog import Catalog, CatalogEntry, Schema
from tap_pinterest.schema import get_schemas, STREAMS


def discover(custom_reports=None):
    schemas, field_metadata = get_schemas()
    catalog = Catalog([])

    for stream_name, schema_dict in schemas.items():

        schema = Schema.from_dict(schema_dict)
        mdata = field_metadata[stream_name]

        catalog.streams.append(CatalogEntry(
            stream=stream_name,
            tap_stream_id=stream_name,
            key_properties=STREAMS[stream_name]['key_properties'],
            schema=schema,
            metadata=mdata
        ))

        custom_reports = [custom_report for custom_report in custom_reports if stream_name == custom_report['name']]

        # If there are custom reports for this stream name, define a custom schema for this report.
        for custom_report in custom_reports:
            tmp_report = dict()
            for key, value in schema_dict.items():
                if key in custom_report['schema']:
                    tmp_report[key] = value
            catalog.streams.append(CatalogEntry(
                stream=stream_name,
                tap_stream_id=stream_name,
                key_properties=STREAMS[stream_name]['key_properties'],
                schema=Schema.from_dict(tmp_report),
                metadata=mdata
            ))

    return catalog
