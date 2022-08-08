from singer.catalog import Catalog, CatalogEntry, Schema
from tap_pinterest.schema import get_schemas, STREAMS
import singer
import json
import sys

LOGGER = singer.get_logger()


def do_discover(custom_report=None):

    LOGGER.info('Starting discover')
    catalog = discover(custom_report)
    json.dump(catalog.to_dict(), sys.stdout, indent=2)
    LOGGER.info('Finished discover')


def discover(custom_reports=None):
    schemas, field_metadata = get_schemas(custom_reports)
    catalog = Catalog([])

    for stream_name, schema_dict in schemas.items():
        schema = Schema.from_dict(schema_dict)
        mdata = field_metadata[stream_name]
        stream = list(filter(lambda s: s.name==stream_name,STREAMS))[0]

        catalog.streams.append(CatalogEntry(
            stream=stream_name,
            tap_stream_id=stream_name,
            key_properties= stream.key_properties,
            schema=schema,
            metadata=mdata
        ))

    return catalog
