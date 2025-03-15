from google.cloud import bigquery
import pandas as pd
from fetch import fetch_stops

# Construct a BigQuery client object.
client = bigquery.Client()

# Set table_id to the ID of the table to create.
table_id = f'{client.project}.routes.stops'

# Function: update_stops

def update_stops():
    """
    Update table containing bus stops.
    """
    # Fetch routes.

    stops = fetch_stops()

    if stops is not None:
        # Set configuration options for load job.

        job_config = bigquery.LoadJobConfig(
            schema = [
                bigquery.SchemaField(
                    name='line_number',
                    field_type='STRING',
                    mode='REQUIRED',
                    description='The line number.'
                ),
                bigquery.SchemaField(
                    name='itinerary_id',
                    field_type='STRING',
                    mode='REQUIRED',
                    description='The itinerary ID.'
                ),
                bigquery.SchemaField(
                    name='group',
                    field_type='STRING',
                    mode='NULLABLE',
                    description='The group of points.'
                ),
                bigquery.SchemaField(
                    name='number',
                    field_type='STRING',
                    mode='NULLABLE',
                    description='The stop number.'
                ),
                bigquery.SchemaField(
                    name='name',
                    field_type='STRING',
                    mode='REQUIRED',
                    description='The stop name.'
                ),
                bigquery.SchemaField(
                    name='type',
                    field_type='STRING',
                    mode='NULLABLE',
                    description='The stop type.'
                ),
                bigquery.SchemaField(
                    name='order',
                    field_type='INT64',
                    mode='REQUIRED',
                    description='The order of the stop within the pair (line_number, itinerary_id).'
                ),
                bigquery.SchemaField(
                    name='direction',
                    field_type='STRING',
                    mode='REQUIRED',
                    description='The stop direction.'
                ),
                bigquery.SchemaField(
                    name='latitude',
                    field_type='FLOAT',
                    mode='REQUIRED',
                    description='The stop latitude.'
                ),
                bigquery.SchemaField(
                    name='longitude',
                    field_type='FLOAT',
                    mode='REQUIRED',
                    description='The stop longitude.'
                )
            ],
            autodetect=False,
            source_format=bigquery.SourceFormat.CSV,
            write_disposition='WRITE_TRUNCATE',
        )

        # Load data.

        job = client.load_table_from_dataframe(
            dataframe=stops,
            destination=table_id,
            job_config=job_config
        )
        job.result()

        table = client.get_table(table_id)
        print(
            'Loaded {0} rows to {1}, which now has {2} rows.'.format(
                stops.shape[0],
                table_id,
                table.num_rows
            )
        )

    else:
        print('No rows have been loaded.')
