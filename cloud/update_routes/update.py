from google.cloud import bigquery
import pandas as pd
from fetch import fetch_routes

# Construct a BigQuery client object.
client = bigquery.Client()

# Set table_id to the ID of the table to create.
table_id = f'{client.project}.routes.routes'

# Function: update_routes

def update_routes():
    """
    Update table containing bus routes.
    """
    # Fetch routes.

    routes = fetch_routes()

    if routes is not None:
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
                    name='route_id',
                    field_type='INT64',
                    mode='REQUIRED',
                    description='The route ID.'
                ),
                bigquery.SchemaField(
                    name='latitude',
                    field_type='FLOAT',
                    mode='REQUIRED',
                    description='A latitude in the set of points comprising the route.'
                ),
                bigquery.SchemaField(
                    name='longitude',
                    field_type='FLOAT',
                    mode='REQUIRED',
                    description='A longitude in the set of points comprising the route.'
                )
            ],
            autodetect=False,
            source_format=bigquery.SourceFormat.CSV,
            write_disposition='WRITE_TRUNCATE',
        )

        # Load data.

        job = client.load_table_from_dataframe(
            dataframe=routes,
            destination=table_id,
            job_config=job_config
        )
        job.result()

        table = client.get_table(table_id)
        print(
            'Loaded {0} rows to {1}, which now has {2} rows.'.format(
                routes.shape[0],
                table_id,
                table.num_rows
            )
        )

    else:
        print('No rows have been loaded.')
