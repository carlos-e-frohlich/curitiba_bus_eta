from google.cloud import bigquery
import pandas as pd
import json
from generate import generate_route_unified
from fetch import fetch_line_routes_from_bq, fetch_line_stops_from_bq

# Load mapping from line numbers into their respective route IDs
with open('line_numbers_to_route_ids_map.json', 'r') as f:
    line_numbers_to_route_ids_map = json.load(f)

# Construct a BigQuery client object.
client = bigquery.Client()

# Set table_id to the ID of the table to create.
table_id = f'{client.project}.routes.routes_unified'

# Function: update_routes_unified


def update_routes_unified():
    """
    Update table containing unified bus routes.
    """
    # Prepare dataframe for unified routes
    routes_unified = pd.DataFrame()

    # Loop over line numbers whose route IDs have been mapped
    for line_number, value in line_numbers_to_route_ids_map.items():
        line_routes = fetch_line_routes_from_bq(line_number=line_number)
        line_stops = fetch_line_stops_from_bq(line_number=line_number)

        route_unified = generate_route_unified(
            line_number=line_number,
            line_routes=line_routes,
            line_stops=line_stops
        )

        routes_unified = pd.concat(
            objs=[
                routes_unified,
                route_unified
            ],
            ignore_index=True
        )

    if routes_unified.shape[0] > 0:
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
                    name='order',
                    field_type='INTEGER',
                    mode='REQUIRED',
                    description='The order of points within the unified route.'
                ),
                bigquery.SchemaField(
                    name='latitude',
                    field_type='FLOAT',
                    mode='REQUIRED',
                    description='The point latitude.'
                ),
                bigquery.SchemaField(
                    name='longitude',
                    field_type='FLOAT',
                    mode='REQUIRED',
                    description='The point longitude.'
                ),
                bigquery.SchemaField(
                    name='point_type',
                    field_type='STRING',
                    mode='NULLABLE',
                    description='The type of the bus stop if point is a bus stop, NULL otherwise.'
                ),
                bigquery.SchemaField(
                    name='stop_number',
                    field_type='STRING',
                    mode='NULLABLE',
                    description='The number of the bus stop if point is a bus stop, NULL otherwise.'
                ),
                bigquery.SchemaField(
                    name='stop_name',
                    field_type='STRING',
                    mode='NULLABLE',
                    description='The name of the bus stop if point is a bus stop, NULL otherwise.'
                ),
                bigquery.SchemaField(
                    name='direction',
                    field_type='STRING',
                    mode='REQUIRED',
                    description='The direction associated with the point.'
                )
            ],
            write_disposition='WRITE_TRUNCATE',
        )

        # Load data.
        job = client.load_table_from_dataframe(
            dataframe=routes_unified,
            destination=table_id,
            job_config=job_config
        )
        job.result()

        table = client.get_table(table_id)
        print(
            'Loaded {0} rows to {1}, which now has {2} rows.'.format(
                routes_unified.shape[0],
                table_id,
                table.num_rows
            )
        )

    else:
        print('No rows have been loaded.')
