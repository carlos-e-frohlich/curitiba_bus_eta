from google.cloud import bigquery
import pandas as pd
from fetch import fetch_set_of_locations

# Construct a BigQuery client object.
client = bigquery.Client()

# Set table_id to the ID of the table to create.
table_id = f'{client.project}.locations.locations'

# Function: ingest_locations

def ingest_locations(line_number: str):
    '''
    Fetch set of locations and ingest it.

    Args:
        line_number (str): The line number whose locations are ingested.
    '''
    # Fetch set of locations.

    set_of_locations = fetch_set_of_locations(line_number=line_number)

    if set_of_locations is not None:
        # Set configuration options for load job.

        job_config = bigquery.LoadJobConfig(
            schema = [
                bigquery.SchemaField(
                    name='fleet_number',
                    field_type='STRING',
                    mode='REQUIRED'
                ),
                bigquery.SchemaField(
                    name='update_datetime',
                    field_type='TIMESTAMP',
                    mode='REQUIRED'
                ),
                bigquery.SchemaField(
                    name='latitude',
                    field_type='FLOAT',
                    mode='REQUIRED'
                ),
                bigquery.SchemaField(
                    name='longitude',
                    field_type='FLOAT',
                    mode='REQUIRED'
                ),
                bigquery.SchemaField(
                    name='line_number',
                    field_type='STRING',
                    mode='REQUIRED'
                ),
                bigquery.SchemaField(
                    name='wheelchair_accessibility',
                    field_type='INTEGER',
                    mode='NULLABLE'
                ),
                bigquery.SchemaField(
                    name='bus_type',
                    field_type='STRING',
                    mode='NULLABLE'
                ),
                bigquery.SchemaField(
                    name='timetable',
                    field_type='INTEGER',
                    mode='NULLABLE'
                ),
                bigquery.SchemaField(
                    name='status_time',
                    field_type='STRING',
                    mode='NULLABLE'
                ),
                bigquery.SchemaField(
                    name='status_route',
                    field_type='STRING',
                    mode='NULLABLE'
                ),
                bigquery.SchemaField(
                    name='direction',
                    field_type='STRING',
                    mode='NULLABLE'
                ),
                bigquery.SchemaField(
                    name='cycle_count',
                    field_type='INTEGER',
                    mode='REQUIRED'
                ),
                bigquery.SchemaField(
                    name='destination',
                    field_type='STRING',
                    mode='NULLABLE'
                )
            ],
            write_disposition='WRITE_APPEND',
        )

        # Load data.

        job = client.load_table_from_dataframe(
            dataframe=set_of_locations,
            destination=table_id,
            job_config=job_config
        )
        job.result()

        table = client.get_table(table_id)
        print(
            'Loaded {0} rows to {1}, which now has {2} rows.'.format(
                set_of_locations.shape[0],
                table_id,
                table.num_rows
            )
        )
        # print(
        #     'Loaded {} rows and {} columns to {}'.format(
        #         table.num_rows, len(table.schema), table_id
        #     )
        # )

    else:
        print('No rows have been loaded.')

