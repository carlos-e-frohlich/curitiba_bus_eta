from google.cloud import bigquery
import pandas as pd
from fetch import fetch_set_of_locations, fetch_recent_locations, drop_potential_location_duplicates

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
    # Fetch and deduplicate set of locations.

    recent_locations = fetch_recent_locations(hours=3)
    set_of_locations = fetch_set_of_locations(line_number=line_number)
    set_of_locations = drop_potential_location_duplicates(
        recent_locations=recent_locations,
        set_of_locations=set_of_locations
    )

    if set_of_locations is not None:
        # Set configuration options for load job.

        job_config = bigquery.LoadJobConfig(
            schema = [
                bigquery.SchemaField(
                    name='fleet_number',
                    field_type='STRING',
                    mode='REQUIRED',
                    description='The vehicle\'s fleet number.'
                ),
                bigquery.SchemaField(
                    name='update_datetime',
                    field_type='TIMESTAMP',
                    mode='REQUIRED',
                    description='The date and time at which the location was reported.'
                ),
                bigquery.SchemaField(
                    name='latitude',
                    field_type='FLOAT',
                    mode='REQUIRED',
                    description='The reported latitutde.'
                ),
                bigquery.SchemaField(
                    name='longitude',
                    field_type='FLOAT',
                    mode='REQUIRED',
                    description='The reported longitude.'
                ),
                bigquery.SchemaField(
                    name='line_number',
                    field_type='STRING',
                    mode='REQUIRED',
                    description='The line number.'
                ),
                bigquery.SchemaField(
                    name='wheelchair_accessibility',
                    field_type='INTEGER',
                    mode='NULLABLE',
                    description='Whether the vehicle has wheelchair accessibility (1 = yes).'
                ),
                bigquery.SchemaField(
                    name='bus_type',
                    field_type='STRING',
                    mode='NULLABLE',
                    description='The bus type.'
                ),
                bigquery.SchemaField(
                    name='timetable',
                    field_type='INTEGER',
                    mode='NULLABLE',
                    description='The timetable followed by the vehicle.'
                ),
                bigquery.SchemaField(
                    name='status_time',
                    field_type='STRING',
                    mode='NULLABLE',
                    description='The location status regarding time.'
                ),
                bigquery.SchemaField(
                    name='status_route',
                    field_type='STRING',
                    mode='NULLABLE',
                    description='The location status regarding the route.'
                ),
                bigquery.SchemaField(
                    name='direction',
                    field_type='STRING',
                    mode='NULLABLE',
                    description='The vehicle\'s direction.'
                ),
                bigquery.SchemaField(
                    name='cycle_count',
                    field_type='INTEGER',
                    mode='REQUIRED',
                    description='Count of cycles with no location update (1 = updated).'
                ),
                bigquery.SchemaField(
                    name='destination',
                    field_type='STRING',
                    mode='NULLABLE',
                    description='The vehicle\'s destination.'
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

    else:
        print('No rows have been loaded.')
