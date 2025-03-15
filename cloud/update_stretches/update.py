from google.cloud import bigquery
import pandas as pd
from fetch import fetch_stretches

# Construct a BigQuery client object.
client = bigquery.Client()

# Set table_id to the ID of the table to create.
table_id = f'{client.project}.routes.stretches'

# Function: update_stretches

def update_stretches():
    """
    Update table containing stretches.
    """
    # Fetch stretches.

    stretches = fetch_stretches()

    if stretches is not None:
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
                    name='line_name_original',
                    field_type='STRING',
                    mode='REQUIRED',
                    description='The original line name.'
                ),
                bigquery.SchemaField(
                    name='service_category_code',
                    field_type='STRING',
                    mode='REQUIRED',
                    description='The service category code.'
                ),
                bigquery.SchemaField(
                    name='service_category',
                    field_type='STRING',
                    mode='REQUIRED',
                    description='The service category name.'
                ),
                bigquery.SchemaField(
                    name='company_code',
                    field_type='STRING',
                    mode='REQUIRED',
                    description='The code of the company operating the line.'
                ),
                bigquery.SchemaField(
                    name='company',
                    field_type='STRING',
                    mode='REQUIRED',
                    description='The code of the company operating the line.'
                ),
                bigquery.SchemaField(
                    name='stop_code_timetable',
                    field_type='STRING',
                    mode='REQUIRED',
                    description='The stop code in the timetable.'
                ),
                bigquery.SchemaField(
                    name='stop_name_timetable',
                    field_type='STRING',
                    mode='REQUIRED',
                    description='The stop name in the timetable.'
                ),
                bigquery.SchemaField(
                    name='order_itinerary',
                    field_type='STRING',
                    mode='REQUIRED',
                    description='The order of the stop in the itinerary.'
                ),
                bigquery.SchemaField(
                    name='itinerary_code',
                    field_type='STRING',
                    mode='REQUIRED',
                    description='The itinerary code.'
                ),
                bigquery.SchemaField(
                    name='itinerary',
                    field_type='STRING',
                    mode='REQUIRED',
                    description='The itinerary name.'
                ),
                bigquery.SchemaField(
                    name='special_stop',
                    field_type='STRING',
                    mode='REQUIRED',
                    description='S = yes; N = no.'
                ),
                bigquery.SchemaField(
                    name='point_code_stretch_start',
                    field_type='STRING',
                    mode='REQUIRED',
                    description='The stop code at the start of the stretch.'
                ),
                bigquery.SchemaField(
                    name='point_order_start',
                    field_type='STRING',
                    mode='REQUIRED',
                    description='The stop order at the start of the stretch.'
                ),
                bigquery.SchemaField(
                    name='point_code_strech_end',
                    field_type='STRING',
                    mode='REQUIRED',
                    description='The stop code at the end of the stretch.'
                ),
                bigquery.SchemaField(
                    name='point_order_end',
                    field_type='STRING',
                    mode='REQUIRED',
                    description='The stop order at the end of the stretch.'
                ),
                bigquery.SchemaField(
                    name='distance_start_to_end',
                    field_type='STRING',
                    mode='REQUIRED',
                    description='The distance from the start to the end of the stretch.'
                ),
                bigquery.SchemaField(
                    name='stretch_type',
                    field_type='STRING',
                    mode='REQUIRED',
                    description='The stretch type.'
                ),
                bigquery.SchemaField(
                    name='stop_code',
                    field_type='STRING',
                    mode='REQUIRED',
                    description='The unique stop code.'
                ),
                bigquery.SchemaField(
                    name='stop_name',
                    field_type='STRING',
                    mode='REQUIRED',
                    description='The unique stop name.'
                ),
                bigquery.SchemaField(
                    name='urbs_code',
                    field_type='STRING',
                    mode='REQUIRED',
                    description='The internal stop code.'
                )
            ],
            autodetect=False,
            source_format=bigquery.SourceFormat.CSV,
            write_disposition='WRITE_TRUNCATE',
        )

        # Load data.

        job = client.load_table_from_dataframe(
            dataframe=stretches,
            destination=table_id,
            job_config=job_config
        )
        job.result()

        table = client.get_table(table_id)
        print(
            'Loaded {0} rows to {1}, which now has {2} rows.'.format(
                stretches.shape[0],
                table_id,
                table.num_rows
            )
        )

    else:
        print('No rows have been loaded.')
