from google.cloud import bigquery
import pandas as pd
from fetch import fetch_lines

# Construct a BigQuery client object.
client = bigquery.Client()

# Set table_id to the ID of the table to create.
table_id = f'{client.project}.lines.lines'

# Function: update_lines

def update_lines():
    """
    Update table containing bus lines.
    """
    # Fetch lines.

    lines = fetch_lines()

    if lines is not None:
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
                    description='The original line name as per the API.'
                ),
                bigquery.SchemaField(
                    name='line_name_short',
                    field_type='STRING',
                    mode='NULLABLE',
                    description='A short line name.'
                ),
                bigquery.SchemaField(
                    name='line_name_long',
                    field_type='STRING',
                    mode='NULLABLE',
                    description='A long line name.'
                ),
                bigquery.SchemaField(
                    name='fare_card_only',
                    field_type='STRING',
                    mode='REQUIRED',
                    description='Whether the line accepts fare cards only.'
                ),
                bigquery.SchemaField(
                    name='service_category',
                    field_type='STRING',
                    mode='REQUIRED',
                    description='The service category.'
                ),
                bigquery.SchemaField(
                    name='color',
                    field_type='STRING',
                    mode='REQUIRED',
                    description='The line color.'
                )
            ],
            write_disposition='WRITE_TRUNCATE',
        )

        # Load data.

        job = client.load_table_from_dataframe(
            dataframe=lines,
            destination=table_id,
            job_config=job_config
        )
        job.result()

        table = client.get_table(table_id)
        print(
            'Loaded {0} rows to {1}, which now has {2} rows.'.format(
                lines.shape[0],
                table_id,
                table.num_rows
            )
        )

    else:
        print('No rows have been loaded.')
