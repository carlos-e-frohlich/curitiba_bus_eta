from google.cloud import bigquery

# Construct a BigQuery client object.
client = bigquery.Client()

# Set table_id to the ID of the table to create.
table_id = f'{client.project}.lines.lines'

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
]

table = bigquery.Table(
    table_ref=table_id,
    schema=schema
)

# Specify the table description.
table.description = 'Information on individual bus lines.'

table = client.create_table(table)  # Make an API request.
print(
    'Created table {}.{}.{}'.format(table.project, table.dataset_id, table.table_id)
)
