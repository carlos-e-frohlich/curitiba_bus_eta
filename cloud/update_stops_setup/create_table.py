from google.cloud import bigquery

# Construct a BigQuery client object.
client = bigquery.Client()

# Set table_id to the ID of the table to create.
table_id = f'{client.project}.routes.stops'

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
]

table = bigquery.Table(
    table_ref=table_id,
    schema=schema
)

# Specify the table description.
table.description = 'Bus stops and their data.'

table = client.create_table(table)  # Make an API request.
print(
    'Created table {}.{}.{}'.format(table.project, table.dataset_id, table.table_id)
)
