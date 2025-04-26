from google.cloud import bigquery

# Construct a BigQuery client object
client = bigquery.Client()

# Set table_id to the ID of the table to create
table_id = f'{client.project}.routes.routes_unified'

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
]

table = bigquery.Table(
    table_ref=table_id,
    schema=schema
)

# Specify the table description
table.description = 'Unified bus routes as sets of coordinates.'

# Make an API request
table = client.create_table(table)
print(
    'Created table {}.{}.{}'.format(table.project, table.dataset_id, table.table_id)
)
