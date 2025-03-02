from google.cloud import bigquery

# Construct a BigQuery client object.
client = bigquery.Client()

# Set table_id to the ID of the table to create.
table_id = f'{client.project}.routes.routes'

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
]

table = bigquery.Table(
    table_ref=table_id,
    schema=schema
)

# Specify the table description.
table.description = 'Bus routes as sets of coordinates.'

table = client.create_table(table)  # Make an API request.
print(
    'Created table {}.{}.{}'.format(table.project, table.dataset_id, table.table_id)
)
