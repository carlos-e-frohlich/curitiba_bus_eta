from google.cloud import bigquery

# Construct a BigQuery client object.
client = bigquery.Client()

# Set table_id to the ID of the table to create.
table_id = f'{client.project}.locations.locations'

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
]

table = bigquery.Table(
    table_ref=table_id,
    schema=schema
)

# Specify the table description.
table.description = 'Bus locations retrieved every two minutes from API.'

table = client.create_table(table)  # Make an API request.
print(
    'Created table {}.{}.{}'.format(table.project, table.dataset_id, table.table_id)
)
