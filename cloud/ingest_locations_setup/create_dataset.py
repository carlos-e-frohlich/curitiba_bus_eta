from google.cloud import bigquery

# Construct a BigQuery client object.
client = bigquery.Client()

# Set dataset_id to the ID of the dataset to create.
dataset_id = '{}.locations'.format(client.project)

# Construct a full Dataset object to send to the API.
dataset = bigquery.Dataset(dataset_ref=dataset_id)

# Specify the geographic location where the dataset should reside.
dataset.location = 'southamerica-east1'

# Send the dataset to the API for creation.
dataset = client.create_dataset(
    dataset=dataset,
    timeout=30
)  # Make an API request.
print('Created dataset {}.{}'.format(client.project, dataset.dataset_id))
