from google.cloud import secretmanager

# Create the Secret Manager client.

client = secretmanager.SecretManagerServiceClient()

# Get the API key for bus data.

response_api_key_bus_data = client.get_secret(request={"name": "projects/441247924338/secrets/api_keys"})
api_key_bus_data = response_api_key_bus_data.labels['api_curitiba_156']
