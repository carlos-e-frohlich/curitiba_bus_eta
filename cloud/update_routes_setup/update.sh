gcloud functions deploy update_routes \
    --region=southamerica-east1 \
    --entry-point=update_routes_entry_point \
    --gen2 \
    --runtime=python312 \
    --timeout=540s \
    --memory=512M \
    --trigger-topic=update_routes
