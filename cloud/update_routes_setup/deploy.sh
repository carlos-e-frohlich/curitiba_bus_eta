gcloud pubsub topics create update_routes
gcloud functions deploy update_routes \
    --region=southamerica-east1 \
    --entry-point=update_routes_entry_point \
    --gen2 \
    --runtime=python312 \
    --timeout=540s \
    --memory=512M \
    --trigger-topic=update_routes
gcloud scheduler jobs create pubsub update_routes \
    --location=southamerica-east1 \
    --schedule="6 3 * * *" \
    --topic=update_routes \
    --message-body="Publishing message to update_routes" \
    --time-zone="America/Sao_Paulo"
