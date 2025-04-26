gcloud pubsub topics create update_routes_unified
gcloud functions deploy update_routes_unified \
    --region=southamerica-east1 \
    --entry-point=update_routes_unified_entry_point \
    --gen2 \
    --runtime=python312 \
    --timeout=540s \
    --memory=512M \
    --trigger-topic=update_routes_unified
gcloud scheduler jobs create pubsub update_routes_unified \
    --location=southamerica-east1 \
    --schedule="6 3 * * *" \
    --topic=update_routes_unified \
    --message-body="Publishing message to update_routes_unified" \
    --time-zone="America/Sao_Paulo"
