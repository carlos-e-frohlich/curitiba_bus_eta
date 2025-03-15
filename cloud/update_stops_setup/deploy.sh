gcloud pubsub topics create update_stops
gcloud functions deploy update_stops \
    --region=southamerica-east1 \
    --entry-point=update_stops_entry_point \
    --gen2 \
    --runtime=python312 \
    --timeout=540s \
    --memory=512M \
    --trigger-topic=update_stops
gcloud scheduler jobs create pubsub update_stops \
    --location=southamerica-east1 \
    --schedule="6 3 * * *" \
    --topic=update_stops \
    --message-body="Publishing message to update_stops" \
    --time-zone="America/Sao_Paulo"
