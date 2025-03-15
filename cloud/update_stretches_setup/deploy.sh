gcloud pubsub topics create update_stretches
gcloud functions deploy update_stretches \
    --region=southamerica-east1 \
    --entry-point=update_stretches_entry_point \
    --gen2 \
    --runtime=python312 \
    --timeout=540s \
    --memory=512M \
    --trigger-topic=update_stretches
gcloud scheduler jobs create pubsub update_stretches \
    --location=southamerica-east1 \
    --schedule="6 3 * * *" \
    --topic=update_stretches \
    --message-body="Publishing message to update_stretches" \
    --time-zone="America/Sao_Paulo"
