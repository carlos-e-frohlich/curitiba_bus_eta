gcloud pubsub topics create update_lines
gcloud functions deploy update_lines \
    --region=southamerica-east1 \
    --entry-point=update_lines_entry_point \
    --gen2 \
    --runtime=python312 \
    --memory=512M \
    --trigger-topic=update_lines
gcloud scheduler jobs create pubsub update_lines \
    --location=southamerica-east1 \
    --schedule="6 3 * * *" \
    --topic=update_lines \
    --message-body="Publishing message to update_lines" \
    --time-zone="America/Sao_Paulo"
