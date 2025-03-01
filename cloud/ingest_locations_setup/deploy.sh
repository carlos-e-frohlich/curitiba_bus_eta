gcloud pubsub topics create ingest_locations
gcloud functions deploy ingest_locations \
    --region=southamerica-east1 \
    --entry-point=ingest_locations_entry_point \
    --gen2 \
    --runtime=python312 \
    --memory=512M \
    --trigger-topic=ingest_locations
gcloud scheduler jobs create pubsub ingest_locations \
    --location=southamerica-east1 \
    --schedule="*/2 0,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23 * * *" \
    --topic=ingest_locations \
    --message-body="Publishing message to ingest_locations" \
    --time-zone="America/Sao_Paulo"
