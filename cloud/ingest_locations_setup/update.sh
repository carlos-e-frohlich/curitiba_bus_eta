gcloud functions deploy ingest_locations \
    --region=southamerica-east1 \
    --entry-point=ingest_locations_entry_point \
    --gen2 \
    --runtime=python312 \
    --memory=512M \
    --trigger-topic=ingest_locations
