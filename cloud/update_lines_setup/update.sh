gcloud functions deploy update_lines \
    --region=southamerica-east1 \
    --entry-point=update_lines_entry_point \
    --gen2 \
    --runtime=python312 \
    --memory=512M \
    --trigger-topic=update_lines
