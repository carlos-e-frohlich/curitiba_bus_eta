from ingest import ingest_locations
import functions_framework

@functions_framework.cloud_event
def ingest_locations_entry_point(cloud_event):
    ingest_locations(line_number='216')
