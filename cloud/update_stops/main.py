from update import update_stops
import functions_framework

@functions_framework.cloud_event
def update_stops_entry_point(cloud_event):
    update_stops()
