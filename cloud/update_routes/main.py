from update import update_routes
import functions_framework

@functions_framework.cloud_event
def update_routes_entry_point(cloud_event):
    update_routes()
