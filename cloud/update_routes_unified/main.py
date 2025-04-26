from update import update_routes_unified
import functions_framework

@functions_framework.cloud_event
def update_routes_unified_entry_point(cloud_event):
    update_routes_unified()
