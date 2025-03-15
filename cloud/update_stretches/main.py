from update import update_stretches
import functions_framework

@functions_framework.cloud_event
def update_stretches_entry_point(cloud_event):
    update_stretches()
