from update import update_lines
import functions_framework

@functions_framework.cloud_event
def update_lines_entry_point(cloud_event):
    update_lines()
