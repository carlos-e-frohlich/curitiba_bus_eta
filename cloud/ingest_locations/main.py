import pandas_gbq
from fetch import fetch_set_of_locations


def append_locations(message=None, context=None):
    set_of_locations = fetch_set_of_locations(line_number='216')

    pandas_gbq.to_gbq(
        set_of_locations,
        'locations.locations',
        project_id='curitiba-bus-eta',
        if_exists='append'
    )

if __name__ == '__main__':
    append_locations()
