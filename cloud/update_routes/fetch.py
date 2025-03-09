import requests
from keys import api_key_bus_data
import pandas as pd
from google.cloud import bigquery

# Construct a BigQuery client object.
client = bigquery.Client()

# Set table_id to the ID of the table to create.
table_id = f'{client.project}.lines.lines'

# Function: fetch_route


def fetch_route(line_number: str) -> pd.DataFrame:
    '''
    Fetch the route for a given line number.

    Args:
        line_number (str): The line number.

    Returns:
        pd.DataFrame: The points defining the route.
    '''

    # 1. Perform request.

    url = f'https://transporteservico.urbs.curitiba.pr.gov.br/getShapeLinha.php?linha={line_number}&c={api_key_bus_data}'

    try:
        r = requests.request(
            method='GET',
            url=url,
            timeout=15
        )

        if r.status_code == 200:
            r_json = r.json()

            if len(r_json) > 0:
                # 2. Format data.

                route = pd.DataFrame()

                for point in r_json:
                    route = pd.concat(
                        objs=[
                            route,
                            pd.DataFrame(
                                data=point,
                                index=range(len(point))
                            )
                        ]
                    )

                route.reset_index(
                    drop=True,
                    inplace=True
                )

                route.rename(
                    mapper={
                        'SHP': 'route_id',
                        'LAT': 'latitude',
                        'LON': 'longitude',
                        'COD': 'line_number'
                    },
                    axis=1,
                    inplace=True
                )

                for column in ['latitude', 'longitude']:
                    route[column] = route[column].str.replace(',', '.')

                route = route.astype(
                    dtype={
                        'route_id': int,
                        'latitude': float,
                        'longitude': float,
                        'line_number': str
                    }
                )

                route = route[
                    [
                        'line_number',
                        'route_id',
                        'latitude',
                        'longitude'
                    ]
                ]

                # 3. Return data.

                return route

            else:
                print(f'Route request for line number {line_number} has resulted in an empty list.')
                return None

    except requests.Timeout:
        print('Route request has timed out.')
        return None

    except requests.JSONDecodeError:
        print('JSON decode error.')
        return None

    except requests.ConnectionError:
        print('Connection error.')
        return None

# Function: fetch_lines


def fetch_lines() -> pd.DataFrame:
    '''
    Fetch data on bus lines.

    Returns:
        pd.DataFrame: Data on bus lines.
    '''

    QUERY = f'''
    SELECT *
    FROM `{client.project}.lines.lines`
    '''

    query_job = client.query(QUERY)
    rows = query_job.result()
    lines = rows.to_dataframe()

    return lines

# Function: fetch_routes


def fetch_routes() -> pd.DataFrame:
    '''
    Fech all routes.

    Returns:
        pd.DataFrame: All bus routes.
    '''

    lines = fetch_lines()

    routes = pd.DataFrame()

    for index, row in lines.iterrows():
        routes = pd.concat(
            objs=[
                routes,
                fetch_route(line_number=row['line_number'])
            ]
        )

    return routes
