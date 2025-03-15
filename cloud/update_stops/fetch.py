import requests
from keys import api_key_bus_data
import pandas as pd
from google.cloud import bigquery

# Construct a BigQuery client object.
client = bigquery.Client()

# Set table_id to the ID of the table to create.
table_id = f'{client.project}.routes.stops'

# Function: fetch_line_stops


def fetch_line_stops(line_number: str) -> pd.DataFrame:
    '''
    Fetch the stops for a given line number.

    Args:
        line_number (str): The line number.

    Returns:
        pd.DataFrame: The stops of the route.
    '''

    # 1. Perform request.

    url = f'https://transporteservico.urbs.curitiba.pr.gov.br/getPontosLinha.php?linha={line_number}&c={api_key_bus_data}'

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

                stops = pd.DataFrame()

                index = 0

                for point in r_json:
                    stops = pd.concat(
                        objs=[
                            stops,
                            pd.DataFrame(
                                data=point,
                                index=[index]
                            )
                        ]
                    )

                    index += 1

                stops.reset_index(
                    drop=True,
                    inplace=True
                )

                stops.rename(
                    mapper={
                        'NOME': 'name',
                        'NUM': 'number',
                        'LAT': 'latitude',
                        'LON': 'longitude',
                        'SEQ': 'order',
                        'GRUPO': 'group',
                        'SENTIDO': 'direction',
                        'TIPO': 'type',
                        'ITINERARY_ID': 'itinerary_id'
                    },
                    axis=1,
                    inplace=True
                )

                stops['line_number'] = line_number

                for column in ['latitude', 'longitude']:
                    stops[column] = stops[column].str.replace(',', '.')

                stops = stops.astype(
                    dtype={
                        'name': str,
                        'number': str,
                        'latitude': str,
                        'longitude': str,
                        'order': int,
                        'group': str,
                        'direction': str,
                        'type': str,
                        'itinerary_id': str,
                        'line_number': str
                    }
                )

                stops = stops[
                    [
                        'line_number',
                        'itinerary_id',
                        'group',
                        'number',
                        'name',
                        'type',
                        'order',
                        'direction',
                        'latitude',
                        'longitude'
                    ]
                ]

                # 3. Return data.

                return stops

            else:
                print(f'Stops request for line number {line_number} has resulted in an empty list.')
                return None

    # 4. Handle errors.

    except requests.Timeout:
        print('Stops request has timed out.')
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

# Function: fetch stops.


def fetch_stops() -> pd.DataFrame:
    '''
    Fetch all bus stops.

    Returns:
        pd.DataFrame: All bus stops.
    '''

    lines = fetch_lines()

    stops = pd.DataFrame()

    for index, row in lines.iterrows():
        stops = pd.concat(
            objs=[
                stops,
                fetch_line_stops(line_number=row['line_number'])
            ]
        )

    return stops
