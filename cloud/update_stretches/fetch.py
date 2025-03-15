import requests
from keys import api_key_bus_data
import pandas as pd
from google.cloud import bigquery

# Construct a BigQuery client object.
client = bigquery.Client()

# Set table_id to the ID of the table to create.
table_id = f'{client.project}.lines.lines'

# Function: fetch_stretch


def fetch_stretch(line_number: str) -> pd.DataFrame:
    '''
    Fetch the stretch for a given line number.

    Args:
        line_number (str): The line number.

    Returns:
        pd.DataFrame: The the stretch the route.
    '''

    # 1. Perform request.

    url = f'https://transporteservico.urbs.curitiba.pr.gov.br/getTrechosItinerarios.php?linha={line_number}&c={api_key_bus_data}'

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

                stretch = pd.DataFrame()

                for point in r_json:
                    point = pd.json_normalize(point)
                    stretch = pd.concat(
                        objs=[
                            stretch,
                            point
                        ]
                    )

                stretch.reset_index(
                    drop=False,
                    inplace=True
                )

                stretch.rename(
                    mapper={
                        'COD_LINHA': 'line_number',
                        'NOME_LINHA': 'line_name_original',
                        'COD_CATEGORIA': 'service_category_code',
                        'NOME_CATEGORIA': 'service_category',
                        'COD_EMPRESA': 'company_code',
                        'NOME_EMPRESA': 'company',
                        'COD_PTO_PARADA_TH': 'stop_code_timetable',
                        'NOME_PTO_PARADA_TH': 'stop_name_timetable',
                        'SEQ_PTO_ITI_TH': 'order_itinerary',
                        'COD_ITINERARIO': 'itinerary_code',
                        'NOME_ITINERARIO': 'itinerary',
                        'PTO_ESPECIAL': 'special_stop',
                        'COD_PTO_TRECHO_A': 'point_code_stretch_start',
                        'SEQ_PONTO_TRECHO_A': 'point_order_start',
                        'COD_PTO_TRECHO_B': 'point_code_strech_end',
                        'SEQ_PONTO_TRECHO_B': 'point_order_end',
                        'EXTENSAO_TRECHO_A_ATE_B': 'distance_start_to_end',
                        'TIPO_TRECHO': 'stretch_type',
                        'STOP_CODE': 'stop_code',
                        'STOP_NAME': 'stop_name',
                        'CODIGO_URBS': 'urbs_code'
                    },
                    axis=1,
                    inplace=True
                )

                stretch['distance_start_to_end'] = stretch['distance_start_to_end'].str.replace(',', '.')

                stretch = stretch.astype(
                    dtype={
                        'line_number': str,
                        'line_name_original': str,
                        'service_category_code': str,
                        'service_category': str,
                        'company_code': str,
                        'company': str,
                        'stop_code_timetable': str,
                        'stop_name_timetable': str,
                        'order_itinerary': str,
                        'itinerary_code': str,
                        'itinerary': str,
                        'special_stop': str,
                        'point_code_stretch_start': str,
                        'point_order_start': str,
                        'point_code_strech_end': str,
                        'point_order_end': str,
                        'distance_start_to_end': float,
                        'stretch_type': str,
                        'stop_code': str,
                        'stop_name': str,
                        'urbs_code': str
                    }
                )

                stretch = stretch[
                    [
                        'line_number',
                        'line_name_original',
                        'service_category_code',
                        'service_category',
                        'company_code',
                        'company',
                        'stop_code_timetable',
                        'stop_name_timetable',
                        'order_itinerary',
                        'itinerary_code',
                        'itinerary',
                        'special_stop',
                        'point_code_stretch_start',
                        'point_order_start',
                        'point_code_strech_end',
                        'point_order_end',
                        'distance_start_to_end',
                        'stretch_type',
                        'stop_code',
                        'stop_name',
                        'urbs_code'
                    ]
                ]

                # 3. Return data.

                return stretch

            else:
                print(f'Stretch request for line number {line_number} has resulted in an empty list.')
                return None

    # 4. Handle errors.

    except requests.Timeout:
        print('Stretch request has timed out.')
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

# Function: fetch_stretches


def fetch_stretches() -> pd.DataFrame:
    '''
    Fetch all stretches.

    Returns:
        pd.DataFrame: All bus stretches.
    '''

    lines = fetch_lines()

    stretches = pd.DataFrame()

    for index, row in lines.iterrows():
        stretches = pd.concat(
            objs=[
                stretches,
                fetch_stretch(line_number=row['line_number'])
            ]
        )

    return stretches
