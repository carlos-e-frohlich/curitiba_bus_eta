import requests
import pandas as pd
from keys import api_key_bus_data
import os
from pathlib import Path

# Preamble.

current_dir = Path(os.path.realpath(__file__)).parent

# Function: fetch_lines.


def fetch_lines():
    '''
    Fetch information on bus lines and return it in a pandas DataFrame.

    Returns:
        DataFrame: A pandas Dataframe containing information on bus lines.
    '''

    # 1. Load alternative line names

    line_names = pd.read_json(
        path_or_buf=current_dir / 'line_names.json',
        orient='index'
    )

    line_names.reset_index(inplace=True)

    line_names.rename(
        mapper={'index': 'line_number'},
        axis=1,
        inplace=True
    )

    line_names.drop(
        labels='line_name_original',
        axis=1,
        inplace=True
    )

    # 2. Perform request.

    lines_url = 'https://transporteservico.urbs.curitiba.pr.gov.br/getLinhas.php?c={0}'

    url = lines_url.format(api_key_bus_data)

    try:
        r = requests.get(url, timeout=15)

        if r.status_code == 200:
            r_json = r.json()

            if len(r_json) > 0:
                # 3. Create lines.

                lines = pd.json_normalize(data=r_json)

                lines.rename(
                    mapper={
                        'COD': 'line_number',
                        'NOME': 'line_name_original',
                        'SOMENTE_CARTAO': 'fare_card_only',
                        'CATEGORIA_SERVICO': 'service_category',
                        'NOME_COR': 'color'
                    },
                    axis=1,
                    inplace=True
                )

                # 4. Set dtypes.

                lines = lines.astype(
                    dtype={
                        'line_number': str,
                        'fare_card_only': str,
                        'service_category': str,
                        'color': str
                    }
                )

                # 5. Add alternative line names.

                lines = lines.merge(
                    right=line_names,
                    how='left',
                    on='line_number'
                )

                # 6. Translate values into English.

                lines['fare_card_only'] = lines['fare_card_only'].replace(
                    to_replace={
                        'S': 'Yes',
                        'N': 'No'
                    }
                )

                lines['service_category'] = lines['service_category'].replace(
                    to_replace={
                        'CONVENCIONAL': 'Standard',
                        'ALIMENTADOR': 'Feeder',
                        'TRONCAL': 'Main line',
                        'LINHA DIRETA': 'Direct line',
                        'EXPRESSO': 'Express',
                        'INTERBAIRROS': 'Inter-neighborhood',
                        'LIGEIRÃO': 'Fast express',
                        'MADRUGUEIRO': 'Night owl',
                        'JARDINEIRA': 'Open top'
                    }
                )

                lines['color'] = lines['color'].replace(
                    to_replace={
                        'AMARELA': 'Yellow',
                        'LARANJA': 'Orange',
                        'PRATA': 'Silver',
                        'VERMELHA': 'Red',
                        'VERDE': 'Green',
                        'MADRUGUEIRO': 'Night owl',
                        'TURISMO': 'Tourism',
                    }
                )

                # 7. Re-order columns and return lines.

                lines = lines[
                    [
                        'line_number',
                        'line_name_original',
                        'line_name_short',
                        'line_name_long',
                        'fare_card_only',
                        'service_category',
                        'color'
                    ]
                ]

                return lines

            else:
                print('Request for lines has resulted in an empty list.')
                return None

        else:
            return None

    except requests.Timeout:
        print('Request has timed out.')
        return None

    except requests.JSONDecodeError:
        print('JSON decode error.')
        return None
