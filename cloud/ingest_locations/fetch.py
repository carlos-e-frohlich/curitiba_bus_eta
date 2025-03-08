import requests
import datetime as dt
import pytz
import pandas as pd
import numpy as np
from convert import resolve_update_datetime
from keys import api_key_bus_data
from google.cloud import bigquery
import datetime as dt
import pytz
import pandas as pd

# Preamble.

client = bigquery.Client()

# Function: fetch_locations_batch.


def fetch_set_of_locations(
        line_number: str,
    ) -> pd.DataFrame:
    """
    Return a pandas Dataframe containing the latest set of bus locations.

    Args:
        line_number (str): The line number.

    Returns:
        pd.DataFrame: Latest bus locations and other data.
    """

    # 1. Perform request.

    positions_url = 'https://transporteservico.urbs.curitiba.pr.gov.br/getVeiculos.php?linha={0}&c={1}'

    url = positions_url.format(line_number, api_key_bus_data)

    try:
        r = requests.get(url, timeout=15)

        if r.status_code == 200:
            r_json = r.json()

            if len(r_json) > 0:
                # 2. Create locations_batch.

                set_of_locations = pd.DataFrame()

                request_datetime = r.headers['Date']
                request_datetime = dt.datetime.strptime(request_datetime, '%a, %d %b %Y %H:%M:%S GMT')
                request_datetime = pytz.timezone('America/Sao_Paulo').localize(request_datetime)

                vehicles = list(r_json.keys())

                for vehicle in vehicles:
                    row = pd.json_normalize(r_json[vehicle])

                    set_of_locations = pd.concat(
                        objs=[set_of_locations, row],
                        axis=0,
                        ignore_index=True
                    )

                set_of_locations.rename(
                    mapper={
                        'COD': 'fleet_number',
                        'REFRESH': 'update_time',
                        'LAT': 'latitude',
                        'LON': 'longitude',
                        'CODIGOLINHA': 'line_number',
                        'ADAPT': 'wheelchair_accessibility',
                        'TIPO_VEIC': 'bus_type',
                        'TABELA': 'timetable',
                        'SITUACAO': 'status_time',
                        'SITUACAO2': 'status_route',
                        'SENT': 'direction',
                        'TCOUNT': 'cycle_count',
                        'SENTIDO': 'destination'
                    },
                    axis=1,
                    inplace=True
                )

                # 3. Resolve update_datetime.

                set_of_locations['update_datetime'] = set_of_locations['update_time'].apply(lambda x: dt.datetime.strptime(x, '%H:%M').replace(year=request_datetime.year, month=request_datetime.month, day=request_datetime.day))

                set_of_locations['update_datetime'] = set_of_locations['update_datetime'].apply(lambda x: x.replace(tzinfo=pytz.timezone('America/Sao_Paulo')))

                set_of_locations['update_datetime'] = set_of_locations['update_datetime'].apply(lambda x: resolve_update_datetime(request_datetime=request_datetime, update_time=x.to_pydatetime().time()))

                # 4. Set dtypes.

                for column in ['latitude', 'longitude']:
                    set_of_locations[column] = pd.to_numeric(set_of_locations[column], errors='coerce').astype(dtype=np.float32)

                set_of_locations['wheelchair_accessibility'] = pd.to_numeric(set_of_locations['wheelchair_accessibility'], errors='coerce').astype(dtype=pd.UInt8Dtype())

                set_of_locations['timetable'] = pd.to_numeric(set_of_locations['timetable'], errors='coerce').astype(pd.UInt8Dtype())

                set_of_locations['cycle_count'] = pd.to_numeric(set_of_locations['cycle_count'], errors='coerce').astype(pd.UInt8Dtype())

                # 5. Translate values into English.

                set_of_locations['bus_type'] = set_of_locations['bus_type'].replace(
                    to_replace={
                        '1': 'Standard', # 'COMUM',
                        '2': 'Semi-padron', # 'SEMI PADRON',
                        '3': 'Padron', # 'PADRON',
                        '4': 'Articulated', # 'ARTICULADO',
                        '5': 'Bi-articulated', # 'BIARTICULADO',
                        '6': 'Microbus', # 'MICRO',
                        '7': 'Special microbus', # 'MICRO ESPECIAL',
                        '8': 'Biofuel bi-articulated', # 'BIARTIC. BIO',
                        '9': 'Biofuel articulated', # 'ARTIC. BIO',
                        '10': 'Hybrid', # 'HIBRIDO',
                        '11': 'Biofuel hybrid', # 'HIBRIDO BIO',
                        '12': 'Electric', # 'ELÉTRICO'
                    }
                )

                set_of_locations['status_time'] = set_of_locations['status_time'].replace(
                    to_replace={
                        'ADIANTADO': 'Early',
                        'ATRASADO': 'Delayed',
                        'NO HORÁRIO': 'On time',
                        'NÃO CONFORMIDADE': 'Nonconformance'
                    }
                )

                set_of_locations['status_route'] = set_of_locations['status_route'].replace(
                    to_replace={
                        'REALIZANDO ROTA': 'On route',
                        'TIPO INCOMPATIVEL': 'Incompatible type',
                        'FALHA DE GPS': 'GPS failure',
                        'COM MENSAGEM NÃO LIDA': 'Has unread message'
                    }
                )

                set_of_locations['direction'] = set_of_locations['direction'].replace(
                    to_replace={
                        'IDA': 'Outbound',
                        'VOLTA': 'Inbound',
                        'CIRCULAR': 'Circular'
                    }
                )

                set_of_locations['destination'] = set_of_locations['destination'].replace(to_replace={'sem tabela': 'No timetable'})

                # 6. Re-order columns and return locations_batch.

                set_of_locations = set_of_locations[
                    [
                        'fleet_number',
                        'update_datetime',
                        'latitude',
                        'longitude',
                        'line_number',
                        'wheelchair_accessibility',
                        'bus_type',
                        'timetable',
                        'status_time',
                        'status_route',
                        'direction',
                        'cycle_count',
                        'destination',
                    ]
                ]

                return set_of_locations

            else:
                print('Request for locations has resulted in an empty list.')
                return None

        else:
            print('Request status code != 200')
            return None

    except requests.Timeout:
        print('Request has timed out.')
        return None

    except requests.JSONDecodeError:
        print('JSON decode error.')
        return None

# Function: convert_to_timezone


def convert_to_timezone(
        datetime: dt.datetime,
        tz_0='UTC',
        tz_1='America/Sao_Paulo',
    ) -> dt.datetime:
    '''
    Convert a datetime between two specified timezones.

    Args:
        datetime (dt.datetime): The datetime to be converted. Must be aware.
        tz_0 (str): The timezone from which to convert. Defaults to 'UTC'.
        tz_1 (str): The timezone to which to convert. Defaults to
            'America/Sao_Paulo'.

    Returns:
        dt.datetime: The converted datetime.
    '''

    datetime = datetime.replace(tzinfo=pytz.timezone(tz_0)).astimezone(pytz.timezone(tz_1))

    return datetime

# Function: fetch_recent_locations


def fetch_recent_locations(hours: int) -> pd.DataFrame:
    '''
    Fetch all locations in locations.locations within the last specified
    number of hours.

    Args:
        hours (int): The lenght of the time window including locations.

    Returns:
        pd.DataFrame: A DataFrame containing all locations within the
        last specified number of hours
    '''

    # Preamble.

    datetime_string_format = '%Y-%m-%d %H:%M:%S.%f%z'

    # Set current time. Cloud Functions uses UTC.
    now_utc = dt.datetime.now()
    now_utc = pytz.timezone('UTC').localize(now_utc)

    # Convert current time from UTC to America/Sao_Paulo.
    now_local = now_utc.replace(tzinfo=pytz.utc).astimezone(pytz.timezone('America/Sao_Paulo'))

    # Get string for current time minus specified time window.
    then_local = now_local - dt.timedelta(hours=hours)
    then_local = then_local.strftime(datetime_string_format)

    query = '''
    SELECT *
    FROM locations.locations
    WHERE update_datetime >= CAST('{0}' AS TIMESTAMP)
    ORDER BY update_datetime DESC
    '''.format(then_local)

    recent_locations = client.query_and_wait(query=query).to_dataframe()

    recent_locations['update_datetime'] = recent_locations['update_datetime'].apply(convert_to_timezone)

    return recent_locations


# Function: drop_potential_location_duplicates


def drop_potential_location_duplicates(
        recent_locations: pd.DataFrame,
        set_of_locations: pd.DataFrame
    ) -> pd.DataFrame:
    '''
    Eliminate from a set of locations the observations in locations.locations.

    Args:
        recent_locations (pd.DataFrame): The most recent locations in
            locations.locations.
        set_of_locations (pd.DataFrame): The set of locations fetch from
            the API.

    Returns:
        pd.DataFrame: The set of locations, the observations already in
            locations.locations dropped.
    '''
    if recent_locations.shape[0] == 0:
        return set_of_locations

    elif set_of_locations is not None:
        fleet_number_condition = set_of_locations['fleet_number'].isin(recent_locations['fleet_number'])
        update_datetime_condition = set_of_locations['update_datetime'].isin(recent_locations['update_datetime'])
        latitude_condition = set_of_locations['latitude'].isin(recent_locations['latitude'])
        longitude_condition = set_of_locations['longitude'].isin(recent_locations['longitude'])
        condition = fleet_number_condition * update_datetime_condition * latitude_condition * longitude_condition

        set_of_locations = set_of_locations.loc[~condition]

        return set_of_locations

    else:
        return None
