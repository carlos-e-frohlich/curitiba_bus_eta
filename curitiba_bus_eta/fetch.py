from convert import resolve_update_datetime
import keyring
import requests
import datetime as dt
import pytz
import pandas as pd
import numpy as np

# Function: fetch_locations_batch


def fetch_locations_batch(route_number: int) -> pd.DataFrame:
    '''
    Return a pandas DataFrame containing the latest bus locations.

    Arguments:
        route_number -- The route number.
    '''

    # 1. Perform request

    api_key = keyring.get_password('api_curitiba_156', 'carlos.e.frohlich@gmail.com')

    positions_url = 'https://transporteservico.urbs.curitiba.pr.gov.br/getVeiculos.php?linha={0}&c={1}'

    route_number = str(route_number).zfill(3)

    url = positions_url.format(route_number, api_key)

    r = requests.get(url)

    r_json = r.json()

    # 2. Create locations_batch

    locations_batch = pd.DataFrame()

    request_datetime = r.headers['Date']
    request_datetime = dt.datetime.strptime(request_datetime, '%a, %d %b %Y %H:%M:%S GMT')
    request_datetime = pytz.timezone('America/Sao_Paulo').localize(request_datetime)

    vehicles = list(r_json.keys())

    for vehicle in vehicles:
        row = pd.json_normalize(r_json[vehicle])

        locations_batch = pd.concat(
            objs=[locations_batch, row],
            axis=0,
            ignore_index=True
        )

    locations_batch.rename(
        mapper={
            'COD': 'fleet_number',
            'REFRESH': 'update_time',
            'LAT': 'latitude',
            'LON': 'longitude',
            'CODIGOLINHA': 'route_number',
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

    # 3. Resolve update_datetime

    locations_batch['update_datetime'] = locations_batch['update_time'].apply(lambda x: dt.datetime.strptime(x, '%H:%M').replace(year=request_datetime.year, month=request_datetime.month, day=request_datetime.day))

    locations_batch['update_datetime'] = locations_batch['update_datetime'].apply(lambda x: x.replace(tzinfo=pytz.timezone('America/Sao_Paulo')))

    locations_batch['update_datetime'] = locations_batch['update_datetime'].apply(lambda x: resolve_update_datetime(request_datetime=request_datetime, update_time=x.to_pydatetime().time()))

    # 4. Set dtypes

    for column in ['latitude', 'longitude']:
        locations_batch[column] = pd.to_numeric(locations_batch[column], errors='coerce').astype(dtype=np.float32)

    locations_batch['route_number'] = pd.to_numeric(locations_batch['route_number'], errors='coerce').astype(dtype=pd.UInt16Dtype())

    locations_batch['wheelchair_accessibility'] = pd.to_numeric(locations_batch['wheelchair_accessibility'], errors='coerce').astype(dtype=pd.UInt8Dtype())

    locations_batch['timetable'] = pd.to_numeric(locations_batch['timetable'], errors='coerce').astype(pd.UInt8Dtype())

    locations_batch['cycle_count'] = pd.to_numeric(locations_batch['cycle_count'], errors='coerce').astype(pd.UInt8Dtype())

    # 5. Translate values into English

    locations_batch['bus_type'] = locations_batch['bus_type'].replace(
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

    locations_batch['status_time'] = locations_batch['status_time'].replace(
        to_replace={
            'ADIANTADO': 'Early',
            'ATRASADO': 'Delayed',
            'NO HORÁRIO': 'On time',
            'NÃO CONFORMIDADE': 'Nonconformance'
        }
    )

    locations_batch['status_route'] = locations_batch['status_route'].replace(
        to_replace={
            'REALIZANDO ROTA': 'On route',
            'TIPO INCOMPATIVEL': 'Incompatible type',
            'FALHA DE GPS': 'GPS failure',
            'COM MENSAGEM NÃO LIDA': 'Has unread message'
        }
    )

    locations_batch['direction'] = locations_batch['direction'].replace(
        to_replace={
            'IDA': 'Outbound',
            'VOLTA': 'Inbound',
            'CIRCULAR': 'Circular'
        }
    )

    locations_batch['destination'] = locations_batch['destination'].replace(to_replace={'sem tabela': 'No timetable'})

    # 6. Re-order columns and return locations_batch

    locations_batch = locations_batch[
        [
            'fleet_number',
            'update_datetime',
            'latitude',
            'longitude',
            'route_number',
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

    return locations_batch

# Function: fetch_weather_batch


def fetch_weather_batch(latitude: float, longitude: float) -> pd.DataFrame:
    '''
    Fetch and return current weather data.

    Arguments:
        latitude -- The latitude for which weather data is to be fetched.
        longitude -- The longitude for which weather data is to be fetched.
    '''

    # 1. Define URL

    api_key = keyring.get_password('OpenWeatherMap', 'carlos.e.frohlich')

    url = 'https://api.openweathermap.org/data/2.5/weather?lat={latitude}&lon={longitude}&appid={api_key}&units=metric'

    url = url.format(
        latitude=latitude,
        longitude=longitude,
        api_key=api_key
    )

    # 2. Perform request

    r = requests.get(url)

    # 3. Create and format DataFrame

    level_0 = [
        'base',
        'visibility',
        'dt',
        'timezone',
        'id',
        'name',
        'cod'
    ]

    level_1 = [
        'coord',
        'weather',
        'main',
        'wind',
        'clouds',
        'sys'
    ]

    weather_batch = pd.concat(
        objs=[pd.json_normalize(r.json())[level_0]] + [pd.json_normalize(r.json()[key]) for key in level_1],
        axis=1
    )

    weather_batch.columns = [
        'base',
        'visibility',
        'datetime',
        'timezone',
        'id',
        'name',
        'code',
        'longitude',
        'latitude',
        'weather_id',
        'weather_main',
        'weather_description',
        'weather_icon_id',
        'temperature',
        'feels_like',
        'temperature_minimum',
        'temperature_maximum',
        'pressure',
        'humidity',
        'wind_speed',
        'wind_direction',
        'cloudiness',
        'sys_type',
        'sys_id',
        'country',
        'sunrise_datetime',
        'sunset_datetime'
    ]

    weather_batch = weather_batch[
        [
            'longitude',
            'latitude',
            'weather_id',
            'weather_main',
            'weather_description',
            'weather_icon_id',
            'base',
            'temperature',
            'feels_like',
            'temperature_minimum',
            'temperature_maximum',
            'pressure',
            'humidity',
            'visibility',
            'wind_speed',
            'wind_direction',
            'cloudiness',
            'datetime',
            'sys_type',
            'sys_id',
            'country',
            'sunrise_datetime',
            'sunset_datetime',
            'timezone',
            'id',
            'name',
            'code',
        ]
    ]

    # 4. Convert datetimes from UNIX to datetime

    datetime_columns = ['datetime', 'sunrise_datetime', 'sunset_datetime']

    timezone = weather_batch['timezone'].squeeze()
    timezone = dt.timedelta(seconds=int(timezone))
    timezone = dt.timezone(timezone)

    for column in datetime_columns:
        timestamp = weather_batch[column].squeeze()
        timestamp = dt.datetime.fromtimestamp(int(timestamp), tz=timezone)

        weather_batch[column] = timestamp

    # 5. Set dtypes

    weather_batch = weather_batch.astype(
        dtype={
            'longitude': np.float64,
            'latitude': np.float64,
            'weather_id': pd.UInt16Dtype(),
            'weather_main': str,
            'weather_description': str,
            'weather_icon_id': str,
            'base': str,
            'temperature': np.float16,
            'feels_like': np.float16,
            'temperature_minimum': np.float16,
            'temperature_maximum': np.float16,
            'pressure': pd.UInt16Dtype(),
            'humidity': pd.UInt16Dtype(),
            'visibility': pd.UInt16Dtype(),
            'wind_speed': np.float16,
            'wind_direction': pd.UInt16Dtype(),
            'cloudiness': pd.UInt8Dtype(),
            'datetime': 'datetime64[ms, UTC-03:00]',
            'sys_type': pd.UInt16Dtype(),
            'sys_id': pd.UInt32Dtype(),
            'country': str,
            'sunrise_datetime': 'datetime64[ms, UTC-03:00]',
            'sunset_datetime': 'datetime64[ms, UTC-03:00]',
            'timezone': pd.Int16Dtype(),
            'id': pd.Int32Dtype(),
            'name': str,
            'code': pd.UInt16Dtype(),
        }
    )

    # 6. Return weather batch DataFrame

    return weather_batch
