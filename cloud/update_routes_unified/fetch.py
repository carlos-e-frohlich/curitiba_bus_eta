from google.cloud import bigquery
import pandas as pd

# Preamble.
client = bigquery.Client()

# Function: fetch_lines_from_bq


def fetch_lines_from_bq() -> pd.DataFrame:
    '''
    Fetch lines from BigQuery.

    Returns
    -------
    pd.DataFrame
        The lines fetched from BigQuery.
    '''

    query = '''
    SELECT *
    FROM lines.lines
    '''

    lines = client.query_and_wait(query=query).to_dataframe()

    return lines

# Function: fetch_routes_from_bq


def fetch_line_routes_from_bq(
        line_number: str,
        sort=True
    ) -> pd.DataFrame:
    '''
    Fetch routes for a specified line number from Bigquery.

    Parameters
    ----------
    line_number : str
        The line number whose routes are to be fetched.
    sort : bool, optional
        If True, the results are sorted by route ID and then by order,
        by default True.

    Returns
    -------
    pd.DataFrame
        The routes for a specified line number fetched from BigQuery.
    '''

    query = f'''
    SELECT *
    FROM routes.routes
    WHERE line_number = \'{line_number}\'
    '''

    routes = client.query_and_wait(query=query).to_dataframe()

    if sort:
        routes.sort_values(
            by=[
                'route_id',
                'order'
            ],
            inplace=True,
            ignore_index=True
        )

    return routes

# Function: fetch_line_stops_from_bq


def fetch_line_stops_from_bq(
        line_number: str,
        sort=True
    ) -> pd.DataFrame:
    '''
    Fetch stops for a specified line number from Bigquery.

    Parameters
    ----------
    line_number : str
        The line number whose stops are to be fetched.
    sort : bool, optional
        If True, the results are sorted by itinerary ID and then by
        order, by default True.

    Returns
    -------
    pd.DataFrame
        The stops for a specified line number fetched from BigQuery.
    '''

    query = f'''
    SELECT *
    FROM routes.stops
    WHERE line_number = \'{line_number}\'
    '''

    stops = client.query_and_wait(query=query).to_dataframe()

    if sort:
        stops.sort_values(
            by=[
                'itinerary_id',
                'order'
            ],
            inplace=True,
            ignore_index=True
        )

    return stops

# Function: fetch_extremities


def fetch_extremities(
        table: pd.DataFrame,
        id: int) -> dict:
    """
    Get extremities relative to (line_number, route_id) or
    (line_number, itinerary_id).

    Parameters
    ----------
    table : pd.DataFrame
        The routes or stops associated with a specific line number.
    id : int
        The route or itinerary ID under consideration.

    Returns
    -------
    dict
        The start and end relative to the pair (line_number, route_id)
        or (line_number, itinerary_id).
    """

    # Keep only route ID under consideration
    if 'route_id' in table.columns:
        table = table.loc[table['route_id'] == id]
    elif 'itinerary_id' in table.columns:
        id = str(id)
        table = table.loc[table['itinerary_id'] == id]

    # Get start
    start = table.loc[
        table['order'] == table['order'].min(),
        ['latitude', 'longitude']
    ]
    start = start.squeeze()
    start = tuple(start)

    # Get end
    end = table.loc[
        table['order'] == table['order'].max(),
        ['latitude', 'longitude']
    ]
    end = end.squeeze()
    end = tuple(end)

    # Prepare and return results
    extremities = {
        'start': start,
        'end': end
    }

    return extremities
