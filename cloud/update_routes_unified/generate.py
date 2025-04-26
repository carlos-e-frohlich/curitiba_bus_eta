from map import map_route_id_to_itinerary_id
from calculate import calculate_distance_between_line_segment_and_point
import pandas as pd
import numpy as np

# Function: generate_route_unified_route_id


def generate_route_unified_route_id(
        line_number: str,
        route_id: int,
        line_routes: pd.DataFrame,
        line_stops: pd.DataFrame,
        tolerance=200
    ) -> pd.DataFrame:
    """
    Generate unified route in the context of a specified route ID.

    Parameters
    ----------
    line_number : str
        The line number whose unified route is to be generated.
    route_id : int
        The route ID providing the context of the unified route.
    line_routes : pd.DataFrame
        The routes DataFrame output by fetch_line_routes_from_bq.
    line_stops : pd.DataFrame
        The stops DataFrame output by fetch_line_stops_from_bq.
    tolerance : float, optional
        The tolerance, in meters, for defining two points as
        sufficiently close to one another, by default 200.

    Returns
    -------
    pd.DataFrame
        A unified route in the context of a specified route ID.
    """

    # Extract context for the route
    line_route = line_routes.loc[line_routes['route_id'] == route_id]
    line_route.reset_index(drop=True, inplace=True)

    # Extract context for the stops
    itinerary_id = map_route_id_to_itinerary_id(
        line_routes=line_route,
        line_stops=line_stops,
        route_id=route_id,
        tolerance=tolerance
    )

    line_stops = line_stops.loc[line_stops['itinerary_id'] == itinerary_id]

    line_stops.reset_index(drop=True, inplace=True)

    line_stops = line_stops[
        [
            'line_number',
            'number',
            'type',
            'name',
            'direction',
            'latitude',
            'longitude'
        ]
    ]

    # Find closest route point to each stop
    stops_and_minimum_distances = pd.DataFrame()

    for index, row in line_stops.iterrows():
        routes_and_distances = pd.DataFrame()
        for jndex, sow in line_route.iterrows():
            if jndex > 0:
                distance = calculate_distance_between_line_segment_and_point(
                    A=(line_route.loc[jndex - 1, 'latitude'], line_route.loc[jndex - 1, 'longitude']),
                    B=(line_route.loc[jndex, 'latitude'], line_route.loc[jndex, 'longitude']),
                    S=(row['latitude'], row['longitude'])
                )
                distance_row = pd.DataFrame(
                    data={
                        'line_number': [line_number],
                        'route_id': [route_id],
                        'order': [line_route.loc[jndex - 1, 'order']],
                        'latitude': [row['latitude']],
                        'longitude': [row['longitude']],
                        'point_type': [row['type']],
                        'stop_number': [row['number']],
                        'stop_name': [row['name']],
                        'direction': [row['direction']],
                        'distance': [distance],
                    }
                )
                routes_and_distances = pd.concat(
                    objs=[
                        routes_and_distances,
                        distance_row
                    ]
                )

        routes_and_distances = routes_and_distances.loc[
            routes_and_distances['distance'] == routes_and_distances['distance'].min()
        ]

        stops_and_minimum_distances = pd.concat(
            objs=[
                stops_and_minimum_distances,
                routes_and_distances
            ],
            ignore_index=True
        )

    stops_and_minimum_distances.drop(
        labels='distance',
        axis=1,
        inplace=True
    )

    # Combine route and stops
    unified_route_route_id = pd.concat(
        objs=[
            line_route,
            stops_and_minimum_distances
        ],
        ignore_index=True
    )

    unified_route_route_id['direction'] = stops_and_minimum_distances['direction'].drop_duplicates().squeeze()

    unified_route_route_id.sort_values(
        by=[
            'order',
            'stop_number'
        ],
        inplace=True,
        na_position='first',
        ignore_index=True
    )

    unified_route_route_id['order'] = unified_route_route_id.index

    # Drop points before first stop and after last stop
    indices = []

    sortings = [
        unified_route_route_id,
        unified_route_route_id.sort_values(by='order', ascending=False)
    ]

    for sorting in sortings:
        for index, row in sorting.iterrows():
            inclusion_test = row['stop_number'] in line_stops['number'].values
            if not inclusion_test:
                indices.append(index)
            else:
                break

    unified_route_route_id.drop(
        labels=indices,
        inplace=True
    )

    unified_route_route_id.reset_index(inplace=True)

    unified_route_route_id['order'] = unified_route_route_id.index

    # Return results
    return unified_route_route_id

# Function: generate_route_unified


def generate_route_unified(
        line_number: str,
        line_routes: pd.DataFrame,
        line_stops: pd.DataFrame
    ) -> pd.DataFrame:
    """
    Generate unified route.

    Parameters
    ----------
    line_number : str
        The line number whose unified route is to be generated.
    line_routes : pd.DataFrame
        The routes DataFrame output by fetch_line_routes_from_bq.
    line_stops : pd.DataFrame
        The stops DataFrame output by fetch_line_stops_from_bq.

    Returns
    -------
    pd.DataFrame
        A unified route.
    """

    # Loop over all route IDs and generate unified routes in the
    # context of each route ID
    route_ids = line_routes['route_id'].drop_duplicates().to_list()

    unified_route = pd.DataFrame()

    for route_id in route_ids:
        unified_route_route_id = generate_route_unified_route_id(
            line_number=line_number,
            route_id=route_id,
            line_routes=line_routes,
            line_stops=line_stops
        )

        unified_route = pd.concat(
            objs=[
                unified_route,
                unified_route_route_id
            ],
            ignore_index=True
        )

    unified_route.drop(
        labels='route_id',
        axis=1,
        inplace=True
    )

    # Set order
    unified_route['order'] = unified_route.index

    # Return results
    return unified_route
