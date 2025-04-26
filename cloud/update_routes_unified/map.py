import pandas as pd
from fetch import fetch_extremities
from haversine import haversine, Unit

# Function: map_route_id_to_itinerary_id


def map_route_id_to_itinerary_id(
        line_routes: pd.DataFrame,
        line_stops: pd.DataFrame,
        route_id: int,
        tolerance: float
    ) -> str:
    """
    Map a route to an itinerary ID.

    Parameters
    ----------
    line_routes : pd.DataFrame
        The routes DataFrame output by fetch_line_routes_from_bq.
    line_stops : pd.DataFrame
        The stops DataFrame output by fetch_line_stops_from_bq.
    route_id : int
        A route ID.
    tolerance : float
        The tolerance, in meters, for defining two points as
        sufficiently close to one another.

    Returns
    -------
    str
        The itinerary ID associated with the given route ID.
    """
    # Fetch route extremities
    route_extremities = fetch_extremities(
        table=line_routes,
        id=route_id
    )

    # Fetch stop extremities.
    stops_extremities = {}

    itinerary_ids = line_stops['itinerary_id'].drop_duplicates().to_list()

    for itinerary_id in itinerary_ids:
        stops_extremities[itinerary_id] = fetch_extremities(
            table=line_stops,
            id=int(itinerary_id)
        )

    # Loop over and test stop extremities
    for key, value in stops_extremities.items():
        d_start = haversine(
            route_extremities['start'],
            value['start'],
            unit=Unit.METERS
        )
        if d_start <= tolerance:
            d_end = haversine(
                route_extremities['end'],
                value['end'],
                unit=Unit.METERS
            )
            if d_end <= tolerance:
                return key

    # Return None if no sufficiently close correspondence has been found
    return None
