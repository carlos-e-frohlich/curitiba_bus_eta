from haversine import haversine, Unit
import numpy as np

# Function: calculate_distance_between_line_segment_and_point


def calculate_distance_between_line_segment_and_point(
        A: tuple,
        B: tuple,
        S: tuple
    ) -> float:
    '''
    Given a line segment l between points A and B, and a point S,
    calculate the distance, in meters, between l and S.

    Parameters
    ----------
    A : tuple
        (latitude, longitude) for point A.
    B : tuple
        (latitude, longitude) for point B.
    S : tuple
        (latitude, longitude) for point S.

    Returns
    -------
    float
        The distance, in meters, between line l and point S.
    '''

    # Convert points to vectors represented by numpy.ndarray
    A = np.array(object=A)
    B = np.array(object=B)
    S = np.array(object=S)

    # Calculate slope and intercept of line l passing through A and B
    slope = (B[1] - A[1]) / (B[0] - A[0])
    intercept = B[1] - slope * B[0]

    # Calculate slope and intercept of line l' passing through S and
    # perpendicular to l
    slope_prime = -1 / slope
    intercept_prime = S[1] - slope_prime * S[0]

    # Find intersection of l and l'
    intersection = (
        (intercept - intercept_prime) / (slope_prime - slope),
        slope * (intercept - intercept_prime) / (slope_prime - slope) + intercept
    )
    T = np.array(object=intersection)

    # Determine if point S is enclosed by two lines perpendicular to the
    # line segment l
    v1 = (T - A) / np.linalg.norm(T - A)
    v2 = (T - B)  / np.linalg.norm(T - B)

    enclosed = np.allclose(v1 + v2, np.zeros(shape=(2,)), rtol=1e-6, atol=1e-6)
    non_enclosed = np.allclose(v1, v2, rtol=1e-6, atol=1e-6)

    # Calculate distance between S and l
    if enclosed:
        distance = haversine(S, intersection, Unit.METERS)
    elif non_enclosed:
        distance = np.min(
            [
                np.linalg.norm(S - A),
                np.linalg.norm(S - B)
            ]
        )
        distance = float(distance)

    # Return distance
    return distance
