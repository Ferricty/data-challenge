"""
Write a function, that takes a postcode or city name and radius (in km) 
as input and returns all postcodes within the radius.
"""

from geopy.distance import geodesic
import os
import pandas as pd

project_dir = os.path.dirname(__file__)

df_city_latlong_output = os.path.join(os.path.dirname(project_dir),
                                    'data/output/df_city_latlong_output.csv')

df_city_details = pd.read_csv(df_city_latlong_output)

df_city_details['coordinate'] = df_city_details.apply(
                                                      lambda row:
                                                      (row["latitude"] , row["longitude"]),
                                                      axis=1,
                                                  )

city_coords = df_city_details[['name','coordinate']].set_index("name").T.to_dict('records')[0]

# Commented for improve readability

# city_coords = {
#              'Aßlar': (50.501544, 26.0705386),
#              'Aach': (47.84204984036061, 8.854742132732317),
#              'Aachen': (50.77643283176553, 6.086669673288873),
#              'Aalen': (48.83874607033844, 10.085620109546616),
#              'Abenberg': (49.23760620916201, 10.951095987821228),
#              'Abensberg': (58.22442178189655, 22.118634066810344),
#              ...}


def closer_cities(limit_distance: int|float,
                  origin_city: str = None,
                  postcode: str|int = None):
    """
    Find cities within a specified distance and criteria.

    Args:
    - limit_distance (int): The maximum distance in kilometers for the search.
    - origin_city (str): The city from which to search for nearby cities (optional).
    - postcode (int): The postcode to determine the city for the search (optional).

    Returns:
    - list: A list of nearby cities and their distances if found.

    Example:
    closer_cities(50, origin_city='Berlin')
    closer_cities(30, postcode=12345)

    - If origin_city is provided, it returns the nearby cities based on the specified city.
    - If postcode is provided, it returns the nearby cities based on the postcode.

    If neither origin_city nor postcode is provided, it prints a message indicating the missing parameters.
    If the provided postcode is not found in the dataset, it prints an error message.

    Helper function:
    The function helper_closer_city is used to find nearby cities based on the specified criteria.
    """
    if origin_city:
      # Criteria city
        return helper_closer_city(city_coords = city_coords, city_name = origin_city, limit_distance = limit_distance)

    elif postcode:
        # Criteria postcode
        try:
            # Get the city name
            city_name = df_city_details[df_city_details['searched_postcode'] == int(postcode)].name.to_list()[0]
            return helper_closer_city(city_coords, city_name, limit_distance)
        except IndexError:
            print("Postal code not found")
    else:
        print("You must provide limit_distance and (postcode or origin_city) parameters")




def helper_closer_city(city_coords: dict, city_name: str, limit_distance: int|float):
    """
    Find nearby cities based on the specified city and distance limitation.

    Args:
    - city_coords (dict): A dictionary containing city coordinates.
    - city_name (str): The name of the city for the search.
    - limit_distance (int): The maximum distance limit for city search.

    Returns:
    - list: A list of nearby cities and their distances if found.

    If city coordinates are not found, it prints an error message.
    """
    
    closer_cities = []
    origen_coords = city_coords.get(city_name)
    if origen_coords:
        for city, coords in city_coords.items():
            if city != city_name:
                distance = geodesic(origen_coords, coords).kilometers
                if distance <= limit_distance:
                    closer_cities.append((city, distance))
        return closer_cities
    else:
        print("City coordinates not found")