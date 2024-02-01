"""
Write a function, that takes a postcode or city name and radius (in km) 
as input and returns all postcodes within the radius.
"""


from geopy.distance import geodesic
import pandas as pd


df_city_details = pd.read_csv('data/output/city_details.csv')

df_city_details['coordinate'] = df_city_details.apply(
                                                      lambda row:
                                                      (row["latitude"] , row["longitude"]),
                                                      axis=1,
                                                  )

city_coords = df_city_details[['name','coordinate']].set_index("name").T.to_dict('records')[0]

# # Commented for improve readability

# # city_coords = {
# #              'Aßlar': (50.501544, 26.0705386),
# #              'Aach': (47.84204984036061, 8.854742132732317),
# #              'Aachen': (50.77643283176553, 6.086669673288873),
# #              'Aalen': (48.83874607033844, 10.085620109546616),
# #              'Abenberg': (49.23760620916201, 10.951095987821228),
# #              'Abensberg': (58.22442178189655, 22.118634066810344),
# #              ...}


def closer_cities(limit_distance: int,
                  origin_city = None,
                  postcode = None):

    if origin_city:
      # Criteria city
        return helper_closer_city(city_coords = city_coords, city_name = origin_city, limit_distance = limit_distance)

    elif postcode:
        # Criteria postcode
        try:
            # Get the city name
            city_name = df_city_details[df_city_details['searched_postcode'] == postcode].name.to_list()[0]
            return helper_closer_city(city_coords, city_name, limit_distance)
        except IndexError:
            print("Postal code not found")
    else:
        print("You must provide limit_distance and (postcode or origin_city) parameters")




def helper_closer_city(city_coords, city_name, limit_distance):
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