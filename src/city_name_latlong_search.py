'''
Convert the postcode to longitude and latitude coordinates and
return the results in a table or csv.
'''

import logging
import requests
from time import sleep
import random
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderUnavailable



logging.getLogger(requests.packages.urllib3.__package__).setLevel(logging.ERROR)

user_agent = 'data-challenge_{}'.format(random.randint(10000,99999))
geolocator = Nominatim(user_agent=user_agent)

def get_longitude_latitude(city_name, geolocator = geolocator):
    """
    Convert the city_name to longitude and latitude coordinates and
    return the results.

    Args:
    - city_name (str): The postcode to convert to coordinates.
    - geolocator (Nominatim): The geolocator object for geocoding.

    Returns:
    - Tuple: A tuple containing the latitude and longitude coordinates.
    """
       
    retry_limit = 5
    retries = 0
    while retries < retry_limit:
        try:
            location = geolocator.geocode({"city": str(city_name), "country": 'Germany'}, country_codes = 'de')
            sleep(random.randint(50,150)/100)
            print(city_name, location)
            if location:
                return location.latitude, location.longitude
            else:
                return None, None
        except GeocoderUnavailable as e:
            print(f"Error: {e}")
            retries += 1
            if retries < retry_limit:
                print(f"Wait 5 second...")
                sleep(5)  # Wait
            else:
                print("Max retries exceeded")




def processing_city_name_to_obtain_lat_long(df, BATCH_SIZE):
    """
    Process the dataframe to obtain latitude and longitude coordinates.

    Args:
    - df (pd.DataFrame): The input dataframe containing city_name data.
    - BATCH_SIZE (int): The batch size for processing.

    Returns:
    - pd.DataFrame: The dataframe with latitude and longitude coordinates added.
    """

    # We divide the process into batches (batch processing) to improve efficiency

    longitudes = []
    latitudes = []
    
    for index in range(0, len(df), BATCH_SIZE):
        progress = round((index//BATCH_SIZE + 1) / (len(df)//BATCH_SIZE + 1) * 100, 2)
        print(f"Latitude and Longitude progress: {progress} %")
        # We obtain the longitudes and latitudes for the current batch
        
        coordinates = df['city_name'].iloc[index:index + BATCH_SIZE].apply(get_longitude_latitude)
        lat, lon = zip(*coordinates)  # We separate the latitudes and longitudes
        latitudes.extend(lat)
        longitudes.extend(lon)

    # We add the columns for latitude and longitude to the DataFrame
    df['latitude'] = latitudes
    df['longitude'] = longitudes

    return df
