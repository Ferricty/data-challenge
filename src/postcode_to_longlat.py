'''
Convert the postcode to longitude and latitude coordinates and
return the results in a table or csv.
'''

from geopy.geocoders import Nominatim
import logging
import pandas as pd
import random
import requests
from time import sleep

# To hide WARNING:urllib3.connectionpool:Retrying
logging.getLogger(requests.packages.urllib3.__package__).setLevel(logging.ERROR)

user_agent = 'data-challenge_{}'.format(random.randint(10000,99999))
geolocator = Nominatim(user_agent=user_agent)

def get_longitude_latitude(postcode, geolocator = geolocator):
    """
    Convert the postcode to longitude and latitude coordinates and
    return the results.

    Args:
    - postcode (str/int): The postcode to convert to coordinates.
    - geolocator (Nominatim): The geolocator object for geocoding.

    Returns:
    - Tuple: A tuple containing the latitude and longitude coordinates.
    """
    location = geolocator.geocode({"postalcode": str(postcode), "country": 'Germany'}, country_codes = 'de')
    sleep(random.randint(50,150)/100)
    if location:
        return location.latitude, location.longitude
    else:
        return None, None
           
def processing_df_to_obtain_lat_long(df: pd.DataFrame, BATCH_SIZE: int) -> pd.DataFrame:
    """
    Process the dataframe to obtain latitude and longitude coordinates.

    Args:
    - df (pd.DataFrame): The input dataframe containing postcode data.
    - BATCH_SIZE (int): The batch size for processing.

    Returns:
    - pd.DataFrame: The dataframe with latitude and longitude coordinates added.
    """
    
    # We divide the process into batches (batch processing) to improve efficiency
    
    longitudes = []
    latitudes = []

    for index in range(0, len(df), BATCH_SIZE):
        
        # We obtain the longitudes and latitudes for the current batch
        coordinates = df['first_postcode'].iloc[index:index + BATCH_SIZE].apply(get_longitude_latitude)
        lat, lon = zip(*coordinates)  # We separate the latitudes and longitudes
        latitudes.extend(lat)
        longitudes.extend(lon)
        
    # We add the columns for latitude and longitude to the DataFrame
    df['latitude'] = latitudes
    df['longitude'] = longitudes

    return df