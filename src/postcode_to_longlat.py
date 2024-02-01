
'''
Convert the postcode to longitude and latitude coordinates and
return the results in a table or csv.
'''
from geopy.geocoders import Nominatim
#from geopy.extra.rate_limiter import RateLimiter
import logging
import requests

# To hide WARNING:urllib3.connectionpool:Retrying
logging.getLogger(requests.packages.urllib3.__package__).setLevel(logging.ERROR)

def get_longitude_latitude(postcode):
    geolocator = Nominatim(user_agent="data-challenge2")
    #geocode = RateLimiter(geolocator.geocode, min_delay_seconds = 1)
    location = geolocator.geocode({"postalcode": str(postcode), "country": 'deutschland'}, country_codes = 'de')
    if location:
        return location.latitude, location.longitude
    else:
        return None, None

def processing_df_to_obtain_lat_long(df, BATCH_SIZE):
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
    
    print(df.head())

    return df