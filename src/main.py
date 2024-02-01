import os
import pandas as pd
from data_cleaner import cleaning_df_city_data
from postcode_to_longlat import processing_df_to_obtain_lat_long
from city_scraper import scraper_basic_info
from missing_coordinates import missing_coordinates


project_dir = os.path.dirname(__file__)

df_city_latlong_output = os.path.join(os.path.dirname(project_dir),
                                    'data/output/df_city_latlong_output.csv')

def main():
    URL_MAIN = 'https://de.wikipedia.org/wiki/Liste_der_St%C3%A4dte_in_Deutschland'
    URL_BASE = 'https://de.wikipedia.org'

    # The fragment size for splitting the list
    FRAGMENT_SIZE = 25

    dict_url_city_name, df_city = scraper_basic_info(URL_MAIN, URL_BASE, FRAGMENT_SIZE)

    df_city = cleaning_df_city_data(df_city, dict_url_city_name)

    """
    The number of records obtained is 2056, the same as the number
    of cities at the beginning of the page.
    """
    
    # The batch size for obtain latitude and longitude
    BATCH_SIZE = 25

    df_city = processing_df_to_obtain_lat_long(df_city, BATCH_SIZE)

    df_city_copy = df_city.copy()
    
    ## Checking for missing values

    df_multiple_row = missing_coordinates(df = df_city, BATCH_SIZE = BATCH_SIZE)
    df_multiple_row_copy = df_multiple_row.copy()
    
    df_single_row = missing_coordinates(df = df_multiple_row_copy, BATCH_SIZE = BATCH_SIZE)
    df_city = df_city.dropna()
    df_multiple_row = df_multiple_row.dropna()
    df_single_row = df_single_row.dropna()

    df_final = pd.concat([df_single_row,df_multiple_row,df_city])
       
    df_final.to_csv(df_city_latlong_output, index=False)
    


# df_city_details = pd.read_csv('city_details.csv')
# df_city_details.head()

# df_city_details['name'].value_counts()

# (df_city_details['name'].value_counts() > 1).head(10).sum()

# """There are 7 cities with the same name"""

# df_city_details['coordinate'] = df_city_details.apply(
#                                                       lambda row:
#                                                       (row["latitude"] , row["longitude"]),
#                                                       axis=1,
#                                                   )
# df_city_details.head()

# city_coords = df_city_details[['name','coordinate']].set_index("name").T.to_dict('records')[0]

# # Commented for improve readability

# #city_coords
# # {'Aßlar': (50.501544, 26.0705386),
# #  'Aach': (47.84204984036061, 8.854742132732317),
# #  'Aachen': (50.77643283176553, 6.086669673288873),
# #  'Aalen': (48.83874607033844, 10.085620109546616),
# #  'Abenberg': (49.23760620916201, 10.951095987821228),
# #  'Abensberg': (58.22442178189655, 22.118634066810344),
# #  ...}

# # Example:
# ciudades_en_radio = closer_cities(limit_distance = 20, origin_city = 'Berlin')
# ciudades_en_radio

