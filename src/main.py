import os
import pandas as pd

from data_cleaner import cleaning_df_city_data
from postcode_to_longlat import processing_df_to_obtain_lat_long
from city_scraper import scraper_basic_info
from missing_coordinates import missing_coordinates
from cities_within import closer_cities

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
    
    df_final.rename(columns={'first_postcode': 'searched_postcode'}, inplace=True)  
     
    df_final.to_csv(df_city_latlong_output, index=False)
    
    """
    Function, that takes a postcode or city name and radius (in km) 
    as input and returns all postcodes within the radius.
    """
    # Example:
    nearby_cities = closer_cities(limit_distance = 20, origin_city = 'Berlin')
    
    print(nearby_cities)

if __name__ == "__main__":
    main()