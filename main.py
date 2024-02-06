import pandas as pd
import os
from scrapy.crawler import CrawlerProcess

from src.cities_within import closer_cities
from src.city_name_latlong_search import processing_city_name_to_obtain_lat_long
from src.data_cleaner import cleaning_df_city_data
from src.german_cities import GermanCitiesSpider
from src.postcode_latlong_search import processing_postcode_to_obtain_lat_long


project_dir = os.path.dirname(__file__)
postcode_latlong = os.path.join(os.path.dirname(project_dir),
                                    '/postcode_latlong.csv')

df_city_latlong = os.path.join(os.path.dirname(project_dir),
                                    '/df_city_latlong.csv')


def main():
    # Read csv file
    df_city = pd.read_csv('city_postal_data_raw.csv')
    
    # Data Cleaning
    postcode_to_search, df_city = cleaning_df_city_data(df_city)
    BATCH_SIZE = 25 
    
    # Using geopy and Nominatim to get latitude and longitude
    postcode_to_search = processing_postcode_to_obtain_lat_long(postcode_to_search, BATCH_SIZE)
    
    # Saving latitude and longitude data
    postcode_to_search = postcode_to_search.dropna()
    postcode_to_search.to_csv('postcode_latlong.csv', index = False)
    
    # Using geopy and Nominatim to get latitude and longitude
    df_city = processing_city_name_to_obtain_lat_long(df_city, BATCH_SIZE)
    df_city = df_city.dropna()
    
    # Saving latitude and longitude data
    df_city.to_csv('df_city_latlong.csv', index = False)
    
    # # Example:
    nearby_cities = closer_cities(limit_distance = 50, origin_city = 'Frankfurt (Oder)')
    
    print(nearby_cities)

    
if __name__ == "__main__":
    try: 
        df = pd.read_csv('city_postal_data_raw.csv')
        if len(df) != 2056:
    
            process = CrawlerProcess({
                'USER_AGENT': 'Mozilla/5.0',
                'FEEDS': {
                    'city_postal_data_raw.csv': {
                        'format': 'csv',
                        'overwrite': True,
                        'fields': ['city_name', 'postcode']
                    },
                }
            })

            process.crawl(GermanCitiesSpider)
            process.start()
            main()
        else:
            main()
            
    except Exception as e:
        print(str(e))    
        process = CrawlerProcess({
            'USER_AGENT': 'Mozilla/5.0',
            'FEEDS': {
                'city_postal_data_raw.csv': {
                    'format': 'csv',
                    'overwrite': True,
                    'fields': ['city_name', 'postcode']
                },
            }
        })

        process.crawl(GermanCitiesSpider)
        process.start()
        main()