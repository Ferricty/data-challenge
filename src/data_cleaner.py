import os
import pandas as pd

project_dir = os.path.dirname(__file__)

df_city_postcodes = os.path.join(os.path.dirname(project_dir),
                                    'data/output/df_city_postcodes.csv')

def cleaning_df_city_data(df_city: pd.DataFrame, dict_url_city_name: dict) -> pd.DataFrame:
    
    """
    Performs data cleaning on the provided dataframe.

    Args:
    - df_city (pd.DataFrame): The input dataframe containing city data.
    - dict_url_city_name (dict): A dictionary mapping URLs to city names.

    Returns:
    - pd.DataFrame: The cleaned dataframe containing city data.

    Example:
    cleaned_df = cleaning_df_city_data(df, url_name_dict)
    
    Performing some data cleaning on the obtained dataframe. This includes: 
    - Mapping city names to the index of the dataframe based on the provided dictionary.
    - Resetting the index of the dataframe.
    - Renaming the index column to 'url'.
    - Removing line breaks from the 'postcode' column.
    - Creating a new column 'first_postcode' which contains the first part of the postcode.
    - Checking for the existence of duplicate values based on the 'first_postcode' column.

    Prints the number of cities with duplicated postcodes and the shape of the dataframe before 
    returning the cleaned dataframe.
    """        
    # Performing some data cleaning on the obtained dataframe.
    df_city['name'] = df_city.index.map(dict_url_city_name.get)
    
    df_city.reset_index(level=0, inplace=True)
    
    df_city = df_city.rename(columns={'index':'url'})
    
    df_city['postcode'] = df_city['postcode'].str.replace('\\n','',regex = True)

    df_city['first_postcode'] = df_city['postcode'].str.slice(0, 5)

    # Checking for the existence of duplicate values.
    print(f"There are {len(df_city[df_city['first_postcode'].duplicated()].sort_values('first_postcode'))} cities with duplicated postcode")
    
    print(f"df_city shape: {df_city.shape}")
    
    # Saving cities names and postcodes to search
    # df_city.to_csv(df_city_postcodes, index=False)
    
    return df_city