"""
Data Cleaner File
"""
import pandas as pd

def cleaning_df_city_data(df_city: pd.DataFrame) -> pd.DataFrame:
    """
    Performs data cleaning on the provided dataframe.

    Args:
    - df_city (pd.DataFrame): The input dataframe containing city data.
    

    Returns:
    - pd.DataFrame: The cleaned dataframe containing city data.

    Example:
    cleaned_df = cleaning_df_city_data(df)

    Performing some data cleaning on the obtained dataframe. This includes:
    - Resetting the index of the dataframe.
    - Renaming the index column to 'url'.
    - Removing line breaks from the 'postcode' column.
    - Creating a new column 'first_postcode' which contains the first part of the postcode.
    - Checking for the existence of duplicate values based on the 'first_postcode' column.

    Prints the number of cities with duplicated postcodes and the shape of the dataframe before returning the cleaned dataframe.
    """

    # Performing some data cleaning on the obtained dataframe.
    #df_city['name'] = df_city.index.map(dict_url_city_name.get)

    df_city.reset_index(level=0, inplace=True)

    df_city = df_city.rename(columns={'index':'url'})

    df_city['postcode'] = df_city['postcode'].str.replace('\\n','',regex = True)

    df_city['first_postcode'] = df_city['postcode'].str.slice(0, 5)

    # Checking for the existence of duplicate values.
    print(f"There are {len(df_city[df_city['first_postcode'].duplicated()].sort_values('first_postcode'))} cities with duplicated postcode")

    print(f"df_city shape: {df_city.shape}")

    return df_city