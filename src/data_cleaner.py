"""
Data Cleaner File
"""
import pandas as pd
import re
# from cache import update_searched_postcode


def list_range(df):
    range_df = [code for code in range(int(df['start_range']), int(df['end_range'])+1)]
    return range_df
    
def cleaning_df_city_data(df_city: pd.DataFrame) -> pd.DataFrame:
    """
    Performs data cleaning on the provided dataframe.

    Args:
    - df_city (pd.DataFrame): The input dataframe containing city data.
    

    Returns:
    - pd.DataFrame: The cleaned dataframe containing city data.
    - pd.DataFrame: The cleaned dataframe containing postcodes.
    
    Example:
    search_postcode_df, df_city = cleaning_df_city_data(df)

    Performing some data cleaning on the obtained dataframe. This includes:
    - Resetting the index of the dataframe.
    - Renaming the index column to 'url'.
    - Removing line breaks from the 'postcode' column.
    - Creating a new column 'searched_postcode'.
    - Checking for the existence of duplicate values based on the 'searched_postcode' column.

    """

    # Performing some data cleaning on the obtained dataframe.

    df_city['postcode'] = df_city['postcode'].str.replace('\\n','',regex = True)

    # df_city['searched_postcode'] = df_city['postcode'].str.slice(0, 5)
    # df_city.apply(update_searched_postcode, axis=1)
   
    df_cp = df_city.copy()
    
    # Using regex to perform a fast preprocessing and get postcodes.
    
    # The numbers between – will be calculated like a range.  
    
    regex_pattern = r'(?<!–)\b(\d{5})\b(?!–)'

    postal_single_dash = df_cp['postcode'].str.extract(regex_pattern)
    postal_single_dash = postal_single_dash.dropna()
    postal_single_dash[0] = postal_single_dash[0].astype(int)
    postal_single_dash = postal_single_dash[0].to_list()

    regex_pattern = r'(\d{5})–(\d{5})'
    postal_range_dash = df_cp['postcode'].str.extract(regex_pattern)
    postal_range_dash = postal_range_dash.dropna()
    postal_range_dash.rename({postal_range_dash.columns[0]:"start_range",
                            postal_range_dash.columns[1]:"end_range"},axis=1,inplace=True)
    postal_range_dash_list = postal_range_dash.apply(list_range,axis=1)
    postal_range_dash_list = postal_range_dash_list.sum()
    all_list_postcode = postal_range_dash_list + postal_single_dash
    d = {'searched_postcode': all_list_postcode}
    search_postcode_df = pd.DataFrame(d)
    
    # Removing duplicate values.
    search_postcode_df = search_postcode_df.drop_duplicates()
    
    print(f"postcode_to_search shape: {search_postcode_df.shape}")
    print(f"city_to_search shape: {df_city.shape}")

    return search_postcode_df, df_city