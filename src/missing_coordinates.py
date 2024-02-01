import pandas as pd
from postcode_to_longlat import processing_df_to_obtain_lat_long

def missing_coordinates(df: pd.DataFrame, BATCH_SIZE: int):
    """
    Process the dataframe to obtain missing latitude and longitude coordinates.

    Args:
    - df (pd.DataFrame): The input dataframe containing geographical data.

    Returns:
    - pd.DataFrame: The dataframe with missing coordinates obtained.
    """

    df_missing_coordenates = df[(df['latitude'].isnull()) | (df['longitude'].isnull())]
    df_missing_coordenates_copy = df_missing_coordenates.copy()

    if len(df_missing_coordenates_copy) > 1:
        df_missing_coordenates_copy["first_postcode"] = df_missing_coordenates_copy["postcode"].str.slice(-5)
    else:
        df_missing_coordenates_copy["first_postcode"] = 37073   # city Göttingen was searched in the documentation

    df_missing_coordenates_copy = df_missing_coordenates_copy.drop(['latitude','longitude'],axis=1)
    df_missing_coordenates_copy = processing_df_to_obtain_lat_long(df_missing_coordenates_copy, BATCH_SIZE)
    return df_missing_coordenates_copy