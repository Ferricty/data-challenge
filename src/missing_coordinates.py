def missing_coordinates_multiple(df_missing_data):
    
    df_missing_coordenates = df_missing_data[
                                            (df_missing_data['latitude'].isnull()) | 
                                            (df_missing_data['longitude'].isnull())
                                            ]



    df_missing_coordenates_copy = df_missing_coordenates.copy()
    df_missing_coordenates_copy["first_postcode"] = df_missing_coordenates_copy["postcode"].str.slice(-5)
    

    df_missing_coordenates_copy = df_missing_coordenates_copy.drop(['latitude','longitude'],axis=1)
    


    # df_missing_coordenates_cp['latitude'] = latitudes_missing
    # df_missing_coordenates_cp['longitude'] = longitudes_missing


    # df_city2 = df_city.dropna()

def join_dataframe():
    pass
    # """Joining the two dataframes into one with the required data"""

    # df_final = pd.concat([df_city2,df_missing_coordenates_cp])

    # df_final = df_final.drop(['url'],axis=1)

    # df_final.rename(columns={'first_postcode': 'searched_postcode'}, inplace=True)

    # df_final.to_csv('data/output/city_details.csv', index=False)