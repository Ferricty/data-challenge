def cleaning_df_city_data(df_city, dict_url_city_name):
        
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
    df_city.to_csv('data/output/df_city_postcodes.csv', index=False)
    
    return df_city