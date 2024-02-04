import sqlite3

def insert_city_postal_data(city_name, postcode):
    conn = sqlite3.connect('geolocation_cache.db')
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO city_postal (city_name, postcode) VALUES (?, ?)", (str(city_name), str(postcode)))
    conn.commit()
    conn.close()

def update_searched_postcode(row):
    conn = sqlite3.connect('geolocation_cache.db')
    cur = conn.cursor()
    cur.execute("UPDATE city_postal SET searched_postcode=? WHERE city_name=?", (str(row['searched_postcode']), str(row['city_name'])))
    conn.commit()
    conn.close()

def update_city_postal_data(searched_postcode, latitude, longitude):
    conn = sqlite3.connect('geolocation_cache.db')
    cur = conn.cursor()
    cur.execute("UPDATE city_postal SET latitude=?, longitude=? WHERE searched_postcode=?", (str(latitude), str(longitude), str(searched_postcode)))
    conn.commit()
    conn.close()

def get_lat_lng_from_cache(searched_postcode):
    conn = sqlite3.connect('geolocation_cache.db')
    cur = conn.cursor()
    cur.execute("SELECT latitude, longitude FROM city_postal WHERE searched_postcode=?", (str(searched_postcode),))
    row = cur.fetchone()
    conn.close()
    return row

def amount_of_city_names():
    conn = sqlite3.connect('geolocation_cache.db')
    cur = conn.cursor()
    cur.execute("SELECT COUNT(city_name) FROM city_postal WHERE city_name IS NOT NULL")
    amount_of_cities = cur.fetchone()[0]
    query = "SELECT * FROM city_postal"      
    conn.close()
    return amount_of_cities, query