"""_summary_

Returns:
    _type_: _description_
"""


import asyncio
import aiohttp
from bs4 import BeautifulSoup
import pandas as pd
import requests
import sqlite3
import time
from cache import amount_of_city_names, insert_city_postal_data

def scraper_basic_info(URL_MAIN: str, URL_BASE: str, FRAGMENT_SIZE: int):
    """
    Scrape information from a website.

    Args:
    - URL_MAIN (str): The main URL from which to scrape the basic information.
    - URL_BASE (str): The base URL to be used for forming absolute URLs.
    - FRAGMENT_SIZE (int): The size of each fragment for scraping.

    Returns:
    - pd.DataFrame: The concatenated dataframe containing the scraped data.

    Example:
    city_dataframe = scraper_basic_info('https://example.com/main', 'https://example.com/', 100)

    The function performs the following steps:
    1. Scrapes the basic information from the main URL and extracts city relative URLs.
    2. Obtains the absolute URL for each city.
    3. Generates smaller fragments of URLs for scraping based on the provided fragment size.
    4. Stores each scraped fragment into a list of dataframes.
    5. Concatenates the list of dataframes into a single dataframe.

    The function returns the concatenated dataframe.
    """

    response = requests.get(URL_MAIN)

    soup = BeautifulSoup(response.text, 'html.parser')

    initial_data = soup.find_all('dd') # All the names are dd tags

    # Get relative URL for each city
    city_url_rel = [city.find("a").get("href") for city in initial_data]
    
    total_of_cities_urls = len(city_url_rel)
    
    print(f"Amount of urls: {total_of_cities_urls}") # 2056
    
    conn = sqlite3.connect('geolocation_cache.db')
    
    amount_of_cities, query = amount_of_city_names()
    
    if total_of_cities_urls == amount_of_cities:
        
        df_city = pd.read_sql_query(query, conn,index_col='id', dtype = {'postcode': str})
        conn.close()
        
        return df_city

    conn.close()
    
    # Obtaining the absolute url for each of the cities.
    url_city_absolute = [URL_BASE + href for href in city_url_rel]

    # Generate smaller fragments for scraping
    fragments = [url_city_absolute[i:i + FRAGMENT_SIZE] for i in range(0, len(url_city_absolute), FRAGMENT_SIZE)]

    # We will store the dataframes in the following list.
    df_list = []

    for index, fragment in enumerate(fragments):
        # Perform scraping with each fragment
        progress = round((index/(len(fragments) - 1)) * 100, 2)
        print(f"Scraping progress: {progress} %")
        scraper = WebScraper(urls = fragment)

        df_list.append(pd.DataFrame.from_dict(scraper.master_dict, orient='index'))

    df_city = pd.concat(df_list)

    return df_city

class WebScraper(object):

    """
    A web scraper for obtaining postcodes from city URLs.

    Attributes:
    - urls (list): A list of city URLs to be scraped.
    - master_dict (dict): A dictionary to store the scraped data.

    Example:
    urls = ['http://example.com/city1', 'http://example.com/city2']
    scraper = WebScraper(urls)
    """

    def __init__(self, urls):

        """
        Initializes the WebScraper with a list of URLs and initiates the scraping process.

        Args:
        - urls (list): A list of city URLs to be scraped.
        """

        self.urls = urls

        # Global Place To Store The Data:
        self.master_dict = {}

        # Run The Scraper:
        asyncio.run(self.main())

    async def fetch(self, session, url):

        """
        Fetches the postcode for a given city URL.

        Args:
        - session (aiohttp.ClientSession): An async HTTP client session.
        - url (str): The URL of the city.

        Returns:
        - Tuple: A tuple containing the URL its corresponding postcode and city name.
        """
        retry_limit = 5
        retries = 0
        while retries < retry_limit:
            try:
                response = requests.get(url)
                response.raise_for_status()
                
                async with session.get(url) as response:
                    # 1. Extracting the Text:
                    city_href = await response.text()

                    # 2. Extracting the postcode:
                    postcode = await self.extract_postcode(city_href)

                    # 2. Extracting the city_name:
                    city_name = await self.extract_city_name(city_href)
                    return url, postcode, city_name

            except Exception as e:
                print(str(e))
                retries += 1
                if retries < retry_limit:
                    print(f"Waiting 5 seconds...")
                    time.sleep(5)

    async def extract_postcode(self, city_href):

        """
        Extracts the postcode from the city's HTML content.

        Args:
        - city_href (str): The HTML content of the city URL.

        Returns:
        - str: The extracted postcode.
        """

        try:

            soup_city = BeautifulSoup(city_href, 'html.parser')

            city_postcode = soup_city.find('a', attrs={'href':"/wiki/Postleitzahl_(Deutschland)"}).find_next().text
            return city_postcode

        except Exception as e:
            print(str(e))

    async def extract_city_name(self, city_href):

        """
        Extracts the name from the city's HTML content.

        Args:
        - city_href (str): The HTML content of the city URL.

        Returns:
        - str: The extracted name.
        """

        try:

            soup_city = BeautifulSoup(city_href, 'html.parser')

            city_name = soup_city.find('h1', attrs={'id':"firstHeading"}).text
            return city_name

        except Exception as e:
            print(str(e))

    async def main(self):
        """
        The main scraping method.

        - Initiates an async HTTP client session.
        - Loops through the provided URLs, initiates the fetch and extracts postcode asynchronously.
        - Stores the fetched postcodes in the master dictionary.
        """

        tasks = []
        headers = {
            "user-agent": "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"}
        async with aiohttp.ClientSession(headers=headers) as session:
            for url in self.urls:
                tasks.append(self.fetch(session, url))

            htmls = await asyncio.gather(*tasks)

            # Storing the data.
            for html in htmls:
                if html is not None:
                    url = html[0]
                    insert_city_postal_data(city_name = html[2], postcode = html[1])
                    self.master_dict[url] = {'postcode': html[1],'city_name': html[2]}
                else:
                    continue
