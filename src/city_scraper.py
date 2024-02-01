import asyncio
import aiohttp
from bs4 import BeautifulSoup
import requests
import pandas as pd

def scraper_basic_info(URL_MAIN, URL_BASE, FRAGMENT_SIZE):
    
    response = requests.get(URL_MAIN)

    soup = BeautifulSoup(response.text, 'html.parser')

    initial_data = soup.find_all('dd') # All the names are dd tags
    
    city_name = [city.find("a").text for city in initial_data]
    
    print(f"Amount of cities: {len(city_name)}") # 2056 it's equals to the amount of cities at the URL_MAIN page

    # Get relative URL for each city
    city_url_rel = [city.find("a").get("href") for city in initial_data]
    print(f"Amount of urls: {len(city_url_rel)}") # 2056
    
    # Obtaining the absolute url for each of the cities.
    url_city_absolute = [URL_BASE + href for href in city_url_rel]

    # We will use the following dictionary to clean the dataframe that we will obtain in future steps.
    dict_url_city_name = dict(zip(url_city_absolute, city_name))
    
    # Generate smaller fragments for scraping
    fragments = [url_city_absolute[i:i + FRAGMENT_SIZE] for i in range(0, len(url_city_absolute), FRAGMENT_SIZE)]

    # We will store the dataframes in the following list.
    df_list = []

    for index, fragment in enumerate(fragments):
        # Perform scraping with each fragment
        print(f"Perform scraping with the fragment: {index} of {len(fragments) - 1}")
        scraper = WebScraper(urls = fragment)

        df_list.append(pd.DataFrame.from_dict(scraper.master_dict, orient='index'))

    df_city = pd.concat(df_list)
    
    return dict_url_city_name, df_city


class WebScraper(object):
    def __init__(self, urls):
        self.urls = urls
        # Global Place To Store The Data:

        self.master_dict = {}
        # Run The Scraper:
        asyncio.run(self.main())

    async def fetch(self, session, url):
        try:
            async with session.get(url) as response:
                # 1. Extracting the Text:
                city_href = await response.text()

                # 2. Extracting the postcode:
                postcode = await self.extract_postcode(city_href)
                return url, postcode

        except Exception as e:
            print(str(e))

    async def extract_postcode(self, city_href):
        try:

            soup_city = BeautifulSoup(city_href, 'html.parser')

            city_postcode = soup_city.find('a', attrs={'href':"/wiki/Postleitzahl_(Deutschland)"}).find_next().text
            return city_postcode

        except Exception as e:
            print(str(e))

    async def main(self):
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
                    self.master_dict[url] = {'postcode': html[1]}
                else:
                    continue
