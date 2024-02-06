import scrapy


class GermanCitiesSpider(scrapy.Spider):
    name = "german-cities"

    start_urls = ['https://de.wikipedia.org/wiki/Liste_der_St%C3%A4dte_in_Deutschland']


    def parse(self, response):

        city_links = response.css('dd a::attr(href)').extract()
        city_names = response.css('dd a::text').extract()
        
        for city in response.css('dd'):
            yield {
                    "city_name": city.css("a::text").get(),
                    "city_href": city.css("a::attr(href)").get(),
                }
        
        # for city_link in city_links:
        #     yield response.follow(city_link, self.parse_city)
        for city_link, city_name in zip(city_links, city_names):
            yield response.follow(city_link, self.parse_city, meta={'city_name': city_name})

    
    def parse_city(self,response):
        def extract_with_xpath(query):
            return response.xpath(query).get()[:-1]
        yield {
            "city_name": response.meta['city_name'],
            # "city_name_h1": response.css('h1 span::text').get(),
            # "city_href": response.request.url,
            "postcode": extract_with_xpath('//a[contains(@href, "/wiki/Postleitzahl_(Deutschland)")]/parent::*/following-sibling::*/text()'),
        }