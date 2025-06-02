from scrapy.crawler import CrawlerProcess
from scrapy.spiders import Spider
from scrapy.http import Response

output_data = {}

class ArxivSpider(Spider):
    name = "arxiv_categories"
    start_urls = ['https://arxiv.org/category_taxonomy']

    def parse(self, response: Response):
        for h4 in response.xpath('//h4'):
            code = h4.xpath('text()').get()
            name = h4.xpath('span/text()').get()
            if code and name:
                output_data[code.strip()] = name.strip("()").strip()

def run_scraper():
    pass
    global output_data
    output_data = {}
    process = CrawlerProcess(settings={"LOG_ENABLED": False})
    process.crawl(ArxivSpider)
    process.start()
    return output_data


if __name__ == '__main__':
    print("For testing module")
    data = run_scraper()
    # print