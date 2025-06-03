import time
import logging
import sys
if sys.platform == "win32":
    import asyncio
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from scrapy import Spider, signals
import xml.etree.ElementTree as ET
from scrapy.http import Request
from scrapy.crawler import CrawlerProcess


class ArxivPapersSpider(Spider):
    name = "arxiv_papers"
    def __init__(self, categories=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.categories = categories or []
        self.batch_size = 1000
        self.max_total = 5000

    def start_requests(self):
        for category_id in self.categories:
            for start in range(0, self.max_total, self.batch_size):
                url = (
                    f"http://export.arxiv.org/api/query?"
                    f"search_query=cat:{category_id}&start={start}&max_results={self.batch_size}"
                )
                yield Request(url=url, callback=self.parse_entries, meta={"category_id": category_id})

    def parse_entries(self, response):
        root = ET.fromstring(response.text)
        entries = root.findall("{http://www.w3.org/2005/Atom}entry")
        for entry in entries:
            category_id = response.meta["category_id"]
            pdf_url = None
            for link in entry.findall("{http://www.w3.org/2005/Atom}link"):
                if link.attrib.get("title") == "pdf":
                    pdf_url = link.attrib["href"]
                    break

            yield {
                "category_id": category_id,
                "category_name": self.categories.get(category_id, "Unknown"),
                "title": entry.find("{http://www.w3.org/2005/Atom}title").text.strip(),
                "link": entry.find("{http://www.w3.org/2005/Atom}id").text.strip(),
                "pdf_url": pdf_url,
                "summary": entry.find("{http://www.w3.org/2005/Atom}summary").text.strip(),
                "published": entry.find("{http://www.w3.org/2005/Atom}published").text.strip(),
                "authors": ', '.join([
                    author.find("{http://www.w3.org/2005/Atom}name").text
                    for author in entry.findall("{http://www.w3.org/2005/Atom}author")
                ])
            }

def run_scraper(categories):
    logging.info(f"Type: {type(categories)}")
    logging.info(f"Len cats: {len(categories)}")
    logging.info(categories)

    results = []

    def collect_results(item):
        results.append(item)

    process = CrawlerProcess(settings={
        "LOG_ENABLED": False,
        "DOWNLOAD_TIMEOUT": 30,
        "RETRY_TIMES": 1,
    })

    crawler = process.create_crawler(ArxivPapersSpider)
    crawler.signals.connect(collect_results, signal=signals.item_scraped)
    process.crawl(crawler, categories=categories)

    start = time.time()
    process.start()
    logging.info(f"Finished in {round(time.time() - start, 2)}s")

    return results


if __name__ == "__main__":
    data = run_scraper(["cs.CL", "stat.ML"])
