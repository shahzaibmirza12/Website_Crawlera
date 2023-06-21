import csv
import json

import scrapy
from datetime import datetime
from urllib.parse import urlparse, urljoin


class WebsiteContentCrawlerSpider(scrapy.Spider):
    name = "website_content_crawler"
    custom_settings = {
        'FEED_URI': 'Website_Crawler_Records2.json',
        'FEED_FORMAT': 'json',
    }
    headers = {}
    already_requested = []

    def start_requests(self):
        input_parameters = self.get_input_parameters()
        for input_data in input_parameters:
            try:
                netloc = self.set_path(input_data['start_URL'])
                if netloc not in self.already_requested:
                    self.already_requested.append(netloc)
                    yield scrapy.Request(url=input_data['start_URL'], headers=self.headers, callback=self.parse,
                                         meta={'input_data': input_data, 'referer': input_data['start_URL'],
                                               'netloc': netloc, 'isFirst': True},
                                         )
            except Exception as e:
                self.logger.info('An Error Occured with an Exception:', exc_info=e)

    def parse(self, response, **kwargs):
        self.logger.info(f'Getting Data for: {response.url}')
        if 'depth' not in response.request.meta:
            response.request.meta['depth'] = 0
        yield from self.get_data(response)
        if response.meta['isFirst']:
            next_links = set(response.xpath('//a[contains(@href, "")]/@href').getall())
            for next_link in next_links:
                next_link = self.check_url(next_link, response.url, response.meta['netloc'])
                if not next_link:
                    continue
                if next_link in self.already_requested:
                    continue
                yield response.follow(url=next_link, headers=self.headers, callback=self.parse,
                                      meta={'input_data': response.meta['input_data'],
                                            'referer': response.url,
                                            'isFirst': False,
                                            'netloc': response.meta['netloc']})

    def get_data(self, response):
        more_urls = set(response.xpath('//a[contains(@href, "")]/@href').getall())
        if any(more_urls):
            for next_link in more_urls:
                next_link = self.check_url(next_link, response.url, response.meta['netloc'])
                if not next_link:
                    continue
                if next_link in self.already_requested:
                    continue
                yield response.follow(url=next_link, headers=self.headers, callback=self.parse,
                                      meta={'input_data': response.meta['input_data'],
                                            'referer': response.url,
                                            'isFirst': False,
                                            'netloc': response.meta['netloc']})
        record = {
            "url": response.url,
            "crawl": {
                "loadedUrl": response.url,
                "loadedTime": datetime.now(),
                "referrerUrl": response.request.headers.get('Referer', '').decode('utf-8') or response.meta.get(
                    'referer', ''),
                "depth": response.meta.get('depth', 0),
                "httpStatusCode": response.status
            },
            "metadata": {
                "canonicalUrl": response.css('[property="og:url"]::attr(content)').get() or response.url,
                "title": response.css('meta[property="og:title"]::attr(content)').get('') or \
                         response.css('meta[name="title"]::attr(content)').get(''),
                "description": response.css('meta[property="og:description"]::attr(content)').get('') or \
                               response.css('meta[name="description"]::attr(content)').get(''),
                "languageCode": response.css('meta[property="og:locale"]::attr(content)').get('')
            },
            "text": self.get_text(response.css('p *::text').getall())
        }
        yield record

    def check_more(self, response):
        print('check_more')
        more_urls = set(response.xpath('//a[contains(@href, "")]/@href').getall())
        if any(more_urls):
            for next_link in more_urls:
                next_link = self.check_url(next_link, response.url, response.meta['netloc'])
                if not next_link:
                    continue
                if next_link in self.already_requested:
                    continue
                yield response.follow(url=next_link, headers=self.headers, callback=self.parse,
                                      meta={'input_data': response.meta['input_data'],
                                            'referer': response.url,
                                            'isFirst': False,
                                            'netloc': response.meta['netloc']})

    def check_url(self, url, response_url, netloc):
        parsed_url = urlparse(url)
        if parsed_url.scheme == '':
            url = urljoin(response_url, url)
        parsed_url_ = urlparse(url)
        if parsed_url_.netloc == netloc:
            return url
        else:
            return None

    def get_text(self, data):
        return '\n'.join(info.strip() for info in data)

    def set_path(self, input_url):
        parsed_url = urlparse(input_url)
        return parsed_url.netloc

    def get_input_parameters(self):
        with open('input_parameters.csv', 'r') as file:
            return list(csv.DictReader(file))
