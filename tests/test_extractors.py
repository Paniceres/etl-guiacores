import unittest
from src.extractors.bulk.collector import BulkCollector
from src.extractors.bulk.scraper import BulkScraper
from src.extractors.sequential.collector import SequentialCollector
from src.extractors.sequential.scraper import SequentialScraper
from src.extractors.manual.scraper import ManualScraper

class TestBulkExtractor(unittest.TestCase):
    def setUp(self):
        self.collector = BulkCollector()
        self.scraper = BulkScraper()

    def test_collect_urls(self):
        urls = self.collector.collect_urls(start_id=1, end_id=10)
        self.assertIsInstance(urls, list)
        self.assertTrue(all(isinstance(url, str) for url in urls))

    def test_scrape_urls(self):
        urls = ['http://example.com/1', 'http://example.com/2']
        data = self.scraper.scrape_urls(urls)
        self.assertIsInstance(data, list)
        self.assertTrue(all(isinstance(item, dict) for item in data))

class TestSequentialExtractor(unittest.TestCase):
    def setUp(self):
        self.collector = SequentialCollector()
        self.scraper = SequentialScraper()

    def test_collect_urls(self):
        urls = self.collector.collect_urls(rubros=['Farmacias'], localidades=['Neuquén'])
        self.assertIsInstance(urls, dict)
        self.assertTrue(all(isinstance(url, str) for url in urls.values()))

    def test_scrape_urls(self):
        urls = {'1': 'http://example.com/1', '2': 'http://example.com/2'}
        data = self.scraper.scrape_urls(urls)
        self.assertIsInstance(data, list)
        self.assertTrue(all(isinstance(item, dict) for item in data))

class TestManualExtractor(unittest.TestCase):
    def setUp(self):
        self.scraper = ManualScraper()

    def test_parse_search_results(self):
        html_content = """
        <div class="card-mobile gc-item">
            <span class="nombre-comercio">
                <a href="?r=search/detail&id=1">Test Business</a>
            </span>
        </div>
        """
        results = self.scraper.parse_search_results_page(html_content)
        self.assertIsInstance(results, list)
        self.assertTrue(len(results) > 0)

    def test_parse_detail_page(self):
        html_content = """
        <a class="search-result-name">
            <h1>Test Business</h1>
        </a>
        <span class="search-result-address">Test Address</span>
        """
        result = self.scraper.parse_detail_page(html_content)
        self.assertIsInstance(result, dict)
        self.assertIn('Nombre', result)
        self.assertIn('Dirección', result)

if __name__ == '__main__':
    unittest.main() 