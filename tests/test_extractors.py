import unittest
from src.extractors.bulk_collector import BulkCollector
from src.extractors.sequential_scraper import SequentialScraper
from src.extractors.manual_scraper import ManualScraper

class TestBulkCollector(unittest.TestCase):
    def setUp(self):
        self.collector = BulkCollector()

    def test_collect_urls(self):
        urls = self.collector.collect_urls()
        self.assertIsInstance(urls, list)
        self.assertTrue(all(isinstance(url, str) for url in urls))

class TestSequentialScraper(unittest.TestCase):
    def setUp(self):
        self.scraper = SequentialScraper()

    def test_scrape(self):
        url = 'https://www.guiacores.com.ar/detalle/123'
        data = self.scraper.scrape(url)
        self.assertIsInstance(data, dict)
        self.assertIn('Nombre', data)
        self.assertIn('Dirección', data)

class TestManualScraper(unittest.TestCase):
    def setUp(self):
        self.scraper = ManualScraper()

    def test_scrape(self):
        url = 'https://www.guiacores.com.ar/detalle/123'
        data = self.scraper.scrape(url)
        self.assertIsInstance(data, dict)
        self.assertIn('Nombre', data)
        self.assertIn('Dirección', data)

if __name__ == '__main__':
    unittest.main() 