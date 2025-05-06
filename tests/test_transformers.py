import unittest
from src.transformers.business_transformer import BusinessTransformer
from src.transformers.data_cleaner import DataCleaner
from src.transformers.url_transformer import URLTransformer

class TestBusinessTransformer(unittest.TestCase):
    def setUp(self):
        self.transformer = BusinessTransformer()

    def test_transform_business_data(self):
        raw_data = {
            'Nombre': 'Test Business',
            'Dirección': 'Test Address',
            'Teléfonos': '123-456-7890',
            'Email': 'test@example.com'
        }
        transformed = self.transformer.transform(raw_data)
        self.assertIsInstance(transformed, dict)
        self.assertIn('name', transformed)
        self.assertIn('address', transformed)
        self.assertIn('phones', transformed)
        self.assertIn('email', transformed)

class TestDataCleaner(unittest.TestCase):
    def setUp(self):
        self.cleaner = DataCleaner()

    def test_clean_phone_number(self):
        phone = '(123) 456-7890'
        cleaned = self.cleaner.clean_phone_number(phone)
        self.assertEqual(cleaned, '1234567890')

    def test_clean_address(self):
        address = '  Test St. #123  '
        cleaned = self.cleaner.clean_address(address)
        self.assertEqual(cleaned, 'Test St. #123')

    def test_clean_email(self):
        email = ' Test@Example.com '
        cleaned = self.cleaner.clean_email(email)
        self.assertEqual(cleaned, 'test@example.com')

class TestURLTransformer(unittest.TestCase):
    def setUp(self):
        self.transformer = URLTransformer()

    def test_normalize_url(self):
        url = 'http://example.com/path?id=1'
        normalized = self.transformer.normalize_url(url)
        self.assertIsInstance(normalized, str)
        self.assertTrue(normalized.startswith('http'))

    def test_validate_url(self):
        valid_url = 'http://example.com'
        invalid_url = 'not-a-url'
        self.assertTrue(self.transformer.validate_url(valid_url))
        self.assertFalse(self.transformer.validate_url(invalid_url))

    def test_extract_id_from_url(self):
        url = 'http://example.com/detail?id=123'
        id = self.transformer.extract_id_from_url(url)
        self.assertEqual(id, '123')

if __name__ == '__main__':
    unittest.main() 