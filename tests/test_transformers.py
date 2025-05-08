import unittest
from src.transformers.business_transformer import BusinessTransformer
from src.transformers.data_cleaner import DataCleaner
from src.transformers.url_transformer import URLTransformer

class TestBusinessTransformer(unittest.TestCase):
    def setUp(self):
        self.transformer = BusinessTransformer()

    def test_transform(self):
        data = {
            'nombre': 'Test Business',
            'direccion': 'Test Address',
            'telefono': '123-456-7890'
        }
        transformed = self.transformer.transform(data)
        self.assertIsInstance(transformed, dict)
        self.assertIn('Nombre', transformed)
        self.assertIn('Dirección', transformed)
        self.assertIn('Teléfono', transformed)

class TestDataCleaner(unittest.TestCase):
    def setUp(self):
        self.cleaner = DataCleaner()

    def test_clean(self):
        data = {
            'Nombre': '  Test Business  ',
            'Dirección': 'Test Address\n',
            'Teléfono': '(123) 456-7890'
        }
        cleaned = self.cleaner.clean(data)
        self.assertIsInstance(cleaned, dict)
        self.assertEqual(cleaned['Nombre'], 'Test Business')
        self.assertEqual(cleaned['Dirección'], 'Test Address')
        self.assertEqual(cleaned['Teléfono'], '1234567890')

class TestURLTransformer(unittest.TestCase):
    def setUp(self):
        self.transformer = URLTransformer()

    def test_transform(self):
        data = {
            'url': 'https://www.guiacores.com.ar/detalle/123'
        }
        transformed = self.transformer.transform(data)
        self.assertIsInstance(transformed, dict)
        self.assertIn('URL', transformed)
        self.assertEqual(transformed['URL'], 'https://www.guiacores.com.ar/detalle/123')

if __name__ == '__main__':
    unittest.main() 