import unittest
import os
import json
from src.loaders.database_loader import DatabaseLoader
from src.loaders.file_loader import FileLoader
from src.loaders.cache_loader import CacheLoader

class TestDatabaseLoader(unittest.TestCase):
    def setUp(self):
        self.loader = DatabaseLoader()

    def test_connect(self):
        connection = self.loader.connect()
        self.assertIsNotNone(connection)
        connection.close()

    def test_save(self):
        data = {
            'id': 1,
            'name': 'Test Business',
            'address': 'Test Address'
        }
        result = self.loader.save(data)
        self.assertTrue(result)

    def test_load_data(self):
        data = self.loader.load_data(id=1)
        self.assertIsInstance(data, dict)

class TestFileLoader(unittest.TestCase):
    def setUp(self):
        self.loader = FileLoader()
        self.test_file = 'test_data.json'

    def tearDown(self):
        if os.path.exists(self.test_file):
            os.remove(self.test_file)

    def test_save_json(self):
        data = {
            'id': 1,
            'name': 'Test Business',
            'address': 'Test Address'
        }
        result = self.loader.save(data, self.test_file)
        self.assertTrue(result)
        self.assertTrue(os.path.exists(self.test_file))
        with open(self.test_file, 'r') as f:
            saved_data = json.load(f)
        self.assertEqual(saved_data, data)

    def test_save_csv(self):
        test_data = [{'col1': 'val1', 'col2': 'val2'}]
        result = self.loader.save_csv(test_data, 'test.csv')
        self.assertTrue(result)
        self.assertTrue(os.path.exists(os.path.join(self.temp_dir, 'test.csv')))

class TestCacheLoader(unittest.TestCase):
    def setUp(self):
        self.loader = CacheLoader()

    def test_save_and_get(self):
        data = {
            'id': 1,
            'name': 'Test Business',
            'address': 'Test Address'
        }
        self.loader.save(data, 'test_key')
        retrieved = self.loader.get('test_key')
        self.assertEqual(retrieved, data)

    def test_delete(self):
        self.loader.save(data, 'test_key')
        self.loader.delete('test_key')
        value = self.loader.get('test_key')
        self.assertIsNone(value)

    def test_clear(self):
        self.loader.save(data, 'key1')
        self.loader.save(data, 'key2')
        self.loader.clear()
        self.assertIsNone(self.loader.get('key1'))
        self.assertIsNone(self.loader.get('key2'))

if __name__ == '__main__':
    unittest.main() 