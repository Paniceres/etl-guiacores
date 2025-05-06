import unittest
import os
import tempfile
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

    def test_save_data(self):
        test_data = {
            'id': 1,
            'name': 'Test Business',
            'address': 'Test Address'
        }
        result = self.loader.save_data(test_data)
        self.assertTrue(result)

    def test_load_data(self):
        data = self.loader.load_data(id=1)
        self.assertIsInstance(data, dict)

class TestFileLoader(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.loader = FileLoader(base_path=self.temp_dir)

    def tearDown(self):
        import shutil
        shutil.rmtree(self.temp_dir)

    def test_save_json(self):
        test_data = {'test': 'data'}
        result = self.loader.save_json(test_data, 'test.json')
        self.assertTrue(result)
        self.assertTrue(os.path.exists(os.path.join(self.temp_dir, 'test.json')))

    def test_load_json(self):
        test_data = {'test': 'data'}
        self.loader.save_json(test_data, 'test.json')
        loaded_data = self.loader.load_json('test.json')
        self.assertEqual(loaded_data, test_data)

    def test_save_csv(self):
        test_data = [{'col1': 'val1', 'col2': 'val2'}]
        result = self.loader.save_csv(test_data, 'test.csv')
        self.assertTrue(result)
        self.assertTrue(os.path.exists(os.path.join(self.temp_dir, 'test.csv')))

class TestCacheLoader(unittest.TestCase):
    def setUp(self):
        self.cache = CacheLoader()

    def test_set_get(self):
        self.cache.set('test_key', 'test_value')
        value = self.cache.get('test_key')
        self.assertEqual(value, 'test_value')

    def test_delete(self):
        self.cache.set('test_key', 'test_value')
        self.cache.delete('test_key')
        value = self.cache.get('test_key')
        self.assertIsNone(value)

    def test_clear(self):
        self.cache.set('key1', 'value1')
        self.cache.set('key2', 'value2')
        self.cache.clear()
        self.assertIsNone(self.cache.get('key1'))
        self.assertIsNone(self.cache.get('key2'))

if __name__ == '__main__':
    unittest.main() 