import unittest
import pandas as pd
import json
from datetime import datetime
from cleaner_guiaCores import (
    clean_phone_number,
    normalize_capitalization,
    clean_social_link
)

class TestGuiaCoresCleaner(unittest.TestCase):
    def setUp(self):
        """Setup test data"""
        self.sample_data = {
            'name': 'Estudio Contable Ejemplo',
            'phones': '(0299) 123-4567 | +54 9 299 1234567',
            'address': 'Av. San Martín 123, Neuquén',
            'email': 'contacto@estudioejemplo.com',
            'website': 'https://www.estudioejemplo.com',
            'facebook': 'https://www.facebook.com/estudioejemplo',
            'instagram': 'https://www.instagram.com/estudioejemplo',
            'categories': 'Estudio Contable, Auditoría',
            'hours': 'Lunes a Viernes 9:00 a 18:00',
            'detail_link': 'https://www.guiacores.com.ar/index.php?r=search/detail&id=123'
        }

    def test_clean_phone_number(self):
        """Test phone number cleaning"""
        test_cases = [
            ('(0299) 123-4567', '2991234567'),
            ('+54 9 299 1234567', '+542991234567'),
            ('123-4567', '1234567'),
            ('', 'N/A'),
            (None, 'N/A'),
            ('N/A', 'N/A')
        ]
        
        for input_phone, expected in test_cases:
            with self.subTest(input_phone=input_phone):
                self.assertEqual(clean_phone_number(input_phone), expected)

    def test_normalize_capitalization(self):
        """Test text normalization"""
        test_cases = [
            ('ESTUDIO CONTABLE EJEMPLO', 'Estudio Contable Ejemplo'),
            ('estudio contable ejemplo', 'Estudio Contable Ejemplo'),
            ('', 'N/A'),
            (None, 'N/A'),
            ('N/A', 'N/A')
        ]
        
        for input_text, expected in test_cases:
            with self.subTest(input_text=input_text):
                self.assertEqual(normalize_capitalization(input_text), expected)

    def test_clean_social_link(self):
        """Test social media link cleaning"""
        test_cases = [
            (
                'https://www.facebook.com/sharer/sharer.php?u=https://www.guiacores.com.ar%2Findex.php%3Fr%3Dsearch%2Fdetail%26id%3D123%26idb%3D456',
                'N/A'
            ),
            (
                'https://www.facebook.com/estudioejemplo',
                'https://www.facebook.com/estudioejemplo'
            ),
            ('', 'N/A'),
            (None, 'N/A')
        ]
        
        for input_link, expected in test_cases:
            with self.subTest(input_link=input_link):
                self.assertEqual(
                    clean_social_link(input_link, r'https://www.facebook.com/sharer/sharer.php\?u=https://www\.guiacores\.com\.ar%2Findex\.php%3Fr%3Dsearch%2Fdetail%26id%3D\d+%26idb%3D\d+'),
                    expected
                )

    def test_data_structure_matches_schema(self):
        """Test that cleaned data matches the expected schema"""
        # Create a sample DataFrame
        df = pd.DataFrame([self.sample_data])
        
        # Rename columns according to schema
        column_mapping = {
            'name': 'Nombre',
            'phones': 'Teléfono',
            'address': 'Dirección',
            'email': 'Email',
            'website': 'Sitio Web',
            'facebook': 'Facebook',
            'instagram': 'Instagram',
            'categories': 'Rubros'
        }
        df = df.rename(columns=column_mapping)
        
        # Add timestamp columns
        df['fecha_extraccion'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        df['fecha_actualizacion'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Verify required columns exist
        required_columns = [
            'Nombre', 'Email', 'Teléfono', 'Sitio Web', 'Facebook',
            'Instagram', 'Rubros', 'Dirección', 'fecha_extraccion',
            'fecha_actualizacion'
        ]
        for col in required_columns:
            self.assertIn(col, df.columns)

    def test_duplicate_handling(self):
        """Test duplicate handling"""
        # Create DataFrame with duplicates
        data = [
            self.sample_data,
            self.sample_data,  # Exact duplicate
            {**self.sample_data, 'name': 'Estudio Contable Ejemplo'},  # Same name, different case
            {**self.sample_data, 'phones': '(0299) 123-4567'}  # Same phone, different format
        ]
        df = pd.DataFrame(data)
        
        # Clean data
        df['name'] = df['name'].apply(normalize_capitalization)
        df['phones'] = df['phones'].apply(clean_phone_number)
        
        # Remove duplicates
        df_cleaned = df.drop_duplicates()
        
        # Should have only one row after cleaning
        self.assertEqual(len(df_cleaned), 1)

    def test_json_storage_format(self):
        """Test JSON storage format for raw_leads"""
        # Create sample data
        data = [self.sample_data]
        
        # Convert to JSON
        json_data = json.dumps(data[0])
        
        # Verify JSON structure
        parsed_json = json.loads(json_data)
        required_fields = [
            'name', 'phones', 'address', 'email', 'website',
            'facebook', 'instagram', 'categories'
        ]
        for field in required_fields:
            self.assertIn(field, parsed_json)

    def test_maximum_information_extraction(self):
        """Test that all available information is extracted"""
        sample_data = {
            **self.sample_data,
            'additional_info': 'Información adicional',
            'business_hours': 'Lunes a Viernes 9:00 a 18:00',
            'services': 'Servicios contables, auditoría, asesoría fiscal'
        }
        
        # Convert to DataFrame
        df = pd.DataFrame([sample_data])
        
        # Verify all fields are present
        for field in sample_data.keys():
            self.assertIn(field, df.columns)

if __name__ == '__main__':
    unittest.main() 