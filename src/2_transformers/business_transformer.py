from typing import List, Dict, Any
from ..common.base import BaseTransformer
from ..common.config import get_config

class BusinessTransformer(BaseTransformer):
    """Transformador para procesar datos de negocios"""
    
    def __init__(self):
        super().__init__('business_transformer')
        self.config = get_config()['transformer']['business']
        
    def transform(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transforma los datos de negocios
        
        Args:
            data: Lista de diccionarios con datos de negocios
            
        Returns:
            Lista de diccionarios con datos transformados
        """
        transformed_data = []
        
        for item in data:
            # Validar campos requeridos
            if not self._validate_required_fields(item):
                continue
                
            # Transformar datos
            transformed_item = {
                'name': self._clean_text(item['name']),
                'url': item['url']
            }
            
            # Agregar campos opcionales si existen
            for field in self.config['optional_fields']:
                if field in item:
                    transformed_item[field] = self._clean_text(item[field])
                    
            transformed_data.append(transformed_item)
            
        return transformed_data
        
    def _validate_required_fields(self, item: Dict[str, Any]) -> bool:
        """
        Valida que los campos requeridos estén presentes
        
        Args:
            item: Diccionario con datos de negocio
            
        Returns:
            True si todos los campos requeridos están presentes
        """
        return all(field in item for field in self.config['required_fields'])
        
    def _clean_text(self, text: str) -> str:
        """
        Limpia un texto
        
        Args:
            text: Texto a limpiar
            
        Returns:
            Texto limpio
        """
        if not text:
            return ''
            
        # Eliminar espacios extra
        text = ' '.join(text.split())
        
        # Eliminar caracteres especiales
        text = text.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
        
        return text.strip() 