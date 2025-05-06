from typing import List, Dict, Any
from ..common.base import BaseTransformer
from ..common.config import get_config

class DataCleaner(BaseTransformer):
    """Transformador para limpiar y estructurar datos"""
    
    def __init__(self):
        super().__init__('data_cleaner')
        
    def transform(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Limpia y estructura los datos
        
        Args:
            data: Lista de diccionarios con datos
            
        Returns:
            Lista de diccionarios con datos limpios
        """
        cleaned_data = []
        
        for item in data:
            # Eliminar campos vacíos
            cleaned_item = {k: v for k, v in item.items() if v}
            
            # Convertir valores a tipos apropiados
            cleaned_item = self._convert_types(cleaned_item)
            
            # Eliminar duplicados
            if cleaned_item not in cleaned_data:
                cleaned_data.append(cleaned_item)
                
        return cleaned_data
        
    def _convert_types(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convierte los valores a tipos apropiados
        
        Args:
            item: Diccionario con datos
            
        Returns:
            Diccionario con valores convertidos
        """
        converted = {}
        
        for key, value in item.items():
            if isinstance(value, str):
                # Convertir números
                if value.isdigit():
                    converted[key] = int(value)
                # Convertir booleanos
                elif value.lower() in ['true', 'false']:
                    converted[key] = value.lower() == 'true'
                else:
                    converted[key] = value
            else:
                converted[key] = value
                
        return converted 