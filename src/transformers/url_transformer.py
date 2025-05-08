from typing import List, Dict, Any
from urllib.parse import urlparse, urljoin
from ..common.base import BaseTransformer
from ..common.config import get_config

class URLTransformer(BaseTransformer):
    """Transformador para normalizar y validar URLs"""
    
    def __init__(self):
        super().__init__('url_transformer')
        self.config = get_config()['transformer']['url']
        
    def transform(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transforma las URLs en los datos
        
        Args:
            data: Lista de diccionarios con datos
            
        Returns:
            Lista de diccionarios con URLs transformadas
        """
        for item in data:
            if 'url' in item:
                item['url'] = self._normalize_url(item['url'])
        return data
        
    def _normalize_url(self, url: str) -> str:
        """
        Normaliza una URL
        
        Args:
            url: URL a normalizar
            
        Returns:
            URL normalizada
        """
        # Parsear URL
        parsed = urlparse(url)
        
        # Validar esquema
        if parsed.scheme not in self.config['allowed_schemes']:
            url = f"https://{url}"
            
        # Validar dominio
        parsed = urlparse(url)
        if parsed.netloc not in self.config['allowed_domains']:
            raise ValueError(f"Dominio no permitido: {parsed.netloc}")
            
        # Truncar si es necesario
        if len(url) > self.config['max_length']:
            url = url[:self.config['max_length']]
            
        return url 