import logging
import re
from typing import List, Dict, Any
from urllib.parse import urlparse
from ..common.config import get_config

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data/logs/transformer/business_transformer.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class BusinessTransformer:
    """Transformador para limpiar y validar datos de negocios"""
    
    def __init__(self):
        self.config = get_config()
        self.transformer_config = self.config['transformer']
        
    def _clean_text(self, text: str) -> str:
        """Limpia texto eliminando espacios extra y caracteres no deseados"""
        if not text or text == 'N/A':
            return 'N/A'
        return ' '.join(text.split())
        
    def _normalize_phone(self, phone: str) -> str:
        """Normaliza número de teléfono"""
        if not phone or phone == 'N/A':
            return 'N/A'
            
        # Eliminar caracteres no numéricos
        digits = re.sub(r'\D', '', phone)
        
        # Validar formato
        if len(digits) < 10:
            return 'N/A'
            
        # Formatear según longitud
        if len(digits) == 10:  # Número local
            return f"{digits[:3]}-{digits[3:7]}-{digits[7:]}"
        elif len(digits) == 11:  # Número con código de área
            return f"{digits[:2]}-{digits[2:5]}-{digits[5:8]}-{digits[8:]}"
        else:
            return digits
            
    def _validate_email(self, email: str) -> str:
        """Valida formato de email"""
        if not email or email == 'N/A':
            return 'N/A'
            
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return email if re.match(pattern, email) else 'N/A'
        
    def _validate_url(self, url: str) -> str:
        """Valida formato de URL"""
        if not url or url == 'N/A':
            return 'N/A'
            
        try:
            result = urlparse(url)
            return url if all([result.scheme, result.netloc]) else 'N/A'
        except:
            return 'N/A'
            
    def transform(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transforma y valida los datos de negocios
        
        Args:
            data (List[Dict[str, Any]]): Lista de diccionarios con datos de negocios
            
        Returns:
            List[Dict[str, Any]]: Lista de diccionarios con datos transformados
        """
        transformed_data = []
        
        for item in data:
            try:
                # Limpiar texto
                if self.transformer_config['clean_text']:
                    for field in ['nombre', 'direccion', 'descripcion', 'rubros']:
                        if field in item:
                            item[field] = self._clean_text(item[field])
                            
                # Normalizar teléfonos
                if self.transformer_config['normalize_phones']:
                    if 'telefonos' in item:
                        phones = [self._normalize_phone(p.strip()) for p in item['telefonos'].split(',')]
                        item['telefonos'] = ', '.join(p for p in phones if p != 'N/A') or 'N/A'
                    if 'whatsapp' in item:
                        item['whatsapp'] = self._normalize_phone(item['whatsapp'])
                        
                # Validar email
                if self.transformer_config['validate_emails'] and 'email' in item:
                    item['email'] = self._validate_email(item['email'])
                    
                # Validar URLs
                if self.transformer_config['validate_urls']:
                    for field in ['sitio_web', 'facebook', 'instagram']:
                        if field in item:
                            item[field] = self._validate_url(item[field])
                            
                transformed_data.append(item)
                
            except Exception as e:
                logger.error(f"Error al transformar item: {e}")
                continue
                
        logger.info(f"Transformados {len(transformed_data)} de {len(data)} registros")
        return transformed_data 