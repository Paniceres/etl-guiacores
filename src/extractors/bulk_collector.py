import logging
from typing import List, Tuple
from ..common.config import get_config

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data/logs/collector/collector_guiaCores_bulk.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class BulkCollector:
    """Colector para el modo bulk que genera y divide IDs en chunks"""
    
    def __init__(self):
        self.config = get_config()
        self.bulk_config = self.config['extractor']['bulk']
        self.start_id = self.bulk_config['start_id']
        self.end_id = self.bulk_config['end_id']
        self.chunk_size = self.bulk_config['chunk_size']
        self.base_url = self.bulk_config['base_url']
        
    def generate_urls(self, start_id: int = None, end_id: int = None) -> List[str]:
        """
        Genera una lista de URLs basadas en un rango de IDs
        
        Args:
            start_id (int, optional): ID inicial. Si no se especifica, usa el valor de configuración.
            end_id (int, optional): ID final. Si no se especifica, usa el valor de configuración.
            
        Returns:
            List[str]: Lista de URLs generadas
        """
        start = start_id if start_id is not None else self.start_id
        end = end_id if end_id is not None else self.end_id
        
        logger.info(f"Generando URLs para IDs desde {start} hasta {end}")
        urls = [f"{self.base_url}{id}" for id in range(start, end + 1)]
        logger.info(f"Generadas {len(urls)} URLs")
        return urls
        
    def generate_chunks(self, urls: List[str]) -> List[List[str]]:
        """
        Divide una lista de URLs en chunks para procesamiento paralelo
        
        Args:
            urls (List[str]): Lista de URLs a dividir
            
        Returns:
            List[List[str]]: Lista de chunks de URLs
        """
        chunks = [urls[i:i + self.chunk_size] for i in range(0, len(urls), self.chunk_size)]
        logger.info(f"Divididas {len(urls)} URLs en {len(chunks)} chunks de tamaño {self.chunk_size}")
        return chunks
        
    def collect_urls(self, start_id: int = None, end_id: int = None) -> Tuple[List[str], List[List[str]]]:
        """
        Genera URLs y las divide en chunks para procesamiento
        
        Args:
            start_id (int, optional): ID inicial. Si no se especifica, usa el valor de configuración.
            end_id (int, optional): ID final. Si no se especifica, usa el valor de configuración.
            
        Returns:
            Tuple[List[str], List[List[str]]]: Tupla con la lista completa de URLs y la lista de chunks
        """
        try:
            urls = self.generate_urls(start_id, end_id)
            chunks = self.generate_chunks(urls)
            return urls, chunks
            
        except Exception as e:
            logger.error(f"Error al generar URLs y chunks: {e}")
            return [], [] 