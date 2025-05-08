import argparse
import logging
import sys
from pathlib import Path
from typing import List, Dict, Any, Optional
from bs4 import BeautifulSoup
import requests
import os
from dotenv import load_dotenv

# Ajustar el path para imports relativos
current_dir = Path(__file__).parent
sys.path.append(str(current_dir.parent))

# Importar componentes
from src.common.config import get_config, COLLECTOR_LOGS, SCRAPER_LOGS, CLEANER_LOGS
from src.common.db import DatabaseConnection
from src.extractors.bulk_collector import BulkCollector
from src.extractors.bulk_scraper import BulkScraper
from src.transformers.business_transformer import BusinessTransformer
from src.loaders.database_loader import DatabaseLoader

# Cargar variables de entorno
load_dotenv()

def setup_logging():
    """Configura el logging para la aplicación"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('data/logs/main.log'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def main():
    """Función principal del ETL"""
    logger = setup_logging()
    logger.info("Iniciando proceso ETL")
    
    try:
        # Obtener configuración
        config = get_config()
        
        # Inicializar componentes
        collector = BulkCollector()
        scraper = BulkScraper()
        transformer = BusinessTransformer()
        loader = DatabaseLoader()
        
        # Recolectar URLs
        logger.info("Iniciando recolección de URLs")
        urls, chunks = collector.collect_urls()
        logger.info(f"Recolectadas {len(urls)} URLs en {len(chunks)} chunks")
        
        # Scraping de datos
        logger.info("Iniciando scraping de datos")
        scraped_data = scraper.scrape_urls(urls)
        logger.info(f"Scrapeados {len(scraped_data)} registros")
        
        # Transformar datos
        logger.info("Iniciando transformación de datos")
        transformed_data = transformer.transform(scraped_data)
        logger.info(f"Transformados {len(transformed_data)} registros")
        
        # Cargar datos
        logger.info("Iniciando carga de datos")
        loader.load(transformed_data)
        logger.info("Carga de datos completada")
        
        logger.info("Proceso ETL completado exitosamente")
        
    except Exception as e:
        logger.error(f"Error en el proceso ETL: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 