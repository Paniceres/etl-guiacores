import argparse
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
from bs4 import BeautifulSoup
import requests

from common.config import get_config
from common.db import DatabaseConnection
from common.utils import setup_logging, retry_on_error
from common.logger import setup_logger
from collectors.manual import ManualCollector
from collectors.bulk import BulkCollector
from collectors.sequential import SequentialCollector
from extractors.contact import ContactExtractor
from transformers.contact import ContactTransformer
from loaders.db import PostgresLoader
from extractors.bulk_extractor import BulkExtractor
from extractors.sequential_extractor import SequentialExtractor
from extractors.manual_extractor import ManualExtractor
from transformers.url_transformer import URLTransformer
from transformers.business_transformer import BusinessTransformer
from transformers.data_cleaner import DataCleaner
from loaders.database_loader import DatabaseLoader
from loaders.file_loader import FileLoader
from loaders.cache_loader import CacheLoader

class ETLPipeline:
    """Pipeline principal de ETL"""
    
    def __init__(self):
        self.logger = setup_logging('pipeline', 'main')
        self.config = get_config()
        self.db = None
        
    def setup(self) -> bool:
        """
        Configura el pipeline
        
        Returns:
            bool: True si la configuración fue exitosa
        """
        try:
            # Conectar a la base de datos
            self.db = DatabaseConnection()
            if not self.db.connect():
                return False
                
            # Crear tablas si no existen
            if not self.db.create_tables():
                return False
                
            self.logger.info("Pipeline configurado exitosamente")
            return True
            
        except Exception as e:
            self.logger.error(f"Error al configurar pipeline: {e}")
            return False
            
    def process_html(self, html_file: str) -> bool:
        """
        Procesa un archivo HTML
        
        Args:
            html_file (str): Ruta al archivo HTML
            
        Returns:
            bool: True si el procesamiento fue exitoso
        """
        try:
            # Leer archivo HTML
            with open(html_file, 'r', encoding='utf-8') as f:
                html_content = f.read()
                
            # Crear colector manual
            collector = ManualCollector()
            urls = collector.collect_urls(html_content)
            
            if not urls:
                self.logger.warning("No se encontraron URLs en el archivo")
                return False
                
            # Guardar URLs
            if not collector.save_urls(urls):
                return False
                
            # Procesar cada URL
            extractor = ContactExtractor()
            transformer = ContactTransformer()
            loader = PostgresLoader('businesses')
            
            for url in urls:
                # Obtener contenido HTML
                response = requests.get(url, timeout=self.config['app']['timeout'])
                response.raise_for_status()
                
                # Extraer datos
                soup = BeautifulSoup(response.text, 'html.parser')
                data = extractor.extract_data(soup)
                
                if not data:
                    continue
                    
                # Transformar datos
                clean_data = transformer.transform_data([data])
                
                if not clean_data:
                    continue
                    
                # Cargar datos
                loader.load_data(clean_data)
                
            self.logger.info("Procesamiento de HTML completado")
            return True
            
        except Exception as e:
            self.logger.error(f"Error al procesar HTML: {e}")
            return False
            
    def process_sequential(self, rubro: str = None, localidad: str = None) -> bool:
        """
        Procesa URLs secuencialmente con carga dinámica
        
        Args:
            rubro (str): Rubro a filtrar (opcional)
            localidad (str): Localidad a filtrar (opcional)
            
        Returns:
            bool: True si el procesamiento fue exitoso
        """
        try:
            # Crear colector secuencial
            collector = SequentialCollector(rubro=rubro, localidad=localidad)
            urls = collector.collect_urls()
            
            if not urls:
                self.logger.warning("No se encontraron URLs")
                return False
                
            # Guardar URLs
            if not collector.save_urls(urls):
                return False
                
            # Procesar cada URL
            extractor = ContactExtractor()
            transformer = ContactTransformer()
            loader = PostgresLoader('businesses')
            
            for url in urls:
                # Obtener contenido HTML
                response = requests.get(url, timeout=self.config['app']['timeout'])
                response.raise_for_status()
                
                # Extraer datos
                soup = BeautifulSoup(response.text, 'html.parser')
                data = extractor.extract_data(soup)
                
                if not data:
                    continue
                    
                # Transformar datos
                clean_data = transformer.transform_data([data])
                
                if not clean_data:
                    continue
                    
                # Cargar datos
                loader.load_data(clean_data)
                
            self.logger.info("Procesamiento secuencial completado")
            return True
            
        except Exception as e:
            self.logger.error(f"Error al procesar secuencialmente: {e}")
            return False
            
    def process_bulk(self, start_id: int = None, end_id: int = None) -> bool:
        """
        Procesa IDs en paralelo
        
        Args:
            start_id (int): ID inicial (opcional)
            end_id (int): ID final (opcional)
            
        Returns:
            bool: True si el procesamiento fue exitoso
        """
        try:
            # Crear colector bulk
            collector = BulkCollector()
            urls = collector.collect_urls(start_id=start_id, end_id=end_id)
            
            if not urls:
                self.logger.warning("No se encontraron URLs")
                return False
                
            # Guardar URLs
            if not collector.save_urls(urls):
                return False
                
            # Procesar cada URL
            extractor = ContactExtractor()
            transformer = ContactTransformer()
            loader = PostgresLoader('businesses')
            
            for url in urls:
                # Obtener contenido HTML
                response = requests.get(url, timeout=self.config['app']['timeout'])
                response.raise_for_status()
                
                # Extraer datos
                soup = BeautifulSoup(response.text, 'html.parser')
                data = extractor.extract_data(soup)
                
                if not data:
                    continue
                    
                # Transformar datos
                clean_data = transformer.transform_data([data])
                
                if not clean_data:
                    continue
                    
                # Cargar datos
                loader.load_data(clean_data)
                
            self.logger.info("Procesamiento bulk completado")
            return True
            
        except Exception as e:
            self.logger.error(f"Error al procesar bulk: {e}")
            return False
            
    def cleanup(self) -> None:
        """Limpia recursos del pipeline"""
        if self.db:
            self.db.disconnect()

def parse_args():
    """Parsea los argumentos de línea de comandos"""
    parser = argparse.ArgumentParser(description='ETL Guía Cores')
    
    # Modos de operación
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument('--html', nargs='?', const='data/html_samples',
                          help='Procesar archivo HTML manual')
    mode_group.add_argument('--sequential', action='store_true',
                          help='Procesar URLs secuencialmente')
    mode_group.add_argument('--bulk', action='store_true',
                          help='Procesar múltiples IDs en paralelo')
    
    # Filtros
    parser.add_argument('--rubro', help='Filtrar por rubro (separado por comas)')
    parser.add_argument('--localidad', help='Filtrar por localidad (separado por comas)')
    
    # Parámetros bulk
    parser.add_argument('--start-id', type=int, help='ID inicial para modo bulk')
    parser.add_argument('--end-id', type=int, help='ID final para modo bulk')
    
    # Opciones de salida
    parser.add_argument('--output', choices=['db', 'file', 'both'], default='both',
                      help='Formato de salida (db, file, both)')
    parser.add_argument('--format', choices=['json', 'csv'], default='json',
                      help='Formato de archivo de salida')
    
    return parser.parse_args()

def setup_pipeline(args: argparse.Namespace) -> Dict[str, Any]:
    """Configura el pipeline ETL según los argumentos"""
    config = get_config()
    
    # Configurar extractor
    if args.html:
        extractor = ManualExtractor(args.html)
    elif args.sequential:
        extractor = SequentialExtractor(args.rubro, args.localidad)
    else:  # bulk
        extractor = BulkExtractor()
        if args.start_id:
            extractor.config['min_id'] = args.start_id
        if args.end_id:
            extractor.config['max_id'] = args.end_id
    
    # Configurar transformadores
    transformers = [
        URLTransformer(),
        BusinessTransformer(),
        DataCleaner()
    ]
    
    # Configurar cargadores
    loaders = []
    if args.output in ['db', 'both']:
        loaders.append(DatabaseLoader())
    if args.output in ['file', 'both']:
        loaders.append(FileLoader(format=args.format))
    loaders.append(CacheLoader())
    
    return {
        'extractor': extractor,
        'transformers': transformers,
        'loaders': loaders
    }

def main():
    """Función principal"""
    # Configurar logging
    setup_logger()
    logger = logging.getLogger(__name__)
    
    try:
        # Parsear argumentos
        args = parse_args()
        logger.info(f"Argumentos: {args}")
        
        # Crear pipeline
        pipeline = ETLPipeline()
        
        # Configurar pipeline
        if not pipeline.setup():
            return
            
        # Procesar según argumentos
        if args.html:
            pipeline.process_html(args.html)
        elif args.sequential:
            pipeline.process_sequential(rubro=args.rubro, localidad=args.localidad)
        elif args.bulk:
            pipeline.process_bulk(start_id=args.start_id, end_id=args.end_id)
        else:
            parser.print_help()
            
        # Configurar pipeline ETL
        pipeline_etl = setup_pipeline(args)
        logger.info("Pipeline configurado")
        
        # Ejecutar pipeline
        logger.info("Iniciando extracción...")
        data = pipeline_etl['extractor'].extract()
        logger.info(f"Extracción completada: {len(data)} registros")
        
        # Transformar datos
        logger.info("Iniciando transformación...")
        for transformer in pipeline_etl['transformers']:
            data = transformer.transform(data)
        logger.info("Transformación completada")
        
        # Cargar datos
        logger.info("Iniciando carga...")
        for loader in pipeline_etl['loaders']:
            loader.load(data)
        logger.info("Carga completada")
        
        logger.info("Proceso finalizado exitosamente")
        
    except Exception as e:
        logger.error(f"Error en el proceso: {str(e)}", exc_info=True)
        raise

if __name__ == '__main__':
    main() 