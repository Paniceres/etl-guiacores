# src/extractors/run_extraction.py

import argparse
import logging
import json
import sys
import os
from typing import Dict, Any, List, Optional, Iterable, TypeVar
from datetime import datetime # Importar datetime

# Importación para procesamiento paralelo
from concurrent.futures import ProcessPoolExecutor, as_completed

# Asegurar que el directorio raíz del proyecto esté en el PATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.common.config import get_config
# Importar los extractores necesarios (ej: SequentialCollector, GuiaCoresScraper)
from src.extractors.sequential_collector import SequentialCollector
from src.extractors.sequential_scraper import GuiaCoresScraper, process_url_chunk_for_sequential

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

T = TypeVar('T') # Tipo genérico para la función chunkify

def chunkify(data: List[T], chunk_size: int) -> Iterable[List[T]]:
    """Divide una lista en sub-listas (trozos o chunks) de un tamaño máximo especificado.

    Args:
        data: La lista a ser dividida en trozos.
        chunk_size: El tamaño máximo de cada trozo.

    Yields:
        Iterable[List[T]]: Un iterable de listas, donde cada lista interna es un trozo.
    """
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]


def run_extraction(
    rubros: Optional[List[str]] = None,
    localidades: Optional[List[str]] = None,
    output_path: str = './data/extracted/sequential_raw_data.json'
) -> Dict[str, Any]:
    """
    Ejecuta la fase de extracción.
    Recolecta URLs y scrapea datos basado en rubros y localidades, guardando datos crudos en output_path.
    """
    logger.info("Iniciando fase de Extracción.")
    all_scraped_data = []
    collector = None # Inicializar collector a None
    try:
        config = get_config()
        collector = SequentialCollector(rubros=rubros, localidades=localidades, config=config)

        logger.info("Recolectando URLs (Sequential)")
        # urls_dict es un Dict[str, str] de {id_negocio: url}
        urls_dict: Dict[str, str] = collector.collect_urls()
        # La limpieza del collector se hará en el finally block

        if not urls_dict:
            logger.warning("No se recolectaron URLs en modo Sequential. El ETL se detendrá.")
            return {"status": "warning", "message": "No se recolectaron URLs en modo Sequential.", "records_processed": 0}
        logger.info(f"Recolectadas {len(urls_dict)} URLs (Sequential)")

        # Preparar lista de diccionarios para el scraper: [{'id_negocio': '...', 'url': '...'}, ...]
        urls_list_for_scraper = [{"id_negocio": id_negocio, "url": url_value} for id_negocio, url_value in urls_dict.items()]

        logger.info("Iniciando scraping de datos (Sequential) con procesamiento paralelo.")
        
        # Implementación real con ProcessPoolExecutor
        max_workers = config.get('MAX_WORKERS', 4) # Obtener de config o un valor por defecto
        chunk_size_scraper = config.get('CHUNK_SIZE_SIZE', 10) # Para distribuir trabajo - CORREGIDO: debería ser CHUNK_SIZE_SCRAPER si existe en config
        
        # Ajustar chunk_size_scraper si no está definido en config o si el nombre de la clave es diferente
        chunk_size_scraper = config.get('CHUNK_SIZE_SCRAPER', 10) if config.get('CHUNK_SIZE_SCRAPER', None) is not None else config.get('CHUNK_SIZE_SIZE', 10) # Fallback para compatibilidad
        
        logger.info(f"Usando {max_workers} workers y chunk size {chunk_size_scraper} para scraping paralelo.")
        
        futures = []
        # Usar ProcessPoolExecutor para ejecutar process_url_chunk_for_sequential en paralelo
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            # Dividir la lista de URLs en trozos
            url_chunks = list(chunkify(urls_list_for_scraper, chunk_size_scraper))
            logger.info(f"Dividiendo {len(urls_list_for_scraper)} URLs en {len(url_chunks)} trozos.")
            
            # Enviar cada trozo al pool de procesos
            for i, chunk in enumerate(url_chunks):
                # process_url_chunk_for_sequential debe aceptar el chunk y config
                logger.info(f"Enviando trozo {i+1}/{len(url_chunks)} de URLs a un worker.")
                futures.append(executor.submit(process_url_chunk_for_sequential, chunk, config))

            # Recolectar resultados a medida que las tareas se completan
            for i, future in enumerate(as_completed(futures)):
                logger.info(f"Recuperando resultado del trozo {i+1}/{len(url_chunks)}.")
                try:
                    chunk_result = future.result()
                    if chunk_result:
                        all_scraped_data.extend(chunk_result)
                        logger.info(f"Añadidos {len(chunk_result)} registros. Total scrapeado hasta ahora: {len(all_scraped_data)}")
                    else:
                         logger.warning(f"El trozo {i+1} no devolvió datos scrapeados.")
                except Exception as exc:
                    # Registrar la excepción pero permitir que otros procesos continúen
                    logger.error(f"Una tarea de scraping (trozo {i+1}) generó una excepción: {exc}", exc_info=True)
        
        # Fin de la implementación real con ProcessPoolExecutor

        logger.info(f"Scrapeados {len(all_scraped_data)} registros (Sequential).")

        if not all_scraped_data:
            logger.warning("No se scrapearon datos en modo Sequential. El ETL se detendrá.")
            return {"status": "warning", "message": "No se scrapearon datos en modo sequential.", "records_processed": 0}

        # Asegurar que el directorio de salida exista
        output_dir = os.path.dirname(output_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
            logger.info(f"Directorio de salida creado: {output_dir}")

        # Guardar datos extraídos en archivo JSON
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(all_scraped_data, f, indent=4, ensure_ascii=False)
            logger.info(f"Datos extraídos guardados en: {output_path}")
        except Exception as file_write_error:
            logger.error(f"Error al escribir el archivo de salida {output_path}: {file_write_error}", exc_info=True)
            return {"status": "error", "message": f"Error al escribir el archivo de salida: {file_write_error}"}

        logger.info("Fase de Extracción completada exitosamente.")
        return {"status": "success", "message": "Extracción completada.", "records_processed": len(all_scraped_data), "output_path": output_path}

    except Exception as e:
        logger.error(f"Error inesperado en la fase de Extracción: {e}", exc_info=True)
        return {"status": "error", "message": f"Error inesperado durante la extracción: {str(e)}"}
    finally:
        # Asegurar la limpieza de recursos si es necesario (ej. driver de Selenium)
        if collector and hasattr(collector, 'cleanup') and callable(collector.cleanup):
            try:
                collector.cleanup()
                logger.info("Recursos del collector limpiados en finally block.")
            except Exception as cleanup_error:
                logger.error(f"Error durante la limpieza final del collector: {cleanup_error}", exc_info=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ejecuta la fase de Extracción del ETL.")
    parser.add_argument("--rubros", type=str, help="Lista de rubros separados por coma (ej., 'restaurantes,hoteles'). Opcional.")
    parser.add_argument("--localidades", type=str, help="Lista de localidades separadas por coma. Opcional.")
    parser.add_argument("--output_path", type=str, default='./data/extracted/sequential_raw_data.json', help="Ruta al archivo de salida para los datos extraídos.")

    args = parser.parse_args()
    
    rubros_list = [r.strip() for r in args.rubros.split(',') if r.strip()] if args.rubros else None
    localidades_list = [l.strip() for l in args.localidades.split(',') if l.strip()] if args.localidades else None

    result = run_extraction(rubros=rubros_list, localidades=localidades_list, output_path=args.output_path)

    if result.get("status") == "error":
        logger.error(f"La extracción falló: {result.get('message')}")
        sys.exit(1)
    elif result.get("status") == "warning":
         logger.warning(f"Extracción completada con advertencias: {result.get('message')}")
         sys.exit(0) # Considerar sys.exit(1) si un warning en extracción es crítico
    else:
        logger.info(f"Extracción completada exitosamente. Registros procesados: {result.get('records_processed')}")
        logger.info(f"Datos guardados en: {result.get('output_path')}")
        sys.exit(0)
