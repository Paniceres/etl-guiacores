# src/extractors/run_sequential_extraction.py

import argparse
import logging
import sys
import json
from typing import List, Optional, Dict, Any
from concurrent.futures import ProcessPoolExecutor, as_completed

# Asegurar que el directorio raíz del proyecto esté en el PATH para imports relativos
# Aunque en un contenedor Docker el PYTHONPATH debería configurarse correctamente,
# esto ayuda para ejecución local si es necesario.
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.common.config import get_config
from src.extractors.sequential_collector import SequentialCollector
from src.extractors.sequential_scraper import process_url_chunk_for_sequential # Importar la función para el pool
from src.main import chunkify # Reutilizar la función chunkify

# Configurar logging
# Usamos basicConfig aquí ya que este será un script standalone/entrypoint
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s',
    handlers=[
        logging.StreamHandler() # Log a la consola/stdout
        # Opcionalmente, añadir un FileHandler si se desea log a archivo en el contenedor
        # logging.FileHandler('data/logs/sequential_extraction.log')
    ]
)

logger = logging.getLogger(__name__)

def run_extraction(
    rubros: Optional[List[str]] = None,
    localidades: Optional[List[str]] = None,
    output_path: str = './data/extracted/sequential_raw_data.json'
) -> Dict[str, Any]:
    """
    Ejecuta la fase de extracción (recolección y scraping) para el modo secuencial.
    Guarda los datos scrapeados en un archivo JSON.
    """
    logger.info("Iniciando fase de Extracción Secuencial.")
    collector = None # Inicializar collector a None
    all_scraped_data = []
    try:
        config = get_config()
        collector = SequentialCollector(rubros=rubros, localidades=localidades, config=config)

        logger.info("Recolectando URLs (Sequential)")
        urls_dict: Dict[str, str] = collector.collect_urls()
        # La limpieza del collector se hará en el finally block

        if not urls_dict:
            logger.warning("No se recolectaron URLs en modo Sequential.")
            return {"status": "warning", "message": "No se recolectaron URLs en modo Sequential.", "records_processed": 0}
        logger.info(f"Recolectadas {len(urls_dict)} URLs (Sequential)")

        # Preparar lista de diccionarios para el scraper
        urls_list_for_scraper = [{"id_negocio": id_negocio, "url": url_value} for id_negocio, url_value in urls_dict.items()]

        logger.info("Iniciando scraping de datos (Sequential) con procesamiento paralelo.")

        max_workers = config.get('MAX_WORKERS', 4)
        chunk_size_scraper = config.get('CHUNK_SIZE_SCRAPER', 10)

        logger.info(f"Usando {max_workers} workers y chunk size {chunk_size_scraper} para scraping paralelo.")

        futures = []
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            url_chunks = list(chunkify(urls_list_for_scraper, chunk_size_scraper))
            logger.info(f"Dividiendo {len(urls_list_for_scraper)} URLs en {len(url_chunks)} trozos para scraping.")

            for i, chunk in enumerate(url_chunks):
                logger.info(f"Enviando trozo {i+1}/{len(url_chunks)} de URLs a un worker.")
                # process_url_chunk_for_sequential necesita el chunk y config
                futures.append(executor.submit(process_url_chunk_for_sequential, chunk, config))

            for i, future in enumerate(as_completed(futures)):
                logger.info(f"Recuperando resultado del trozo {i+1}.")
                try:
                    chunk_result = future.result()
                    if chunk_result:
                        all_scraped_data.extend(chunk_result)
                        logger.info(f"Añadidos {len(chunk_result)} registros. Total scrapeado hasta ahora: {len(all_scraped_data)}")
                    else:
                        logger.warning(f"El trozo {i+1} no devolvió datos scrapeados.")
                except Exception as exc:
                    logger.error(f"Una tarea de scraping (trozo {i+1}) generó una excepción: {exc}", exc_info=True)

        logger.info(f"Scrapeados {len(all_scraped_data)} registros (Sequential).")

        if not all_scraped_data:
            logger.warning("No se scrapearon datos en modo Sequential.")
            return {"status": "warning", "message": "No se scrapearon datos en modo sequential.", "records_processed": 0}

        # Asegurar que el directorio de salida exista
        output_dir = os.path.dirname(output_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
            logger.info(f"Directorio de salida creado: {output_dir}")
            
        # Guardar datos scrapeados en archivo JSON
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(all_scraped_data, f, indent=4, ensure_ascii=False)
        logger.info(f"Datos scrapeados guardados en: {output_path}")

        logger.info("Fase de Extracción Secuencial completada exitosamente.")
        return {"status": "success", "message": "Extracción Sequential completada.", "records_processed": len(all_scraped_data), "output_path": output_path}

    except Exception as e:
        logger.error(f"Error en la fase de Extracción Secuencial: {e}", exc_info=True)
        return {"status": "error", "message": str(e)}
    finally:
        # Asegurar que el driver del collector se limpie
        if collector and hasattr(collector, 'cleanup') and callable(collector.cleanup):
            try:
                collector.cleanup()
                logger.info("Driver del collector sequential limpiado en finally block.")
            except Exception as cleanup_error:
                logger.error(f"Error durante la limpieza final del collector sequential: {cleanup_error}", exc_info=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ejecuta la fase de Extracción Secuencial del ETL.")
    parser.add_argument("--rubros", type=str, help="Lista de rubros separados por coma (ej., 'restaurantes,hoteles'). Opcional.")
    parser.add_argument("--localidades", type=str, help="Lista de localidades separadas por coma. Opcional.")
    parser.add_argument("--output_path", type=str, default='./data/extracted/sequential_raw_data.json', help="Ruta al archivo de salida para los datos scrapeados.")

    args = parser.parse_args()

    rubros_list = [r.strip() for r in args.rubros.split(',') if r.strip()] if args.rubros else None
    localidades_list = [l.strip() for l in args.localidades.split(',') if l.strip()] if args.localidades else None

    result = run_extraction(rubros_list, localidades_list, args.output_path)

    if result.get("status") == "error":
        logger.error(f"La extracción falló: {result.get('message')}")
        sys.exit(1) # Salir con código de error si falla
    elif result.get("status") == "warning":
         logger.warning(f"Extracción completada con advertencias: {result.get('message')}")
         # Podríamos salir con un código diferente o 0 dependiendo de la política para warnings
         sys.exit(0)
    else:
        logger.info(f"Extracción completada exitosamente. Registros procesados: {result.get('records_processed')}")
        logger.info(f"Datos guardados en: {result.get('output_path')}")
        sys.exit(0)
