import logging
import sys
from pathlib import Path
from typing import List, Optional, Dict, Any, Iterable, TypeVar
from datetime import datetime
import os # Importar os para DataVersioning

from dotenv import load_dotenv

# Ajustar el path para permitir imports relativos cuando se ejecuta como script.
# No es necesario agregar el path manualmente si ya está en PYTHONPATH

# Importación para procesamiento paralelo (previsto para run_sequential_etl)
from concurrent.futures import ProcessPoolExecutor, as_completed

# Importar componentes del sistema ETL
from src.common.config import get_config
from src.extractors.bulk_collector import BulkCollector
from src.extractors.bulk_scraper import BulkScraper
from src.extractors.sequential_collector import SequentialCollector
# Se espera que GuiaCoresScraper y su función process_url_chunk_for_sequential
# sean utilizados por ProcessPoolExecutor dentro de run_sequential_etl, según la arquitectura discutida.
from src.extractors.sequential_scraper import GuiaCoresScraper, process_url_chunk_for_sequential

from src.transformers.business_transformer import BusinessTransformer
from src.loaders.database_loader import DatabaseLoader
from src.loaders.file_loader import FileLoader

# Importar DataVersioning (asegurarse de que esté disponible o añadirla)
# from src.common.versioning import DataVersioning # Descomentar si DataVersioning existe aquí

# Cargar variables de entorno desde .env
load_dotenv()

T = TypeVar('T') # Tipo genérico para la función chunkify

# Instancia global del logger
logger = logging.getLogger(__name__)

def setup_logging_if_not_configured():
    """Configura el logging usando basicConfig si no hay manejadores (handlers) ya establecidos.

    Esto asegura que el logging se configure una sola vez, ya sea que el script
    se ejecute directamente o se importe como un módulo. Los logs se dirigen
    a 'data/logs/etl_api.log' y a la salida estándar (stdout).
    """
    if not logger.handlers:  # Verificar si ya se han añadido manejadores
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s',
            handlers=[
                logging.FileHandler('data/logs/etl_api.log'),  # Log para ETLs iniciados por API
                logging.StreamHandler()
            ]
        )

# Configurar el logging al momento de importar el módulo
setup_logging_if_not_configured()

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

def _get_loaders(output_type: str, config: dict) -> List[Any]:
    """Instancia y devuelve una lista de objetos 'loader' basados en el output_type.

    Args:
        output_type: Una cadena que indica la salida deseada.
                     Acepta "database", "file", o "both".
        config: El diccionario de configuración de la aplicación.

    Returns:
        List[Any]: Una lista conteniendo objetos 'loader' instanciados (DatabaseLoader, FileLoader).

    Raises:
        ValueError: Si output_type no es uno de los valores aceptados.
    """
    loaders = []
    if output_type in ["database", "both"]:
        loaders.append(DatabaseLoader(config=config))
    if output_type in ["file", "both"]:
        # Asumiendo que FileLoader está implementado y acepta config si es necesario
        loaders.append(FileLoader(config=config))

    if not loaders:
        raise ValueError(f"Tipo de salida inválido: {output_type}. Debe ser 'database', 'file', o 'both'.")
    return loaders

def run_bulk_etl(start_id: int, end_id: int, output: str = "both") -> Dict[str, Any]:
    """Ejecuta el proceso ETL en modo 'bulk' (masivo) para un rango de IDs dado.

    Esta función orquesta la recolección de URLs (vía BulkCollector),
    el scraping de datos de esas URLs (vía BulkScraper), la transformación
    de los datos scrapeados (vía BusinessTransformer), y la carga de los
    datos transformados a los destinos de salida especificados.

    Args:
        start_id: El ID inicial para el rango de procesamiento masivo.
        end_id: El ID final para el rango de procesamiento masivo.
        output: El destino para los datos de salida.
                Puede ser "file", "database", o "both". Por defecto es "both".

    Returns:
        Dict[str, Any]: Un diccionario conteniendo el estado del proceso ETL,
                        un mensaje, y el número de registros procesados.
    """
    logger.info(f"Iniciando ETL BULK. Start ID: {start_id}, End ID: {end_id}, Output: {output}")
    try:
        config = get_config()
        collector = BulkCollector(config=config, start_id=start_id, end_id=end_id)
        scraper = BulkScraper(config=config)
        transformer = BusinessTransformer(config=config)
        loaders = _get_loaders(output, config)

        logger.info("Recolectando URLs (Bulk)")
        urls_data = collector.collect_urls()
        logger.info(f"Recolectadas {len(urls_data)} URLs (Bulk)")

        if not urls_data:
            logger.warning("No se recolectaron URLs en modo Bulk. El ETL se detendrá.")
            return {"status": "warning", "message": "No se recolectaron URLs en modo Bulk.", "records_processed": 0}

        logger.info("Haciendo scraping de datos (Bulk)")
        scraped_data = scraper.scrape_urls(urls_data)
        logger.info(f"Scrapeados {len(scraped_data)} registros (Bulk)")

        if not scraped_data:
            logger.warning("No se scrapearon datos en modo Bulk. El ETL se detendrá.")
            return {"status": "warning", "message": "No se scrapearon datos en modo Bulk.", "records_processed": 0}

        logger.info("Transformando datos (Bulk)")
        transformed_data = transformer.transform(scraped_data)
        logger.info(f"Transformados {len(transformed_data)} registros (Bulk)")

        logger.info("Cargando datos (Bulk)")
        for loader in loaders:
            loader.load(transformed_data)
        logger.info(f"Carga de datos completada (Bulk) usando {output}")

        logger.info("Proceso ETL BULK completado exitosamente.")
        return {"status": "success", "message": "ETL Bulk completado.", "records_processed": len(transformed_data)}
    except Exception as e:
        logger.error(f"Error en el proceso ETL BULK: {e}", exc_info=True)
        return {"status": "error", "message": str(e)}

def process_manual_input(url: Optional[str] = None, file: Optional[str] = None, output: str = "both") -> Dict[str, Any]:
    """Ejecuta el proceso ETL para una única URL (modo manual).

    Esta función orquesta el procesamiento de datos para el modo manual.
    Puede scrapear una única URL o leer y procesar archivos HTML de un directorio.
    Luego, transforma los datos procesados y los carga a los destinos de salida especificados.

    Args:
 url: La URL desde la cual extraer datos. Opcional.

        file: La ruta al directorio que contiene archivos HTML para procesar. Opcional.

        output: El destino para los datos de salida.
                Puede ser "file", "database", o "both". Por defecto es "both".

    Returns:
        Dict[str, Any]: Un diccionario conteniendo el estado del proceso ETL,
                        un mensaje, y el número de registros procesados.
    """
    logger.info(f"Iniciando ETL MANUAL. URL: {url}, Output: {output}")
    scraped_data = []
    try:
        from src.extractors.manual_scraper import ManualScraper
        config = get_config()
        transformer = BusinessTransformer(config=config)
        loaders = _get_loaders(output, config)

        if url:
            logger.info(f"Haciendo scraping de URL (Manual): {url}")
            scraper = ManualScraper(config=config)
            scraped_data = scraper.scrape_single_url(url)
            if not scraped_data:
                 logger.warning(f"No se scrapearon datos para la URL (Manual): {url}")
                 return {"status": "warning", "message": f"No se scrapearon datos para la URL: {url}", "records_processed": 0}
            logger.info(f"Scrapeados {len(scraped_data)} registros (Manual)")

        elif file:
            logger.info(f"Procesando archivos HTML desde: {file}")
            html_files_path = Path(file)
            if not html_files_path.is_dir():
                 logger.error(f"Error: La ruta proporcionada no es un directorio: {file}")
                 return {"status": "error", "message": f"La ruta proporcionada no es un directorio: {file}"}

            for html_file in html_files_path.glob("*.html"):
                try:
                    with open(html_file, "r", encoding="utf-8") as f:
                        html_content = f.read()
                    # Process the html_content here. This is where you'd need
                    # to adapt your scraping logic to work with raw HTML.
                    # For now, let's just store the content.
                    scraped_data.append({"html_content": html_content}) # You'll need to define a structure for this

                except Exception as e:
                    logger.error(f"Error al leer o procesar el archivo {html_file}: {e}", exc_info=True)

            if not scraped_data:
                logger.warning(f"No se encontraron archivos HTML en {file} o no se pudieron procesar.")
                return {"status": "warning", "message": f"No se encontraron archivos HTML en {file} o no se pudieron procesar.", "records_processed": 0}
            logger.info(f"Procesados {len(scraped_data)} archivos HTML.")

        logger.info("Transformando datos (Manual)")
        transformed_data = transformer.transform(scraped_data)
        logger.info(f"Transformados {len(transformed_data)} registros (Manual)")

        logger.info("Cargando datos (Manual)")
        for loader in loaders:
            loader.load(transformed_data)
        logger.info(f"Carga de datos completada (Manual) usando {output}")
        
        # Guardar leads en CSV con versionado
        # Asegurarse de que DataVersioning esté importado y disponible
        # if output in ["file", "both"]:
        #     try:
        #         # from src.extractors.manual_scraper import save_leads # Si save_leads está aquí
        #         # save_leads(transformed_data)
        #         # versioner = DataVersioning(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        #         # versioned_path = versioner.version_csv_file('data/raw/csv/estudiosContables_leads.csv')
        #         # logger.info(f"Archivo CSV versionado guardado en: {versioned_path}")
        #         pass # Placeholder if DataVersioning is not ready or logic needs refinement
        #     except Exception as e:
        #         logger.error(f"Error al guardar leads en CSV: {e}")

        logger.info("Proceso ETL MANUAL completado exitosamente.")
        return {"status": "success", "message": "ETL Manual completado.", "records_processed": len(transformed_data)}
    except Exception as e:
        logger.error(f"Error en el proceso ETL MANUAL: {e}", exc_info=True)
        return {"status": "error", "message": str(e)}

def run_sequential_etl(rubros: Optional[List[str]] = None, localidades: Optional[List[str]] = None, output: str = "both") -> Dict[str, Any]:
    """Ejecuta el proceso ETL secuencialmente basado en categorías (rubros) y localidades.

    Esta función primero recolecta URLs basadas en los rubros y localidades provistos
    usando SequentialCollector. Luego, hace scraping de datos de estas URLs
    en paralelo usando instancias de GuiaCoresScraper gestionadas por un ProcessPoolExecutor.
    Los datos scrapeados son luego transformados y cargados.

    Args:
        rubros: Una lista opcional de categorías (rubros) a procesar.
        localidades: Una lista opcional de localidades por las cuales filtrar.
        output: El destino para los datos de salida.
                Puede ser "file", "database", o "both". Por defecto es "both".

    Returns:
        Dict[str, Any]: Un diccionario conteniendo el estado del proceso ETL,
                        un mensaje, y el número de registros procesados.
    """
    logger.info(f"Iniciando ETL SEQUENTIAL. Rubros: {rubros}, Localidades: {localidades}, Output: {output}")
    all_scraped_data = [] 
    collector = None # Inicializar collector a None
    try:
        from src.extractors.sequential_collector import SequentialCollector
        from src.extractors.sequential_scraper import GuiaCoresScraper, process_url_chunk_for_sequential # Asegurar importación local si es necesario
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
        chunk_size_scraper = config.get('CHUNK_SIZE_SCRAPER', 10) # Para distribuir trabajo
        
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

        transformer = BusinessTransformer(config=config)
        loaders = _get_loaders(output, config)

        logger.info("Transformando datos (Sequential)")
        transformed_data = transformer.transform(all_scraped_data)
        logger.info(f"Transformados {len(transformed_data)} registros (Sequential)")

        logger.info("Cargando datos (Sequential)")
        for loader in loaders:
            loader.load(transformed_data)
        logger.info(f"Carga de datos completada (Sequential) usando {output}")

        logger.info("Proceso ETL SEQUENTIAL completado.")
        return {"status": "success", "message": "ETL Sequential completado.", "records_processed": len(transformed_data)}

    except Exception as e:
        logger.error(f"Error en el proceso ETL SEQUENTIAL: {e}", exc_info=True)
        return {"status": "error", "message": str(e)}
    finally:
        # Asegurar que el driver del collector se limpie
        if collector and hasattr(collector, 'cleanup') and callable(collector.cleanup):
            try:
                collector.cleanup()
                logger.info("Driver del collector sequential limpiado en finally block.")
            except Exception as cleanup_error:
                logger.error(f"Error durante la limpieza final del collector sequential: {cleanup_error}", exc_info=True)


# Bloque principal para ejecución CLI (Interfaz de Línea de Comandos)
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Ejecutor de Procesos ETL para Guia Cores")
    # 'required=True' para subparsers asegura que un modo siempre sea especificado.
    subparsers = parser.add_subparsers(dest="mode", help="Modo ETL a ejecutar", required=True)

    # Modo Bulk (Masivo)
    bulk_parser = subparsers.add_parser("bulk", help="Ejecutar ETL en modo masivo para un rango de IDs.")
    bulk_parser.add_argument("--start_id", type=int, required=True, help="ID inicial para el procesamiento masivo.")
    bulk_parser.add_argument("--end_id", type=int, required=True, help="ID final para el procesamiento masivo.")
    bulk_parser.add_argument("--output", type=str, default="both", choices=["file", "database", "both"], help="Destino de salida (file, database, o both).")

    # Modo Manual
    manual_parser = subparsers.add_parser("manual", help="Ejecutar ETL para una URL única.")
    # Create a mutually exclusive group for --url and --file
    manual_group = manual_parser.add_mutually_exclusive_group(required=True)
    manual_group.add_argument("--url", type=str, help="La URL específica a scrapear.")
    manual_group.add_argument("--file", type=str, help="Ruta al archivo de entrada.")

    manual_parser.add_argument("--output", type=str, default="both", choices=["file", "database", "both"], help="Destino de salida.")

    # Modo Sequential (Secuencial)
    sequential_parser = subparsers.add_parser("sequential", help="Ejecutar ETL secuencialmente basado en categorías (rubros) y/o localidades.")
    sequential_parser.add_argument("--rubros", type=str, help="Lista de rubros separados por coma (ej., 'restaurantes,hoteles'). Opcional.")
    sequential_parser.add_argument("--localidades", type=str, help="Lista de localidades separadas por coma. Opcional.")
    sequential_parser.add_argument("--output", type=str, default="both", choices=["file", "database", "both"], help="Destino de salida.")

    args = parser.parse_args()

    # setup_logging_if_not_configured() ya se llama a nivel de módulo, asegurando logs para CLI.

    try:
        if args.mode == "bulk":
            run_bulk_etl(args.start_id, args.end_id, args.output)
        elif args.mode == "manual":
            # Depending on which argument was provided, call the appropriate function
            if args.url:
                process_manual_input(url=args.url, output=args.output)
            elif args.file: 
                process_manual_input(file=args.file, output=args.output)
        elif args.mode == "sequential":
            rubros_list = [r.strip() for r in args.rubros.split(',') if r.strip()] if args.rubros else None
            localidades_list = [l.strip() for l in args.localidades.split(',') if l.strip()] if args.localidades else None
            run_sequential_etl(rubros_list, localidades_list, args.output)
    except Exception as e:
        logger.error(f"Error en la ejecución del ETL: {e}", exc_info=True)
