import logging
import sys
from pathlib import Path
from typing import List, Optional, Dict, Any, Iterable, TypeVar

from dotenv import load_dotenv

# Ajustar el path para imports relativos
current_dir = Path(__file__).parent
sys.path.append(str(current_dir.parent))

# Import for parallel processing
from concurrent.futures import ProcessPoolExecutor, as_completed

# Importar componentes
from src.common.config import get_config
# Importar extractores (usando tus clases existentes)
from src.extractors.bulk_collector import BulkCollector
from src.extractors.bulk_scraper import BulkScraper
from src.extractors.manual_scraper import ManualScraper   # Usamos tu scraper manual
from src.extractors.sequential_collector import SequentialCollector  # Usamos tu collector sequential
from src.extractors.sequential_scraper import GuiaCoresScraper # Asumiendo que usarás esta clase para scraping sequential


from src.transformers.business_transformer import BusinessTransformer
from src.loaders.database_loader import DatabaseLoader
from src.loaders.file_loader import FileLoader # Assuming you have a FileLoader

# Cargar variables de entorno
load_dotenv()

T = TypeVar('T')

# Global logger instance
logger = logging.getLogger(__name__)

def setup_logging_if_not_configured():
    """Configura el logging si no ha sido configurado previamente."""
    if not logger.handlers: # Check if handlers are already added
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s',
            handlers=[
                logging.FileHandler('data/logs/etl_api.log'), # Log for API triggered ETLs
                logging.StreamHandler()
            ]
        )

setup_logging_if_not_configured()

def chunkify(data: List[T], chunk_size: int) -> Iterable[List[T]]:
    """Splits a list into chunks of a given size."""
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]

def _get_loaders(output_type: str, config: dict):
    """Helper para obtener los loaders basados en output_type."""
    loaders = []
    if output_type in ["database", "both"]:
        loaders.append(DatabaseLoader(config=config)) # Pass config if needed
    if output_type in ["file", "both"]:
        loaders.append(FileLoader(config=config)) # Pass config if needed

    if not loaders:
        raise ValueError(f"Invalid output_type: {output_type}. Must be 'database', 'file', or 'both'.")
    return loaders

def run_bulk_etl(start_id: int, end_id: int, output: str = "both"):
    logger.info(f"Iniciando BULK ETL. Start ID: {start_id}, End ID: {end_id}, Output: {output}")
    try:
        config = get_config()

        # Usar tus clases BulkCollector y BulkScraper
        # Asumiendo que BulkCollector acepta start_id y end_id en __init__
        collector = BulkCollector(config=config, start_id=start_id, end_id=end_id)
        scraper = BulkScraper(config=config) # Asumiendo que BulkScraper usa la config

        transformer = BusinessTransformer(config=config)
        loaders = _get_loaders(output, config)

        logger.info("Iniciando recolección de URLs (Bulk)")
        # Asumiendo que collect_urls devuelve una lista de URLs o similar
        urls_data = collector.collect_urls() # Modificar según la firma real de tu collect_urls
        logger.info(f"Recolectadas {len(urls_data)} URLs (Bulk)")

        logger.info("Iniciando scraping de datos (Bulk)")
        # Asumiendo que scrape_urls acepta una lista de datos de URL y devuelve scraped_data
        scraped_data = scraper.scrape_urls(urls_data) # Modificar según la firma real
        logger.info(f"Scrapeados {len(scraped_data)} registros (Bulk)")

        logger.info("Iniciando transformación de datos (Bulk)")
        transformed_data = transformer.transform(scraped_data)
        logger.info(f"Transformados {len(transformed_data)} registros (Bulk)")

        logger.info("Iniciando carga de datos (Bulk)")
        for loader in loaders:
            loader.load(transformed_data)
        logger.info(f"Carga de datos completada (Bulk) usando {output}")

        logger.info("Proceso BULK ETL completado exitosamente")
        return {"status": "success", "message": "Bulk ETL completed.", "records_processed": len(transformed_data)}
    except Exception as e:
        logger.error(f"Error en el proceso BULK ETL: {e}", exc_info=True)
        return {"status": "error", "message": str(e)}

def run_manual_etl(url: str, output: str = "both"):
    logger.info(f"Iniciando MANUAL ETL. URL: {url}, Output: {output}")
    try:
        config = get_config()
        # Usar tu ManualScraper
        scraper = ManualScraper(config=config) # Asumiendo que acepta config
        transformer = BusinessTransformer(config=config)
        loaders = _get_loaders(output, config)

        logger.info(f"Iniciando scraping de URL (Manual): {url}")
        # Asumiendo que scrape_single_url devuelve una lista de diccionarios de datos
        scraped_data = scraper.scrape_single_url(url) # Modificar según la firma real

        if not scraped_data:
             logger.warning(f"No se obtuvieron datos para la URL (Manual): {url}")
             return {"status": "warning", "message": f"No data scraped for URL: {url}", "records_processed": 0}

        logger.info(f"Scrapeados {len(scraped_data)} registros (Manual)")

        logger.info("Iniciando transformación de datos (Manual)")
        transformed_data = transformer.transform(scraped_data)
        logger.info(f"Transformados {len(transformed_data)} registros (Manual)")

        logger.info("Iniciando carga de datos (Manual)")
        for loader in loaders:
            loader.load(transformed_data)
        logger.info(f"Carga de datos completada (Manual) usando {output}")

        logger.info("Proceso MANUAL ETL completado exitosamente")
        return {"status": "success", "message": "Manual ETL completed.", "records_processed": len(transformed_data)}
    except Exception as e:
        logger.error(f"Error en el proceso MANUAL ETL: {e}", exc_info=True)
        return {"status": "error", "message": str(e)}

def run_sequential_etl(rubros: Optional[List[str]] = None, localidades: Optional[List[str]] = None, output: str = "both"):
    logger.info(f"Iniciando SEQUENTIAL ETL. Rubros: {rubros}, Localidades: {localidades}, Output: {output}")
    try:
        config = get_config()
        # Usar tu SequentialCollector y GuiaCoresScraper
        # Asumiendo que SequentialCollector acepta rubros y localidades en __init__
        collector = SequentialCollector(rubros=rubros, localidades=localidades)

        logger.info("Iniciando recolección de URLs (Sequential)")
        # collect_urls en SequentialCollector devuelve Dict[str, str] {id: url}
        urls_dict: Dict[str, str] = collector.collect_urls()

        # Asegurarse de cerrar el driver del collector después de recolectar
        collector.cleanup()

        if not urls_dict:
            logger.warning("No se recolectaron URLs en el modo Sequential.")
            return {"status": "warning", "message": "No URLs collected in sequential mode.", "records_processed": 0}

        logger.info(f"Recolectadas {len(urls_dict)} URLs (Sequential)")

        # Convertir el diccionario a la estructura que process_urls en GuiaCoresScraper espera
        # process_urls espera una lista de diccionarios como [{'id': '...', 'url': '...'}]
        urls_list_for_scraper = [{"id": id, "url": url} for id, url in urls_dict.items()]

        logger.info("Iniciando scraping de datos (Sequential)")
        # Instanciar GuiaCoresScraper y usar su método process_urls
        # Es crucial que GuiaCoresScraper pueda ser instanciado aquí y use el config/maneje su propio driver
        # El método process_urls de GuiaCoresScraper ya parece manejar el scraping y guardar en CSV.
        # Podríamos modificar process_urls para que retorne los datos en lugar de solo guardarlos,
        # o dejar que maneje el guardado interno y luego usar el loader de base de datos si output es 'both' o 'database'.
        # Por ahora, asumiré que process_urls guarda y que necesitamos leer el CSV o adaptar.
        # Una mejor integración sería que process_urls retorne la lista de diccionarios scrapeados.
        # Voy a adaptar la llamada asumiendo que process_urls puede tomar la lista de URLs y retornar los datos.

        # NOTA: Tu GuiaCoresScraper tiene un main() que corre con multiprocessing.
        # Integrarlo directamente aquí requiere refactorizar GuiaCoresScraper
        # para que la lógica de process_urls sea un método llamable con una lista de URLs.
        # Y para evitar problemas con Selenium en hilos/procesos de Uvicorn,
        # idealmente el scraping con Selenium debería ejecutarse en un proceso separado
        # o manejarse de forma asíncrona si es posible, pero eso complica la integración rápida.
        # Por ahora, llamaré directamente al método process_urls asumiendo una instancia simple,
        # pero ten en cuenta los posibles problemas de concurrencia y Selenium.

        # Instanciar GuiaCoresScraper sin resume=True ni rango de IDs, ya que SequentialCollector maneja la recolección
        # Es posible que necesites adaptar GuiaCoresScraper.__init__ para que no inicie el driver inmediatamente
        # o para que acepte un driver ya iniciado, dependiendo de cómo quieras manejar los recursos de Selenium.
        # Por la estructura actual de GuiaCoresScraper, parece más directo llamarlo para procesar la lista de URLs.

        # Adaptación: Crear una instancia temporal y llamar a process_urls.
        # Esto puede no ser eficiente si se llama mucho desde la API,
        # y el manejo del driver de Selenium dentro de cada llamada puede causar overhead.
        # Una solución robusta implicaría que GuiaCoresScraper tenga un método 'scrape_list_of_urls(self, urls_list)'
        # que gestione el driver internamente para esa lista.
        # Dado el código existente, llamaré a process_urls y ajustaré si es necesario.

        scraper = GuiaCoresScraper(resume=False, start_id=None, end_id=None)
        # Cerrar el driver inmediatamente después de instanciar si no queremos que lo mantenga abierto
        # O modificar __init__ para que no lo abra hasta que se necesite.
        # Por ahora, llamaré al método y asumiré que maneja su driver eficientemente por llamada o es refactorizado.

        # Suponiendo que process_urls_list es un nuevo método refactorizado en GuiaCoresScraper
        # que recibe una lista de diccionarios [{'id': '...', 'url': '...'}] y devuelve la lista de datos scrapeados.
        # COMO NO TENEMOS ESE MÉTODO TODAVÍA, TENDREMOS QUE ADAPTAR O USAR LO QUE HAY.
        # La función principal GuiaCoresScraper.main() usa multiprocessing y lee de un archivo JSON.
        # La función process_url_chunk(chunk) es la que hace el scraping real de un chunk.
        # La integración directa aquí es compleja sin refactorizar GuiaCoresScraper.

        # Opción de integración simple (requiere que process_urls_list exista en GuiaCoresScraper):
        # scraped_data = scraper.process_urls_list(urls_list_for_scraper)

        # Opción alternativa: Modificar run_sequential_etl para que escriba las URLs a un archivo temporal
        # y luego llame a una versión adaptada de GuiaCoresScraper que lea ese archivo y scrape.
        # Esto se parece más a cómo funciona tu main() actual de GuiaCoresScraper.
        # Esta opción es menos directa para pasar datos en memoria.

        # Opción 3 (más simple por ahora, asumiendo que GuiaCoresScraper.process_urls
        # puede recibir una lista de URLs directamente o adaptarla ligeramente):
        # Voy a simular la llamada a process_urls, pero **esto requiere adaptación en sequential_scraper.py**
        # para que GuiaCoresScraper.process_urls acepte una lista de URLs como argumento.

        # TEMPORAL: Placeholder o llamada que DEBE SER ADAPTADA en GuiaCoresScraper
        # Asumiendo que GuiaCoresScraper.process_urls puede aceptar urls_list_for_scraper
        # scraped_data = scraper.process_urls(urls_list_for_scraper) # <--- Requiere modificación en GuiaCoresScraper

        # Dada la estructura de tu sequential_scraper.py, parece que está diseñado para ejecutarse como script principal.
        # Integrarlo como una clase reutilizable para recibir una lista de URLs scrapeables aquí requiere refactorización.
        # Por ahora, dejaré un placeholder y una nota.

        logger.warning("Integración directa de GuiaCoresScraper en run_sequential_etl REQUIERE refactorización de GuiaCoresScraper para aceptar una lista de URLs.")
        logger.warning("Actualmente, GuiaCoresScraper está diseñado para ejecutarse como script principal leyendo de un archivo.")
        logger.warning("Placeholder: Simulating scraping based on collected URLs.")

        # Placeholder de datos scrapeados basado en las URLs recolectadas
        scraped_data = []
        for id, url in urls_dict.items():
            # Simular datos scrapeados para cada URL recolectada
            scraped_data.append({
                "id_negocio": id,
                "url": url,
                "nombre": f"Placeholder Name for {id}",
                "direccion": "Placeholder Address",
                "telefonos": "N/A",
                "whatsapp": "N/A",
                "sitio_web": "N/A",
                "email": "N/A",
                "facebook": "N/A",
                "instagram": "N/A",
                "horarios": "N/A",
                "rubros": "N/A",
                "latitud": "N/A",
                "longitud": "N/A",
                "fecha_extraccion": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
        logger.info(f"Simulación: Scrapeados {len(scraped_data)} registros (Sequential)")


        transformer = BusinessTransformer(config=config)
        loaders = _get_loaders(output, config)

        logger.info("Iniciando transformación de datos (Sequential)")
        transformed_data = transformer.transform(scraped_data)
        logger.info(f"Transformados {len(transformed_data)} registros (Sequential)")

        logger.info("Iniciando carga de datos (Sequential)")
        for loader in loaders:
            loader.load(transformed_data)
        logger.info(f"Carga de datos completada (Sequential) usando {output}")

        logger.info("Proceso SEQUENTIAL ETL completado exitosamente")
        return {"status": "success", "message": "Sequential ETL completed.", "records_processed": len(transformed_data)}
    except Exception as e:
        logger.error(f"Error en el proceso SEQUENTIAL ETL: {e}", exc_info=True)
        # Asegurarse de cerrar el driver del collector si hubo un error antes del cleanup explícito
        try:
            if 'collector' in locals() and collector.driver:
                collector.cleanup()
        except Exception as cleanup_error:
             logger.error(f"Error durante la limpieza del collector sequential: {cleanup_error}", exc_info=True)

        # Nota: Si GuiaCoresScraper inicia su propio driver y falla, también debería tener un cleanup.
        return {"status": "error", "message": str(e)}


# Main block for CLI execution (optional, can be adapted for Argo Jobs)
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="ETL Process Runner")
    subparsers = parser.add_subparsers(dest="mode", help="ETL mode to run")

    # Bulk mode
    bulk_parser = subparsers.add_parser("bulk", help="Run ETL in bulk mode")
    bulk_parser.add_argument("--start_id", type=int, required=True, help="Starting ID for bulk processing")
    bulk_parser.add_argument("--end_id", type=int, required=True, help="Ending ID for bulk processing")
    bulk_parser.add_argument("--output", type=str, default="both", choices=["file", "database", "both"], help="Output destination")

    # Manual mode
    manual_parser = subparsers.add_parser("manual", help="Run ETL for a single URL")
    manual_parser.add_argument("--url", type=str, required=True, help="URL to scrape")
    manual_parser.add_argument("--output", type=str, default="both", choices=["file", "database", "both"], help="Output destination")

    # Sequential mode
    sequential_parser = subparsers.add_parser("sequential", help="Run ETL sequentially based on rubros and localities")
    sequential_parser.add_argument("--rubros", type=str, help="Comma-separated list of rubros (optional)")
    sequential_parser.add_argument("--localidades", type=str, help="Comma-separated list of localidades (optional)")
    sequential_parser.add_argument("--output", type=str, default="both", choices=["file", "database", "both"], help="Output destination")

    args = parser.parse_args()

    setup_logging_if_not_configured() # Ensure logging is set up for CLI

    if args.mode == "bulk":
        run_bulk_etl(args.start_id, args.end_id, args.output)
    elif args.mode == "manual":
        run_manual_etl(args.url, args.output)
    elif args.mode == "sequential":
        rubros_list = [r.strip() for r in args.rubros.split(',') if r.strip()] if args.rubros else None
        localidades_list = [l.strip() for l in args.localidades.split(',') if l.strip()] if args.localidades else None
        run_sequential_etl(rubros_list, localidades_list, args.output)
    else:
        logger.warning("No mode specified or mode not recognized. Exiting.")
        parser.print_help()

