# src/loaders/run_loading.py

import argparse
import logging
import json
import sys
import os
from typing import Dict, Any, List # Importar List

# Asegurar que el directorio raíz del proyecto esté en el PATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.common.config import get_config
# Importar los loaders necesarios (ej: DatabaseLoader, FileLoader)
from src.loaders.database_loader import DatabaseLoader
from src.loaders.file_loader import FileLoader

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def _get_loaders(output_type: str, config: dict) -> List[Any]:
    """Instancia y devuelve una lista de objetos 'loader' basados en el output_type.

    Esta función se replica de main.py para que este script sea standalone.

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

def run_loading(
    input_path: str = './data/transformed/sequential_transformed_data.json',
    output_type: str = "both" # choices: "file", "database", "both"
) -> Dict[str, Any]:
    """
    Ejecuta la fase de carga.
    Lee datos de input_path y los carga a los destinos especificados por output_type.
    """
    logger.info(f"Iniciando fase de Carga. Input: {input_path}, Output Type: {output_type}")
    transformed_data = [] # Inicializar transformed_data
    try:
        config = get_config()
        loaders = _get_loaders(output_type, config) # Usar la función _get_loaders replicada

        # Leer datos transformados del archivo de entrada
        logger.info(f"Leyendo datos de: {input_path}")
        if not os.path.exists(input_path):
            logger.error(f"Archivo de entrada no encontrado: {input_path}")
            return {"status": "error", "message": f"Archivo de entrada no encontrado: {input_path}"}

        try:
            with open(input_path, 'r', encoding='utf-8') as f:
                transformed_data = json.load(f)
            logger.info(f"Leídos {len(transformed_data)} registros para cargar.")
        except json.JSONDecodeError:
            logger.error(f"Error decodificando JSON del archivo: {input_path}. Asegúrese de que sea un JSON válido.", exc_info=True)
            return {"status": "error", "message": f"Error decodificando JSON del archivo: {input_path}"}
        except Exception as file_read_error:
             logger.error(f"Error al leer el archivo de entrada {input_path}: {file_read_error}", exc_info=True)
             return {"status": "error", "message": f"Error al leer el archivo de entrada: {file_read_error}"}

        if not transformed_data:
            logger.warning("Archivo de entrada vacío o sin datos. No hay nada que cargar.")
            return {"status": "warning", "message": "No hay datos para cargar.", "records_processed": 0}

        # --- Lógica de Carga Real ---
        logger.info("Cargando datos...")
        for loader in loaders:
            loader.load(transformed_data)
        logger.info(f"Carga de datos completada usando {output_type}.")
        # --- Fin Lógica de Carga Real ---

        logger.info("Fase de Carga completada exitosamente.")
        return {"status": "success", "message": f"Carga completada a {output_type}.", "records_processed": len(transformed_data)}

    except Exception as e:
        logger.error(f"Error inesperado en la fase de Carga: {e}", exc_info=True)
        return {"status": "error", "message": f"Error inesperado durante la carga: {str(e)}"}

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ejecuta la fase de Carga del ETL.")
    parser.add_argument("--input_path", type=str, default='./data/transformed/sequential_transformed_data.json', help="Ruta al archivo de entrada con datos transformados.")
    parser.add_argument("--output_type", type=str, default="both", choices=["file", "database", "both"], help="Destino de salida (file, database, o both).")

    args = parser.parse_args()

    result = run_loading(args.input_path, args.output_type)

    if result.get("status") == "error":
        logger.error(f"La carga falló: {result.get('message')}")
        sys.exit(1)
    elif result.get("status") == "warning":
         logger.warning(f"Carga completada con advertencias: {result.get('message')}")
         sys.exit(0) # Considerar sys.exit(1) si un warning en carga es crítico
    else:
        logger.info(f"Carga completada exitosamente. Registros procesados: {result.get('records_processed')}")
        sys.exit(0)
