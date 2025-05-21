# src/transformers/run_transformation.py


import argparse
import logging
import json
import sys
import os
from typing import Dict, Any # Importar Dict y Any

# Asegurar que el directorio raíz del proyecto esté en el PATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.common.config import get_config
from src.transformers.business_transformer import BusinessTransformer

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def run_transformation(
    input_path: str = './data/extracted/sequential_raw_data.json',
    output_path: str = './data/transformed/sequential_transformed_data.json'
) -> Dict[str, Any]:
    """
    Ejecuta la fase de transformación.
    Lee datos de input_path, los transforma y guarda en output_path.
    """
    logger.info("Iniciando fase de Transformación.")
    raw_data = [] # Inicializar raw_data
    try:
        config = get_config()
        transformer = BusinessTransformer(config=config)

        # Leer datos crudos del archivo de entrada
        logger.info(f"Leyendo datos de: {input_path}")
        if not os.path.exists(input_path):
            logger.error(f"Archivo de entrada no encontrado: {input_path}")
            return {"status": "error", "message": f"Archivo de entrada no encontrado: {input_path}"}

        try:
            with open(input_path, 'r', encoding='utf-8') as f:
                raw_data = json.load(f)
            logger.info(f"Leídos {len(raw_data)} registros para transformar.")
        except json.JSONDecodeError:
            logger.error(f"Error decodificando JSON del archivo: {input_path}. Asegúrese de que sea un JSON válido.", exc_info=True)
            return {"status": "error", "message": f"Error decodificando JSON del archivo: {input_path}"}
        except Exception as file_read_error:
             logger.error(f"Error al leer el archivo de entrada {input_path}: {file_read_error}", exc_info=True)
             return {"status": "error", "message": f"Error al leer el archivo de entrada: {file_read_error}"}

        if not raw_data:
            logger.warning("Archivo de entrada vacío o sin datos. No hay nada que transformar.")
            return {"status": "warning", "message": "No hay datos para transformar.", "records_processed": 0}

        # Transformar los datos
        logger.info("Transformando datos...")
        transformed_data = transformer.transform(raw_data)
        logger.info(f"Transformados {len(transformed_data)} registros.")

        # Asegurar que el directorio de salida exista
        output_dir = os.path.dirname(output_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
            logger.info(f"Directorio de salida creado: {output_dir}")

        # Guardar datos transformados en archivo JSON
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(transformed_data, f, indent=4, ensure_ascii=False)
            logger.info(f"Datos transformados guardados en: {output_path}")
        except Exception as file_write_error:
            logger.error(f"Error al escribir el archivo de salida {output_path}: {file_write_error}", exc_info=True)
            return {"status": "error", "message": f"Error al escribir el archivo de salida: {file_write_error}"}

        logger.info("Fase de Transformación completada exitosamente.")
        return {"status": "success", "message": "Transformación completada.", "records_processed": len(transformed_data), "output_path": output_path}

    except Exception as e:
        logger.error(f"Error inesperado en la fase de Transformación: {e}", exc_info=True)
        return {"status": "error", "message": f"Error inesperado durante la transformación: {str(e)}"}

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ejecuta la fase de Transformación del ETL.")
    parser.add_argument("--input_path", type=str, default='./data/extracted/sequential_raw_data.json', help="Ruta al archivo de entrada con datos crudos.")
    parser.add_argument("--output_path", type=str, default='./data/transformed/sequential_transformed_data.json', help="Ruta al archivo de salida para los datos transformados.")

    args = parser.parse_args()

    result = run_transformation(args.input_path, args.output_path)

    if result.get("status") == "error":
        logger.error(f"La transformación falló: {result.get('message')}")
        sys.exit(1)
    elif result.get("status") == "warning":
         logger.warning(f"Transformación completada con advertencias: {result.get('message')}")
         sys.exit(0)
    else:
        logger.info(f"Transformación completada exitosamente. Registros procesados: {result.get('records_processed')}")
        logger.info(f"Datos guardados en: {result.get('output_path')}")
        sys.exit(0)
