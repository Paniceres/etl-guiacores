import logging
import logging.config
import json
from pathlib import Path
from datetime import datetime
from .config import COLLECTOR_LOGS, SCRAPER_LOGS, CLEANER_LOGS, LOG_CONFIG
from typing import Optional

def setup_logging(name: str, component: str) -> logging.Logger:
    """
    Configura el logging para un componente
    
    Args:
        name (str): Nombre del logger
        component (str): Componente (collector, extractor, etc)
        
    Returns:
        logging.Logger: Logger configurado
    """
    # Configurar logging
    logging.config.dictConfig(LOG_CONFIG)
    
    # Crear logger
    logger = logging.getLogger(f"{component}.{name}")
    
    return logger

def save_json(data, filename, directory):
    """Guarda datos en formato JSON"""
    filepath = Path(directory) / filename
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    return filepath

def load_json(filename, directory):
    """Carga datos desde un archivo JSON"""
    filepath = Path(directory) / filename
    if not filepath.exists():
        return None
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)

def generate_filename(prefix, extension='json'):
    """Genera un nombre de archivo con timestamp"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    return f"{prefix}_{timestamp}.{extension}"

def check_duplicates(items, key_func):
    """Verifica duplicados en una lista de items usando una función key"""
    seen = set()
    duplicates = []
    unique_items = []
    
    for item in items:
        key = key_func(item)
        if key in seen:
            duplicates.append(item)
        else:
            seen.add(key)
            unique_items.append(item)
    
    return unique_items, duplicates

def retry_on_error(max_attempts: int = 3, delay: int = 5):
    """
    Decorador para reintentar operaciones en caso de error
    
    Args:
        max_attempts (int): Número máximo de intentos
        delay (int): Tiempo de espera entre intentos en segundos
        
    Returns:
        function: Función decorada
    """
    import time
    from functools import wraps
    
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            while attempts < max_attempts:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempts += 1
                    if attempts == max_attempts:
                        raise e
                    time.sleep(delay)
            return None
        return wrapper
    return decorator

def clean_text(text: str) -> Optional[str]:
    """
    Limpia y normaliza un texto
    
    Args:
        text (str): Texto a limpiar
        
    Returns:
        str: Texto limpio
    """
    if not text:
        return None
        
    # Eliminar espacios extras
    text = ' '.join(text.split())
    
    # Eliminar caracteres especiales
    text = text.strip()
    
    return text

def extract_id_from_url(url: str) -> Optional[str]:
    """
    Extrae el ID de un negocio de su URL
    
    Args:
        url (str): URL del negocio
        
    Returns:
        str: ID del negocio
    """
    if not url:
        return None
        
    try:
        # Buscar parámetro id en la URL
        if 'id=' in url:
            return url.split('id=')[1].split('&')[0]
        return None
    except:
        return None

def format_phone(phone: str) -> Optional[str]:
    """
    Formatea un número de teléfono
    
    Args:
        phone (str): Número de teléfono
        
    Returns:
        str: Número formateado
    """
    if not phone:
        return None
        
    # Extraer solo números
    numbers = ''.join(filter(str.isdigit, phone))
    
    if not numbers:
        return None
        
    # Formatear según el largo
    if len(numbers) == 10:
        return f"{numbers[:3]}-{numbers[3:6]}-{numbers[6:]}"
    elif len(numbers) == 7:
        return f"{numbers[:3]}-{numbers[3:]}"
    else:
        return numbers 