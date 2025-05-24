import os
from pathlib import Path
from typing import Dict, Any
import yaml
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Directorios base
BASE_DIR = Path(__file__).parent.parent.parent
SRC_DIR = BASE_DIR / 'src'
DATA_DIR = BASE_DIR / 'data'
LOGS_DIR = DATA_DIR / 'logs'

# Crear directorios si no existen
DATA_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)

# Configuración de logs
COLLECTOR_LOGS = str(LOGS_DIR / 'collector' / 'collector_guiaCores_bulk.log')
SCRAPER_LOGS = str(LOGS_DIR / 'scraper' / 'scraper_guiaCores_bulk.log')
CLEANER_LOGS = str(LOGS_DIR / 'cleaner' / 'cleaner_guiaCores_bulk.log')

# Crear directorios de logs si no existen
for log_dir in [LOGS_DIR / 'collector', LOGS_DIR / 'scraper', LOGS_DIR / 'cleaner']:
    log_dir.mkdir(exist_ok=True)

# Configuración de logging
LOG_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'INFO',
            'formatter': 'standard',
            'stream': 'ext://sys.stdout'
        },
        'file': {
            'class': 'logging.FileHandler',
            'level': 'DEBUG',
            'formatter': 'standard',
            'filename': str(LOGS_DIR / 'etl.log'),
            'mode': 'a'
        }
    },
    'loggers': {
        '': {
            'handlers': ['console', 'file'],
            'level': 'INFO',
            'propagate': True
        }
    }
}

# Configuración de la aplicación
APP_CONFIG = {
    'base_url': 'https://www.guiacores.com.ar',
    'timeout': 30,
    'retry_attempts': 3,
    'retry_delay': 5
}

# Configuración del extractor
EXTRACTOR_CONFIG = {
    'bulk': {
        'start_id': 1,
        'end_id': 99999,
        'chunk_size': 100,
        'max_workers': 4,
        'timeout': 30,
        'base_url': 'https://www.guiacores.com.ar/index.php?r=search/detail&id='
    }
}

# Configuración del transformador
TRANSFORMER_CONFIG = {
    'clean_text': True,
    'normalize_phones': True,
    'validate_emails': True,
    'validate_urls': True
}

# Configuración del loader
LOADER_CONFIG = {
    'batch_size': 1000,
    'max_retries': 3,
    'retry_delay': 5
}

def get_config() -> Dict[str, Any]:
    """
    Obtiene la configuración del proyecto
    
    Returns:
        dict: Configuración
    """
    return {
        'base_dir': str(BASE_DIR),
        'data_dir': str(DATA_DIR),
        'logs_dir': str(LOGS_DIR),
        'log': LOG_CONFIG,
        'app': APP_CONFIG,
        'extractor': EXTRACTOR_CONFIG,
        'transformer': TRANSFORMER_CONFIG,
        'loader': LOADER_CONFIG,
    }