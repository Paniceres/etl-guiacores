import os
from pathlib import Path
from typing import Dict, Any
import yaml

# Directorios base
BASE_DIR = Path(__file__).parent.parent.parent
DATA_DIR = BASE_DIR / 'data'
LOGS_DIR = BASE_DIR / 'logs'

# Crear directorios si no existen
DATA_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)

# Configuración de la base de datos
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', '5432')),
    'database': os.getenv('DB_NAME', 'guiacores'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'postgres')
}

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
    'search_url': 'https://www.guiacores.com.ar/index.php?r=search%2Findex',
    'detail_url': 'https://www.guiacores.com.ar/index.php?r=search/detail',
    'timeout': 10,
    'retry_attempts': 3,
    'retry_delay': 5
}

# Configuración de los extractores
EXTRACTOR_CONFIG = {
    'manual': {
        'html_dir': str(DATA_DIR / 'html'),
        'max_file_size': 10 * 1024 * 1024,  # 10MB
        'default_path': 'data/html_samples'
    },
    'sequential': {
        'max_pages': 10,
        'click_delay': 2,
        'load_timeout': 10,
        'script_timeout': 30,
        'implicit_wait': 10,
        'button_selector': '.load-more',
        'business_selector': '.business-item',
        'search_url': 'https://www.guiacores.com.ar/buscar.php',
        'retry': {
            'max_attempts': 3,
            'delay': 5,
            'backoff_factor': 2
        },
        'proxy': {
            'enabled': False,
            'http': None,
            'https': None,
            'no_proxy': 'localhost,127.0.0.1'
        },
        'browser': {
            'headless': True,
            'disable_gpu': True,
            'disable_dev_shm_usage': True,
            'no_sandbox': True,
            'window_size': '1920,1080',
            'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
    },
    'bulk': {
        'min_id': 1,
        'max_id': 99999,
        'chunk_size': 100,
        'max_workers': 4,
        'timeout': 10,
        'detail_url': 'https://www.guiacores.com.ar/detalle.php'
    }
}

# Configuración de los transformadores
TRANSFORMER_CONFIG = {
    'url': {
        'max_length': 255,
        'allowed_schemes': ['http', 'https'],
        'allowed_domains': ['guiacores.com.ar']
    },
    'business': {
        'required_fields': ['name', 'url'],
        'optional_fields': ['address', 'phone', 'description', 'rubro', 'localidad']
    }
}

# Configuración de los cargadores
LOADER_CONFIG = {
    'database': {
        'batch_size': 1000,
        'max_retries': 3,
        'retry_delay': 5
    },
    'file': {
        'output_dir': str(DATA_DIR / 'processed'),
        'format': 'json',
        'compression': False
    },
    'cache': {
        'enabled': True,
        'ttl': 3600,  # 1 hora
        'max_size': 1000
    }
}

# Configuración de las tablas
TABLE_CONFIG = {
    'urls': {
        'name': 'urls',
        'columns': [
            ('id', 'SERIAL PRIMARY KEY'),
            ('url', 'VARCHAR(255) UNIQUE NOT NULL'),
            ('business_id', 'VARCHAR(50)'),
            ('source', 'VARCHAR(50)'),
            ('created_at', 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'),
            ('updated_at', 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP')
        ]
    },
    'businesses': {
        'name': 'businesses',
        'columns': [
            ('id', 'VARCHAR(50) PRIMARY KEY'),
            ('name', 'VARCHAR(255) NOT NULL'),
            ('url', 'VARCHAR(255) UNIQUE NOT NULL'),
            ('address', 'VARCHAR(255)'),
            ('phone', 'VARCHAR(50)'),
            ('description', 'TEXT'),
            ('rubro', 'VARCHAR(100)'),
            ('localidad', 'VARCHAR(100)'),
            ('created_at', 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'),
            ('updated_at', 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP')
        ]
    }
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
        'db': DB_CONFIG,
        'log': LOG_CONFIG,
        'app': APP_CONFIG,
        'extractor': EXTRACTOR_CONFIG,
        'transformer': TRANSFORMER_CONFIG,
        'loader': LOADER_CONFIG,
        'tables': TABLE_CONFIG
    } 