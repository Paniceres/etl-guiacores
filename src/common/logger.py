import logging
import sys
from pathlib import Path
from typing import Optional

def setup_logger(name: str, module: str, level: str = 'INFO') -> logging.Logger:
    """
    Configura y retorna un logger con el nombre y módulo especificados.
    
    Args:
        name (str): Nombre del logger
        module (str): Nombre del módulo
        level (str): Nivel de logging (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        
    Returns:
        logging.Logger: Logger configurado
    """
    # Crear el logger
    logger = logging.getLogger(f"{name}.{module}")
    
    # Configurar el nivel
    logger.setLevel(getattr(logging, level.upper()))
    
    # Si ya tiene handlers, no agregar más
    if logger.handlers:
        return logger
    
    # Crear el formatter
    formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Handler para consola
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Handler para archivo
    log_dir = Path(__file__).parent.parent.parent / 'logs'
    log_dir.mkdir(exist_ok=True)
    
    file_handler = logging.FileHandler(
        log_dir / f"{name}.log",
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    return logger 