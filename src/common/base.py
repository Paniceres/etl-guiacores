from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from pathlib import Path
import logging
from .utils import setup_logging
from .db import DatabaseConnection

class BaseCollector(ABC):
    """Clase base para todos los colectores de URLs"""
    
    def __init__(self, logger_name: str = None):
        self.logger = setup_logging(logger_name or self.__class__.__name__, 'collector')
        self.db = None
        
    def connect_db(self) -> bool:
        """Intenta conectar a la base de datos"""
        self.db = DatabaseConnection()
        return self.db.connect()
        
    @abstractmethod
    def collect_urls(self) -> List[str]:
        """Recolecta URLs de una fuente"""
        pass
        
    @abstractmethod
    def save_urls(self, urls: List[str]) -> bool:
        """Guarda las URLs recolectadas"""
        pass

class BaseExtractor(ABC):
    """Clase base para extractores"""
    
    def __init__(self, name: str):
        self.name = name
        
    @abstractmethod
    def extract(self) -> List[Dict[str, Any]]:
        """
        Extrae datos de la fuente
        
        Returns:
            Lista de diccionarios con datos extraídos
        """
        pass

class BaseTransformer(ABC):
    """Clase base para transformadores"""
    
    def __init__(self, name: str):
        self.name = name
        
    @abstractmethod
    def transform(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transforma los datos
        
        Args:
            data: Lista de diccionarios con datos
            
        Returns:
            Lista de diccionarios con datos transformados
        """
        pass

class BaseLoader(ABC):
    """Clase base para cargadores"""
    
    def __init__(self, name: str):
        self.name = name
        
    @abstractmethod
    def load(self, data: List[Dict[str, Any]]) -> None:
        """
        Carga los datos en el destino
        
        Args:
            data: Lista de diccionarios con datos
        """
        pass 