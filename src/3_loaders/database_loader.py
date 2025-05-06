from typing import List, Dict, Any
import psycopg2
from psycopg2.extras import execute_values
from ..common.base import BaseLoader
from ..common.config import get_config
from ..common.db import DatabaseConnection

class DatabaseLoader(BaseLoader):
    """Cargador para base de datos PostgreSQL"""
    
    def __init__(self):
        super().__init__('database_loader')
        self.config = get_config()['loader']['database']
        self.db = DatabaseConnection()
        
    def load(self, data: List[Dict[str, Any]]) -> None:
        """
        Carga los datos en la base de datos
        
        Args:
            data: Lista de diccionarios con datos
        """
        if not data:
            return
            
        # Conectar a la base de datos
        self.db.connect()
        
        try:
            # Preparar datos
            columns = list(data[0].keys())
            values = [[item[col] for col in columns] for item in data]
            
            # Construir query
            table = 'businesses'
            query = f"""
                INSERT INTO {table} ({', '.join(columns)})
                VALUES %s
                ON CONFLICT (url) DO UPDATE
                SET {', '.join(f"{col} = EXCLUDED.{col}" for col in columns if col != 'url')}
            """
            
            # Ejecutar query
            with self.db.connection.cursor() as cursor:
                execute_values(cursor, query, values)
                
            # Commit
            self.db.connection.commit()
            
        except Exception as e:
            self.db.connection.rollback()
            raise e
            
        finally:
            self.db.disconnect()
            
    def __del__(self):
        """Destructor"""
        if hasattr(self, 'db'):
            self.db.disconnect() 