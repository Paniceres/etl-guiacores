import logging
import psycopg2
from psycopg2 import pool
from typing import Optional, List, Dict, Any
from .config import get_config
from .utils import setup_logging

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data/logs/db/database.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DatabaseConnection:
    """Manejador de conexión a la base de datos PostgreSQL"""
    
    _connection_pool = None
    
    def __init__(self):
        self.logger = setup_logging('db', 'common')
        self.config = get_config()
        self.db_config = self.config['db']
        self.connection = None
        
    def connect(self) -> bool:
        """
        Establece conexión con la base de datos
        
        Returns:
            bool: True si la conexión fue exitosa
        """
        try:
            if DatabaseConnection._connection_pool is None:
                DatabaseConnection._connection_pool = pool.SimpleConnectionPool(
                    1,  # minconn
                    10,  # maxconn
                    host=self.db_config['host'],
                    port=self.db_config['port'],
                    database=self.db_config['database'],
                    user=self.db_config['user'],
                    password=self.db_config['password']
                )
                
            self.connection = DatabaseConnection._connection_pool.getconn()
            self.logger.info("Conexión a base de datos establecida")
            return True
            
        except Exception as e:
            self.logger.error(f"Error al conectar a la base de datos: {e}")
            return False
            
    def disconnect(self) -> None:
        """Cierra la conexión a la base de datos"""
        if self.connection:
            try:
                DatabaseConnection._connection_pool.putconn(self.connection)
                self.connection = None
                self.logger.info("Conexión a base de datos cerrada")
            except Exception as e:
                self.logger.error(f"Error al cerrar conexión: {e}")
                
    def get_connection(self):
        """Retorna la conexión actual"""
        if not self.connection:
            raise Exception("No hay conexión activa")
        return self.connection
        
    def execute_query(self, query: str, params: Optional[tuple] = None) -> Optional[list]:
        """
        Ejecuta una consulta SQL
        
        Args:
            query (str): Consulta SQL a ejecutar
            params (tuple, optional): Parámetros para la consulta
            
        Returns:
            Optional[list]: Resultados de la consulta o None si hay error
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, params)
                    if cur.description:
                        return cur.fetchall()
                    conn.commit()
                    return None
                    
        except Exception as e:
            self.logger.error(f"Error al ejecutar consulta: {e}")
            return None
            
    def execute_many(self, query: str, params_list: list) -> bool:
        """
        Ejecuta una consulta SQL múltiples veces
        
        Args:
            query (str): Consulta SQL a ejecutar
            params_list (list): Lista de parámetros para la consulta
            
        Returns:
            bool: True si la ejecución fue exitosa
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.executemany(query, params_list)
                    conn.commit()
                    return True
                    
        except Exception as e:
            self.logger.error(f"Error al ejecutar consulta múltiple: {e}")
            return False
            
    def create_tables(self) -> bool:
        """
        Crea las tablas necesarias
        
        Returns:
            bool: True si las tablas se crearon correctamente
        """
        from .config import TABLE_CONFIG
        
        try:
            # Crear tabla de URLs
            urls_table = TABLE_CONFIG['urls']
            urls_columns = ', '.join(f"{col[0]} {col[1]}" for col in urls_table['columns'])
            urls_query = f"""
                CREATE TABLE IF NOT EXISTS {urls_table['name']} (
                    {urls_columns}
                )
            """
            
            # Crear tabla de negocios
            businesses_table = TABLE_CONFIG['businesses']
            businesses_columns = ', '.join(f"{col[0]} {col[1]}" for col in businesses_table['columns'])
            businesses_query = f"""
                CREATE TABLE IF NOT EXISTS {businesses_table['name']} (
                    {businesses_columns}
                )
            """
            
            # Ejecutar queries
            self.execute_query(urls_query)
            self.execute_query(businesses_query)
            
            self.logger.info("Tablas creadas exitosamente")
            return True
            
        except Exception as e:
            self.logger.error(f"Error al crear tablas: {e}")
            return False
            
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()

    def check_duplicate(self, table, field, value):
        """Verifica si existe un registro duplicado"""
        query = f"SELECT COUNT(*) as count FROM {table} WHERE {field} = %s"
        result = self.execute_query(query, (value,))
        return result[0]['count'] > 0 if result else False 