import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Optional, List, Dict, Any
from .config import DB_CONFIG
from .utils import setup_logging

class DatabaseConnection:
    """Manejador de conexión a PostgreSQL"""
    
    def __init__(self):
        self.logger = setup_logging('db', 'common')
        self.conn = None
        self.config = DB_CONFIG
        
    def connect(self) -> bool:
        """
        Establece conexión con la base de datos
        
        Returns:
            bool: True si la conexión fue exitosa
        """
        try:
            self.conn = psycopg2.connect(
                host=self.config['host'],
                port=self.config['port'],
                database=self.config['database'],
                user=self.config['user'],
                password=self.config['password'],
                cursor_factory=RealDictCursor
            )
            self.logger.info("Conexión exitosa a la base de datos")
            return True
            
        except Exception as e:
            self.logger.error(f"Error al conectar a la base de datos: {e}")
            return False
            
    def disconnect(self) -> None:
        """Cierra la conexión a la base de datos"""
        if self.conn:
            self.conn.close()
            self.conn = None
            self.logger.info("Conexión cerrada")
            
    def execute_query(self, query: str, params: tuple = None) -> Optional[List[Dict[str, Any]]]:
        """
        Ejecuta una consulta SQL
        
        Args:
            query (str): Consulta SQL
            params (tuple): Parámetros de la consulta
            
        Returns:
            list: Resultados de la consulta
        """
        if not self.conn:
            if not self.connect():
                return None
                
        try:
            with self.conn.cursor() as cur:
                cur.execute(query, params)
                if cur.description:
                    return cur.fetchall()
                self.conn.commit()
                return None
                
        except Exception as e:
            self.logger.error(f"Error al ejecutar consulta: {e}")
            if self.conn:
                self.conn.rollback()
            return None
            
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