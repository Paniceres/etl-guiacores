import logging
import psycopg2
from typing import List, Dict, Any
from ..common.config import get_config
from ..common.db import DatabaseConnection

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data/logs/loader/database_loader.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DatabaseLoader:
    """Cargador para guardar datos en la base de datos"""
    
    def __init__(self):
        self.config = get_config()
        self.loader_config = self.config['loader']
        self.db = DatabaseConnection()
        
    def _create_tables(self):
        """Crea las tablas necesarias si no existen"""
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    # Tabla de negocios
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS negocios (
                            id SERIAL PRIMARY KEY,
                            id_negocio VARCHAR(50) UNIQUE,
                            nombre VARCHAR(255),
                            direccion TEXT,
                            telefonos TEXT,
                            whatsapp VARCHAR(50),
                            sitio_web VARCHAR(255),
                            email VARCHAR(255),
                            facebook VARCHAR(255),
                            instagram VARCHAR(255),
                            horarios TEXT,
                            rubros TEXT,
                            descripcion TEXT,
                            servicios TEXT,
                            latitud VARCHAR(50),
                            longitud VARCHAR(50),
                            url VARCHAR(255),
                            fecha_extraccion TIMESTAMP,
                            fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                    
                    # Índices
                    cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_negocios_id_negocio ON negocios(id_negocio);
                        CREATE INDEX IF NOT EXISTS idx_negocios_nombre ON negocios(nombre);
                        CREATE INDEX IF NOT EXISTS idx_negocios_rubros ON negocios(rubros);
                    """)
                    
            logger.info("Tablas creadas exitosamente")
            return True
            
        except Exception as e:
            logger.error(f"Error al crear tablas: {e}")
            return False
            
    def _upsert_business(self, cur, business: Dict[str, Any]):
        """Inserta o actualiza un negocio en la base de datos"""
        try:
            cur.execute("""
                INSERT INTO negocios (
                    id_negocio, nombre, direccion, telefonos, whatsapp,
                    sitio_web, email, facebook, instagram, horarios,
                    rubros, descripcion, servicios, latitud, longitud,
                    url, fecha_extraccion
                ) VALUES (
                    %(id_negocio)s, %(nombre)s, %(direccion)s, %(telefonos)s,
                    %(whatsapp)s, %(sitio_web)s, %(email)s, %(facebook)s,
                    %(instagram)s, %(horarios)s, %(rubros)s, %(descripcion)s,
                    %(servicios)s, %(latitud)s, %(longitud)s, %(url)s,
                    %(fecha_extraccion)s
                )
                ON CONFLICT (id_negocio) DO UPDATE SET
                    nombre = EXCLUDED.nombre,
                    direccion = EXCLUDED.direccion,
                    telefonos = EXCLUDED.telefonos,
                    whatsapp = EXCLUDED.whatsapp,
                    sitio_web = EXCLUDED.sitio_web,
                    email = EXCLUDED.email,
                    facebook = EXCLUDED.facebook,
                    instagram = EXCLUDED.instagram,
                    horarios = EXCLUDED.horarios,
                    rubros = EXCLUDED.rubros,
                    descripcion = EXCLUDED.descripcion,
                    servicios = EXCLUDED.servicios,
                    latitud = EXCLUDED.latitud,
                    longitud = EXCLUDED.longitud,
                    url = EXCLUDED.url,
                    fecha_extraccion = EXCLUDED.fecha_extraccion,
                    fecha_actualizacion = CURRENT_TIMESTAMP
            """, business)
            
        except Exception as e:
            logger.error(f"Error al upsert negocio {business.get('id_negocio')}: {e}")
            raise
            
    def load(self, data: List[Dict[str, Any]]) -> bool:
        """
        Carga los datos en la base de datos
        
        Args:
            data (List[Dict[str, Any]]): Lista de diccionarios con datos de negocios
            
        Returns:
            bool: True si la carga fue exitosa
        """
        if not data:
            logger.warning("No hay datos para cargar")
            return False
            
        try:
            # Conectar a la base de datos
            if not self.db.connect():
                raise Exception("No se pudo conectar a la base de datos")
                
            # Crear tablas si no existen
            if not self._create_tables():
                raise Exception("No se pudieron crear las tablas")
                
            # Cargar datos en lotes
            batch_size = self.loader_config['batch_size']
            total_loaded = 0
            
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    for i in range(0, len(data), batch_size):
                        batch = data[i:i + batch_size]
                        for business in batch:
                            self._upsert_business(cur, business)
                        total_loaded += len(batch)
                        logger.info(f"Cargados {total_loaded} de {len(data)} registros")
                        
            logger.info(f"Carga completada: {total_loaded} registros")
            return True
            
        except Exception as e:
            logger.error(f"Error en la carga de datos: {e}")
            return False
            
        finally:
            self.db.disconnect() 