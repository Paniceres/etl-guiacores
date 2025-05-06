import pandas as pd
import re
import os
import psycopg2
import json
from psycopg2.extras import execute_values
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data/cleaner.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Database configuration from environment variables
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'etl_db')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'postgres')

def get_db_connection():
    """Establece conexión con la base de datos PostgreSQL"""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return conn
    except Exception as e:
        logger.error(f"Error al conectar a la base de datos: {e}")
        raise

def register_data_source(source_name, source_type="web_scraping"):
    """Registra una nueva fuente de datos y retorna su ID"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO data_sources (source_type, source_name, notes, created_at)
                VALUES (%s, %s, %s, %s)
                RETURNING source_id
            """, (
                source_type,
                source_name,
                f"Extracción automática de {source_name}",
                datetime.now()
            ))
            source_id = cur.fetchone()[0]
            conn.commit()
            return source_id
    except Exception as e:
        logger.error(f"Error al registrar la fuente de datos: {e}")
        raise
    finally:
        conn.close()

def validate_data(lead):
    """Valida los datos del lead antes de guardarlos"""
    required_fields = ['name', 'phones', 'address']
    for field in required_fields:
        if field not in lead or pd.isna(lead[field]) or str(lead[field]).strip() == '':
            lead[field] = 'N/A'
    
    # Validar formato de email
    if 'email' in lead and lead['email'] != 'N/A':
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if not re.match(email_pattern, lead['email']):
            lead['email'] = 'N/A'
    
    # Validar URLs
    url_fields = ['website', 'facebook', 'instagram']
    for field in url_fields:
        if field in lead and lead[field] != 'N/A':
            if not lead[field].startswith(('http://', 'https://')):
                lead[field] = 'N/A'
    
    return lead

def save_raw_leads_to_db(leads_data, source_id):
    """Guarda los leads crudos en la base de datos"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Preparar los datos para la inserción
            values = []
            current_time = datetime.now()
            
            for lead in leads_data:
                # Validar y limpiar datos
                lead = validate_data(lead)
                
                # Convertir timestamps a datetime si son strings
                scraped_at = lead.get('scraped_at')
                if isinstance(scraped_at, str):
                    try:
                        scraped_at = datetime.strptime(scraped_at, '%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        scraped_at = current_time
                
                values.append((
                    'web_scraping',  # source_type
                    'guiacores',     # source_name
                    json.dumps(lead), # raw_data como JSONB
                    scraped_at or current_time,  # scraped_at
                    'pending',       # etl_status
                    None,           # etl_notes
                    current_time,   # created_at
                    current_time    # updated_at
                ))

            # Insertar en raw_leads
            execute_values(cur, """
                INSERT INTO raw_leads (
                    source_type, source_name, raw_data,
                    scraped_at, etl_status, etl_notes,
                    created_at, updated_at
                ) VALUES %s
            """, values)

            conn.commit()
            logger.info(f"Guardados {len(values)} leads en la base de datos")
    except Exception as e:
        logger.error(f"Error al guardar leads crudos en la base de datos: {e}")
        raise
    finally:
        conn.close()

def clean_phone_number(phone_str):
    """
    Limpia una cadena de teléfono, dejando solo dígitos y el signo +.
    Elimina espacios en blanco y guiones comunes.
    """
    if pd.isna(phone_str) or str(phone_str).strip() == '':
        return 'N/A'
    # Convertir a string, eliminar espacios, guiones, paréntesis
    cleaned = re.sub(r'[()\s-]+', '', str(phone_str))
    # Eliminar todo lo que no sea dígito o +
    cleaned = re.sub(r'[^\d+]', '', cleaned)
    return cleaned if cleaned else 'N/A'

def normalize_capitalization(text):
    """Normaliza la capitalización de un texto (primera letra de cada palabra mayúscula, resto minúsculas)."""
    if pd.isna(text) or str(text).strip() == '' or str(text).strip() == 'N/A':
        return 'N/A'
    # Convertir a minúsculas y luego a capitalización de título
    return str(text).lower().title()

def clean_leads_csv(input_csv_path='data/guiaCores_leads.csv', output_csv_path='data/guiaCores_leads_cleaned.csv'):
    """
    Lee el CSV de leads, realiza transformaciones y limpieza, y guarda el resultado.
    """
    start_time = datetime.now()
    logger.info(f"Iniciando proceso de limpieza a las {start_time}")
    
    if not os.path.exists(input_csv_path):
        logger.error(f"Error: El archivo de entrada no se encuentra en {input_csv_path}")
        return

    try:
        df = pd.read_csv(input_csv_path)
        logger.info(f"Datos leídos exitosamente de {input_csv_path}")
    except Exception as e:
        logger.error(f"Error al leer el archivo CSV: {e}")
        return

    # Renombrar columnas para mantener consistencia
    column_mapping = {
        'name': 'Nombre',
        'phones': 'Teléfono',
        'address': 'Dirección',
        'email': 'Email',
        'website': 'Sitio Web',
        'facebook': 'Facebook',
        'instagram': 'Instagram',
        'categories': 'Rubros',
        'description': 'Descripción',
        'services': 'Servicios',
        'hours': 'Horarios',
        'latitude': 'Latitud',
        'longitude': 'Longitud'
    }
    df = df.rename(columns=column_mapping)

    # 1. Limpiar y Deduplicar Teléfonos
    df['Teléfono'] = df['Teléfono'].apply(clean_phone_number)

    # 2. Eliminar duplicados
    initial_count = len(df)
    df.drop_duplicates(inplace=True)
    logger.info(f"Duplicados eliminados: {initial_count - len(df)} registros")

    # 3. Limpieza de Redes Sociales
    GUIA_CORES_FB_PATTERN = r'https://www.facebook.com/sharer/sharer.php\?u=https://www\.guiacores\.com\.ar%2Findex\.php%3Fr%3Dsearch%2Fdetail%26id%3D\d+%26idb%3D\d+'
    GUIA_CORES_IG_PATTERN = r'https://www\.instagram\.com/guiacores/'

    def clean_social_link(link, cores_pattern):
        if pd.isna(link) or str(link).strip() == '':
            return 'N/A'
        link_str = str(link).strip()
        if re.fullmatch(cores_pattern, link_str):
            return 'N/A'
        return link_str

    df['Facebook'] = df['Facebook'].apply(lambda x: clean_social_link(x, GUIA_CORES_FB_PATTERN))
    df['Instagram'] = df['Instagram'].apply(lambda x: clean_social_link(x, GUIA_CORES_IG_PATTERN))

    # 4. Normalizar capitalización
    columns_to_normalize = ['Nombre', 'Dirección', 'Rubros', 'Descripción', 'Servicios']
    for col in columns_to_normalize:
        if col in df.columns:
            df[col] = df[col].apply(normalize_capitalization)

    # 5. Manejar timestamps
    if 'extraction_start' in df.columns:
        df['fecha_extraccion'] = df['extraction_start']
    else:
        df['fecha_extraccion'] = start_time.strftime('%Y-%m-%d %H:%M:%S')
    
    df['fecha_actualizacion'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # 6. Reordenar columnas
    final_columns_order = [
        'Nombre', 'Email', 'Teléfono', 'Sitio Web', 'Facebook', 
        'Instagram', 'Rubros', 'Dirección', 'Descripción', 'Servicios',
        'Horarios', 'Latitud', 'Longitud', 'fecha_extraccion', 
        'fecha_actualizacion'
    ]
    final_columns_order_existing = [col for col in final_columns_order if col in df.columns]
    other_columns = [col for col in df.columns if col not in final_columns_order_existing]
    df = df[final_columns_order_existing + other_columns]

    try:
        # Registrar la fuente de datos
        source_id = register_data_source("GuiaCores Web Scraping")
        logger.info(f"Fuente de datos registrada con ID: {source_id}")

        # Guardar datos crudos en la base de datos
        raw_data = df.to_dict('records')
        save_raw_leads_to_db(raw_data, source_id)

        # Guardar CSV limpio para carga manual
        os.makedirs(os.path.dirname(output_csv_path), exist_ok=True)
        df.to_csv(output_csv_path, index=False, encoding='utf-8')
        logger.info(f"CSV limpio guardado en {output_csv_path}")

    except Exception as e:
        logger.error(f"Error en el proceso de guardado: {e}")
        raise

    end_time = datetime.now()
    duration = end_time - start_time
    logger.info(f"Proceso de limpieza completado en {duration}")

if __name__ == "__main__":
    clean_leads_csv() 