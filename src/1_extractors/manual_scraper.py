import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import time
import urllib.parse
import re
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import logging
from typing import List, Dict, Any
from ..common.versioning import DataVersioning

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data/logs/collector/scraper_estudiosContables.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# URLs
BASE_URL = "https://www.guiacores.com.ar/"

# Database configuration from environment variables
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'etl_db')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'postgres')

# Ruta al directorio que contiene los archivos HTML locales
LOCAL_HTML_DIRECTORY_PATH = '/app/html_samples/'
OUTPUT_CSV_PATH = '/app/data/leads_from_local_files.csv'

# Inicializar el versionador
versioner = DataVersioning(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

# --- Funciones de parsing (copiadas de scraper.py) ---
# Se copian aquí para que este script sea independiente

def parse_search_results_page(html_content):
    """
    Parsea el contenido HTML para extraer los URLs de las páginas de detalle
    y su ID único (basado solo en 'id').
    Retorna una lista de tuplas (id_contador, url_completa).
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    detail_urls_with_ids = []

    # Encuentra todos los elementos que contienen la información de un contador
    # Basado en la estructura del HTML proporcionado: div con clase 'card-mobile gc-item'
    contador_cards = soup.find_all('div', class_='card-mobile gc-item')

    for card in contador_cards:
        # Dentro de cada tarjeta, encuentra el enlace a la página de detalle
        # Basado en la estructura del HTML: span con clase 'nombre-comercio' y un enlace <a> dentro
        name_link = card.find('span', class_='nombre-comercio').find('a')
        if name_link and 'href' in name_link.attrs:
            detail_url = name_link['href']
            # Asegúrate de que la URL sea absoluta si es relativa
            if not detail_url.startswith('http'):
                detail_url = BASE_URL + detail_url

            # Extraer el parámetro 'id' de la URL
            parsed_url = urllib.parse.urlparse(detail_url)
            query_params = urllib.parse.parse_qs(parsed_url.query)
            contador_id = query_params.get('id', [None])[0] # Obtiene el valor del parámetro 'id'

            if contador_id:
                detail_urls_with_ids.append((contador_id, detail_url))
            # No emitimos advertencia si falta 'id', simplemente no lo añadimos a la lista

    return detail_urls_with_ids

def parse_detail_page(html_content):
    """
    Parsea el contenido HTML de una página de detalle de contador
    para extraer la información relevante.
    """
    soup = BeautifulSoup(html_content, 'html.parser')

    data = {}

    # Extraer Nombre del Contador
    # Basado en la estructura del HTML: h1 dentro de a con clase 'search-result-name'
    name_tag = soup.select_one('a.search-result-name h1')
    data['Nombre'] = name_tag.get_text(strip=True) if name_tag else 'N/A'

    # Extraer Dirección
    # Basado en la estructura del HTML: span con clase 'search-result-address'
    address_tag = soup.select_one('span.search-result-address')
    data['Dirección'] = address_tag.get_text(strip=True) if address_tag else 'N/A'

    # Extraer Teléfonos
    # Busca enlaces con href que empiecen con 'tel:'
    phone_links = soup.select('a[href^="tel:"]')
    phones = [link.get_text(strip=True) for link in phone_links]
    data['Teléfonos'] = ', '.join(phones) if phones else 'N/A'

    # Extraer WhatsApp
    # Busca enlaces con href que empiecen con 'https://api.whatsapp.com/send?'
    whatsapp_link = soup.select_one('a[href^="https://api.whatsapp.com/send?"]')
    whatsapp_number = 'N/A'
    if whatsapp_link:
        # Intenta obtener el número del texto del enlace primero
        whatsapp_number = whatsapp_link.get_text(strip=True)
        # Si el texto no parece un número, intenta parsear el href
        if not whatsapp_number or not any(char.isdigit() for char in whatsapp_number):
             if 'href' in whatsapp_link.attrs:
                 try:
                     query_params = urllib.parse.parse_qs(urllib.parse.urlparse(whatsapp_link['href']).query)
                     if 'phone' in query_params and query_params['phone']:
                         whatsapp_number = query_params['phone'][0]
                 except Exception as e:
                     print(f"Error al parsear URL de WhatsApp: {e}")

    data['WhatsApp'] = whatsapp_number


    # Extraer Sitio Web
    # Busca enlaces con icono de nube (fa fa-cloud) cerca o con itemprop="url"
    website_link = soup.select_one('a[itemprop="url"]')
    if not website_link:
         # Fallback si no hay itemprop, busca por icono
         website_icon = soup.select_one('i.fa.fa-cloud')
         if website_icon:
             # El enlace del sitio web puede ser el siguiente <a> después del icono
             website_link = website_icon.find_next('a', class_='search-result-link')

    data['Sitio Web'] = website_link['href'] if website_link and 'href' in website_link.attrs else 'N/A'


    # Extraer Email
    # Busca enlaces con icono de sobre (fa fa-envelope) cerca o con onclick="irContacto()"
    email_link = soup.select_one('a[onclick="irContacto()"]')
    if not email_link:
        # Fallback si no hay onclick, busca por icono
        email_icon = soup.select_one('i.fa.fa-envelope')
        if email_icon:
            # El enlace del email puede ser el siguiente <a> después del icono
            email_link = email_icon.find_next('a', class_='search-result-link')

    data['Email'] = email_link.get_text(strip=True) if email_link else 'N/A'


    # Extraer Redes Sociales (Facebook, Instagram)
    # Busca enlaces con iconos específicos
    facebook_link = soup.select_one('a[href*="facebook.com"]')
    data['Facebook'] = facebook_link['href'] if facebook_link and 'href' in facebook_link.attrs else 'N/A'

    instagram_link = soup.select_one('a[href*="instagram.com"]')
    data['Instagram'] = instagram_link['href'] if instagram_link and 'href' in instagram_link.attrs else 'N/A'


    # Puedes añadir más campos si los necesitas (ej. horarios, rubros, etc.)
    # Horarios: Busca el span con clase 'search-result-address' después del icono de reloj
    horario_icon = soup.select_one('i.far.fa-clock')
    if horario_icon:
        horario_span = horario_icon.find_next('span', class_='search-result-address')
        data['Horario'] = horario_span.get_text(strip=True).replace('Cerrado', '').replace('Abierto', '').strip() if horario_span else 'N/A'
    else:
        data['Horario'] = 'N/A'


    # Rubros: Busca div con id 'yw0' y clase 'list-view', luego elementos 'a' dentro de 'div.items'
    rubros_div = soup.select_one('div#yw0.list-view div.items')
    if rubros_div:
        rubro_links = rubros_div.find_all('a', class_='search-result-link')
        rubros = [link.get_text(strip=True) for link in rubro_links]
        data['Rubros'] = ', '.join(rubros) if rubros else 'N/A'
    else:
        data['Rubros'] = 'N/A'


    return data

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
        print(f"Error al conectar a la base de datos: {e}")
        raise

def init_db():
    """Inicializa las tablas necesarias en la base de datos"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Tabla para almacenar los leads
            cur.execute("""
                CREATE TABLE IF NOT EXISTS leads (
                    id SERIAL PRIMARY KEY,
                    contador_id VARCHAR(50) UNIQUE,
                    nombre VARCHAR(255),
                    direccion TEXT,
                    telefonos TEXT,
                    whatsapp VARCHAR(50),
                    sitio_web TEXT,
                    email VARCHAR(255),
                    facebook TEXT,
                    instagram TEXT,
                    horario TEXT,
                    rubros TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Tabla para el log de scraping
            cur.execute("""
                CREATE TABLE IF NOT EXISTS scraping_log (
                    id SERIAL PRIMARY KEY,
                    start_time TIMESTAMP,
                    end_time TIMESTAMP,
                    total_leads_processed INTEGER,
                    status VARCHAR(50),
                    error_message TEXT
                )
            """)
            
            conn.commit()
    except Exception as e:
        print(f"Error al inicializar la base de datos: {e}")
        raise
    finally:
        conn.close()

def save_leads_to_db(leads_data):
    """Guarda los leads en la base de datos"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Preparar los datos para la inserción
            values = []
            for lead in leads_data:
                # Extraer el contador_id de la URL si está disponible
                contador_id = None
                if 'url' in lead:
                    parsed_url = urllib.parse.urlparse(lead['url'])
                    query_params = urllib.parse.parse_qs(parsed_url.query)
                    contador_id = query_params.get('id', [None])[0]

                values.append((
                    contador_id,
                    lead.get('Nombre', 'N/A'),
                    lead.get('Dirección', 'N/A'),
                    lead.get('Teléfonos', 'N/A'),
                    lead.get('WhatsApp', 'N/A'),
                    lead.get('Sitio Web', 'N/A'),
                    lead.get('Email', 'N/A'),
                    lead.get('Facebook', 'N/A'),
                    lead.get('Instagram', 'N/A'),
                    lead.get('Horario', 'N/A'),
                    lead.get('Rubros', 'N/A')
                ))

            # Usar UPSERT para actualizar registros existentes
            execute_values(cur, """
                INSERT INTO leads (
                    contador_id, nombre, direccion, telefonos, whatsapp,
                    sitio_web, email, facebook, instagram, horario, rubros
                ) VALUES %s
                ON CONFLICT (contador_id) DO UPDATE SET
                    nombre = EXCLUDED.nombre,
                    direccion = EXCLUDED.direccion,
                    telefonos = EXCLUDED.telefonos,
                    whatsapp = EXCLUDED.whatsapp,
                    sitio_web = EXCLUDED.sitio_web,
                    email = EXCLUDED.email,
                    facebook = EXCLUDED.facebook,
                    instagram = EXCLUDED.instagram,
                    horario = EXCLUDED.horario,
                    rubros = EXCLUDED.rubros,
                    updated_at = CURRENT_TIMESTAMP
            """, values)

            conn.commit()
    except Exception as e:
        print(f"Error al guardar leads en la base de datos: {e}")
        raise
    finally:
        conn.close()

def log_scraping_session(start_time, end_time, total_leads, status, error_message=None):
    """Registra una sesión de scraping en la base de datos"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO scraping_log (
                    start_time, end_time, total_leads_processed,
                    status, error_message
                ) VALUES (%s, %s, %s, %s, %s)
            """, (start_time, end_time, total_leads, status, error_message))
            conn.commit()
    except Exception as e:
        print(f"Error al registrar la sesión de scraping: {e}")
        raise
    finally:
        conn.close()

# --- Función principal para el script local ---

def scrape_from_local_html_directory(directory_path):
    """
    Procesa archivos HTML locales y extrae información de leads.
    """
    start_time = time.time()
    all_leads = []
    processed_files = 0
    error_files = 0

    try:
        # Obtener lista de archivos HTML en el directorio
        html_files = [f for f in os.listdir(directory_path) if f.endswith('.html')]
        
        for html_file in html_files:
            file_path = os.path.join(directory_path, html_file)
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    html_content = f.read()

                # Extraer URLs de la página de resultados
                detail_urls_with_ids = parse_search_results_page(html_content)
                
                # Procesar cada URL de detalle
                for contador_id, detail_url in detail_urls_with_ids:
                    try:
                        # Simular la obtención del contenido HTML de la página de detalle
                        # En un caso real, esto sería una petición HTTP
                        detail_html = html_content  # Por ahora usamos el mismo contenido
                        lead_data = parse_detail_page(detail_html)
                        lead_data['ID'] = contador_id
                        lead_data['URL'] = detail_url
                        all_leads.append(lead_data)
                    except Exception as e:
                        logger.error(f"Error al procesar URL {detail_url}: {e}")
                        continue

                processed_files += 1
                logger.info(f"Archivo procesado exitosamente: {html_file}")

            except Exception as e:
                error_files += 1
                logger.error(f"Error al procesar archivo {html_file}: {e}")
                continue

        # Convertir a DataFrame y guardar
        if all_leads:
            df = pd.DataFrame(all_leads)
            
            # Versionar el archivo CSV
            versioned_path = versioner.version_csv_file(OUTPUT_CSV_PATH)
            if versioned_path:
                df.to_csv(versioned_path, index=False, encoding='utf-8')
                logger.info(f"Datos guardados exitosamente en: {versioned_path}")
            else:
                logger.error("Error al versionar el archivo CSV")

        end_time = time.time()
        log_scraping_session(
            start_time,
            end_time,
            len(all_leads),
            'success',
            f"Procesados {processed_files} archivos, {error_files} errores"
        )

        return all_leads

    except Exception as e:
        end_time = time.time()
        log_scraping_session(
            start_time,
            end_time,
            len(all_leads),
            'error',
            str(e)
        )
        logger.error(f"Error general en el proceso de scraping: {e}")
        return []

# Ejecutar el scraper desde el directorio de archivos locales
if __name__ == "__main__":
    scrape_from_local_html_directory(LOCAL_HTML_DIRECTORY_PATH)

def save_leads(leads: List[Dict[str, Any]], output_file: str = 'data/raw/csv/estudiosContables_leads.csv') -> None:
    """
    Guarda los leads en un archivo CSV.
    
    Args:
        leads: Lista de diccionarios con los datos de los leads
        output_file: Ruta del archivo CSV de salida
    """
    try:
        # Crear directorio de salida si no existe
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        # ... (resto del código sin cambios)
