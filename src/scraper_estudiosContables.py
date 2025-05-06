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
    Lee todos los archivos HTML en un directorio local, extrae URLs de detalle únicos
    de todos ellos, visita cada URL único y extrae los datos, guardando en la base de datos.
    """
    start_time = datetime.now()
    all_leads_data = []
    all_detail_urls_dict = {}

    try:
        # Inicializar la base de datos
        init_db()

        if not os.path.isdir(directory_path):
            print(f"Error: El directorio local no se encuentra en {directory_path}")
            return

        print(f"Procesando archivos HTML en el directorio: {directory_path}")

        # Obtener la lista de archivos HTML en el directorio
        html_files = [f for f in os.listdir(directory_path) if f.endswith('.html')]
        if not html_files:
            print(f"No se encontraron archivos HTML en el directorio {directory_path}")
            return

        print(f"Encontrados {len(html_files)} archivos HTML para procesar.")

        # --- Parte 1: Extraer URLs de detalle de todos los archivos locales ---
        for file_name in html_files:
            file_path = os.path.join(directory_path, file_name)
            print(f"Extrayendo URLs de: {file_name}")
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    local_html_content = f.read()

                page_detail_urls_with_ids = parse_search_results_page(local_html_content)

                added_count = 0
                for contador_id, detail_url in page_detail_urls_with_ids:
                    if contador_id not in all_detail_urls_dict:
                        all_detail_urls_dict[contador_id] = detail_url
                        added_count += 1
                    else:
                        all_detail_urls_dict[contador_id] = detail_url
                print(f"  Encontrados {len(page_detail_urls_with_ids)} URLs. Añadidos {added_count} IDs únicos.")

            except Exception as e:
                print(f"Error al leer o parsear el archivo {file_name}: {e}. Saltando.")
                continue

        print(f"Total de IDs de contador únicos recolectados: {len(all_detail_urls_dict)}")

        # --- Parte 2: Visitar cada URL de detalle único y extraer datos ---
        print("Scrapeando páginas de detalle de los URLs únicos encontrados...")
        unique_detail_urls_list = list(all_detail_urls_dict.values())
        print(f"Procesando {len(unique_detail_urls_list)} URLs únicas de detalle.")

        for i, detail_url in enumerate(unique_detail_urls_list):
            print(f"Procesando URL {i+1}/{len(unique_detail_urls_list)}: {detail_url}")
            try:
                response = requests.get(detail_url)
                response.raise_for_status()
                detail_page_html = response.text

                lead_data = parse_detail_page(detail_page_html)
                lead_data['url'] = detail_url  # Añadir la URL al diccionario de datos
                all_leads_data.append(lead_data)

                time.sleep(0.5)

            except requests.exceptions.RequestException as e:
                print(f"Error al obtener la página {detail_url}: {e}. Saltando.")
                continue
            except Exception as e:
                print(f"Error inesperado al parsear la página {detail_url}: {e}. Saltando.")
                continue

        # --- Parte 3: Guardar los datos en la base de datos ---
        if all_leads_data:
            save_leads_to_db(all_leads_data)
            print(f"Datos guardados exitosamente en la base de datos")
            
            # También guardar en CSV como backup
            df = pd.DataFrame(all_leads_data)
            os.makedirs(os.path.dirname(OUTPUT_CSV_PATH), exist_ok=True)
            df.to_csv(OUTPUT_CSV_PATH, index=False, encoding='utf-8')
            print(f"Backup CSV guardado en {OUTPUT_CSV_PATH}")
        else:
            print("No se encontraron datos para guardar.")

        # Registrar la sesión de scraping exitosa
        end_time = datetime.now()
        log_scraping_session(
            start_time=start_time,
            end_time=end_time,
            total_leads=len(all_leads_data),
            status='success'
        )

    except Exception as e:
        # Registrar la sesión de scraping fallida
        end_time = datetime.now()
        log_scraping_session(
            start_time=start_time,
            end_time=end_time,
            total_leads=len(all_leads_data),
            status='error',
            error_message=str(e)
        )
        raise

# Ejecutar el scraper desde el directorio de archivos locales
if __name__ == "__main__":
    scrape_from_local_html_directory(LOCAL_HTML_DIRECTORY_PATH)
