import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    WebDriverException
)
from bs4 import BeautifulSoup
import logging
import re
from datetime import datetime
import os
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
import json
import urllib.parse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(processName)s - %(message)s', # Añadimos processName para identificar logs por proceso
    handlers=[
        logging.FileHandler('data/logs/scraper/scraper_guiaCores_bulk.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Directorio para guardar los resultados
OUTPUT_DIR = 'data/raw/csv'
os.makedirs(OUTPUT_DIR, exist_ok=True)
OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'guiaCores_bulk_leads.csv') # Archivo de salida para el scraper bulk

# Rango de IDs a intentar
START_ID = 1
END_ID = 99999 # Basado en tu estimación de IDs de hasta 5 dígitos

# URL base para los detalles del negocio por ID
DETAIL_BASE_URL = "https://www.guiacores.com.ar/index.php?r=search/detail&id="

class GuiaCoresDetailScraper:
    def __init__(self):
        self.driver = None
        self.setup_driver()

    def setup_driver(self):
        """Configura el driver de Chrome con opciones apropiadas para cada proceso worker"""
        try:
            chrome_options = Options()
            chrome_options.add_argument('--headless=new') # Ejecutar en modo headless
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--window-size=1920,1080')
            chrome_options.add_argument('--disable-extensions')
            chrome_options.add_argument('--disable-notifications')
            chrome_options.add_argument('--disable-infobars')
            chrome_options.add_argument('--disable-popup-blocking')
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')

            service = Service()
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            self.driver.set_page_load_timeout(30) # Reducir timeout para páginas de detalle
            logger.info("Driver de Chrome configurado exitosamente para el worker.")

        except Exception as e:
            logger.error(f"Error al configurar el driver de Chrome en el worker: {e}")
            # Es importante cerrar el driver si falla la configuración inicial
            if self.driver:
                self.driver.quit()
            raise

    def extract_detailed_info(self, business_id):
        """Extrae información detallada de la página de detalle de un negocio por su ID"""
        url = f"{DETAIL_BASE_URL}{business_id}"
        logger.info(f"Visitando ID: {business_id}")
        
        try:
            self.driver.get(url)

            # Verificar si la página es una página de error o no existe el negocio
            # Podemos buscar un elemento que solo aparece en páginas de negocio válidas
            # Por ejemplo, el nombre del negocio o la dirección
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'a.search-result-name h1, span.search-result-address'))
                )
            except TimeoutException:
                # Si los elementos clave no aparecen, asumimos que el ID no es válido o la página no existe
                logger.warning(f"Timeout o elementos clave no encontrados para ID {business_id}. Probablemente no es un ID válido.")
                return None # Retornar None para indicar que no se encontró un negocio válido
                
            # Esperar un momento adicional para asegurar que todo el contenido dinámico se cargue
            time.sleep(random.uniform(1, 2)) # Pequeña pausa aleatoria

            soup = BeautifulSoup(self.driver.page_source, 'html.parser')

            info = {}
            info['id_negocio'] = str(business_id) # Guardar el ID como string
            info['url'] = url
            info['fecha_extraccion'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # Extraer nombre del negocio
            name_tag = soup.select_one('a.search-result-name h1')
            info['nombre'] = name_tag.get_text(strip=True) if name_tag else 'N/A'

            # Extraer dirección
            address_tag = soup.select_one('span.search-result-address')
            info['direccion'] = address_tag.get_text(strip=True) if address_tag else 'N/A'

            # Extraer teléfonos
            phone_links = soup.select('a[href^="tel:"]')
            phones = [link.get_text(strip=True) for link in phone_links]
            info['telefonos'] = ', '.join(phones) if phones else 'N/A'

            # Extraer WhatsApp
            whatsapp_link = soup.select_one('a[href^="https://api.whatsapp.com/send?"]')
            whatsapp_number = 'N/A'
            if whatsapp_link:
                 if 'href' in whatsapp_link.attrs:
                     try:
                         # Intentar extraer el número del href si el texto no es claro
                         query_params = urllib.parse.parse_qs(urllib.parse.urlparse(whatsapp_link['href']).query)
                         if 'phone' in query_params and query_params['phone']:
                             whatsapp_number = query_params['phone'][0]
                         elif 'text' in query_params and query_params['text']:
                             # A veces el número está en el parámetro text
                             match = re.search(r'\d+', query_params['text'][0])
                             if match:
                                 whatsapp_number = match.group(0)
                     except Exception as e:
                         logger.debug(f"Error al parsear URL de WhatsApp para ID {business_id}: {e}")
                 # Si no se pudo extraer del href, intentar con el texto del enlace
                 if whatsapp_number == 'N/A':
                     whatsapp_number = whatsapp_link.get_text(strip=True)
                     if not whatsapp_number or not any(char.isdigit() for char in whatsapp_number):
                          whatsapp_number = 'N/A' # Si el texto tampoco es un número válido

            info['whatsapp'] = whatsapp_number

            # Extraer sitio web
            website_link = soup.select_one('a[itemprop="url"]')
            if not website_link:
                # Buscar por icono si itemprop="url" no está presente
                website_icon = soup.select_one('i.fa.fa-cloud')
                if website_icon:
                    website_link = website_icon.find_next('a', class_='search-result-link')
            info['sitio_web'] = website_link['href'] if website_link and 'href' in website_link.attrs else 'N/A'

            # Extraer email
            # El email a menudo está en un enlace con onclick="irContacto()" o cerca de un icono de sobre
            email_link = soup.select_one('a[onclick="irContacto()"]')
            if not email_link:
                email_icon = soup.select_one('i.fa.fa-envelope')
                if email_icon:
                    email_link = email_icon.find_next('a', class_='search-result-link') # Buscar el enlace siguiente con clase search-result-link
                    if not email_link:
                         # A veces el email está directamente en un span o texto sin enlace directo cerca del icono
                         email_text_element = email_icon.find_next(text=True)
                         if email_text_element and '@' in email_text_element:
                             info['email'] = email_text_element.strip()
                             email_link = None # Marcar como encontrado para no sobrescribir
            
            if email_link:
                info['email'] = email_link.get_text(strip=True) if email_link.get_text(strip=True) and '@' in email_link.get_text(strip=True) else 'N/A'
            elif 'email' not in info:
                 info['email'] = 'N/A'


            # Extraer redes sociales
            # Buscar enlaces que contengan los nombres de dominio
            facebook_link = soup.select_one('a[href*="facebook.com"]')
            info['facebook'] = facebook_link['href'] if facebook_link and 'href' in facebook_link.attrs else 'N/A'

            instagram_link = soup.select_one('a[href*="instagram.com"]')
            info['instagram'] = instagram_link['href'] if instagram_link and 'href' in instagram_link.attrs else 'N/A'

            # Extraer horarios
            horario_icon = soup.select_one('i.far.fa-clock')
            if horario_icon:
                # Buscar el texto del horario que suele estar cerca del icono o en un span específico
                horario_span = horario_icon.find_next(['span', 'div'], class_='search-result-address') # Puede estar en span o div
                if horario_span:
                     horarios_text = horario_span.get_text(strip=True)
                     # Limpiar texto de "Cerrado" o "Abierto" si están presentes
                     horarios_text = horarios_text.replace('Cerrado', '').replace('Abierto', '').strip()
                     info['horarios'] = horarios_text if horarios_text else 'N/A'
                else:
                    info['horarios'] = 'N/A'
            else:
                info['horarios'] = 'N/A'

            # Extraer rubros/categorías
            rubros_div = soup.select_one('div#yw0.list-view div.items') # Selector basado en el HTML proporcionado previamente
            if rubros_div:
                rubro_links = rubros_div.find_all('a', class_='search-result-link')
                rubros = [link.get_text(strip=True) for link in rubro_links]
                info['rubros'] = ', '.join(rubros) if rubros else 'N/A'
            else:
                # Buscar otras posibles ubicaciones de rubros si el selector anterior falla
                rubros_span = soup.select_one('span.search-result-category') # Otro selector común para categorías
                if rubros_span:
                    info['rubros'] = rubros_span.get_text(strip=True) if rubros_span.get_text(strip=True) else 'N/A'
                else:
                    info['rubros'] = 'N/A'


            # Extraer descripción
            description_tag = soup.select_one('div.search-result-description')
            info['descripcion'] = description_tag.get_text(strip=True) if description_tag else 'N/A'

            # Extraer servicios (buscar elementos comunes cerca de un icono de lista o similar)
            # Esto puede variar, si hay un patrón claro en el HTML, ajustarlo aquí.
            # Basado en el HTML previo, no hay una sección clara para "Servicios".
            # Podríamos intentar buscar texto que mencione servicios o patrones comunes.
            # Por ahora, lo dejaremos como N/A a menos que se identifique un selector fiable.
            info['servicios'] = 'N/A' # Placeholder, ajustar si se encuentra un selector

            # Extraer coordenadas si están disponibles (del elemento del mapa)
            map_element = soup.find('div', class_='map')
            if map_element:
                info['latitud'] = map_element.get('data-lat', 'N/A')
                info['longitud'] = map_element.get('data-lng', 'N/A')
            else:
                info['latitud'] = 'N/A'
                info['longitud'] = 'N/A'


            logger.info(f"Información detallada extraída para ID {business_id}: {info.get('nombre', 'N/A')}")
            return info

        except TimeoutException:
            logger.warning(f"Timeout al cargar o procesar ID {business_id}. Saltando.")
            return None
        except Exception as e:
            logger.error(f"Error al extraer información de ID {business_id}: {e}")
            return None # Retornar None en caso de error para no incluir datos incompletos


    def quit_driver(self):
        """Cierra el driver de Selenium"""
        if self.driver:
            try:
                self.driver.quit()
                logger.info("Driver de Chrome cerrado para el worker.")
            except Exception as e:
                logger.debug(f"Error al cerrar el driver en el worker: {e}") # Usar debug para errores de cierre no críticos

def scrape_id_range(id_chunk):
    """Función worker para procesar un chunk de IDs"""
    scraper = None
    scraped_data = []
    try:
        scraper = GuiaCoresDetailScraper()
        for business_id in id_chunk:
            data = scraper.extract_detailed_info(business_id)
            if data:
                scraped_data.append(data)
    except Exception as e:
        logger.error(f"Error en el worker al procesar chunk: {e}")
    finally:
        if scraper:
            scraper.quit_driver()
    return scraped_data

def load_processed_ids(filepath):
    """Carga los IDs ya procesados desde un archivo CSV existente"""
    processed_ids = set()
    if os.path.exists(filepath):
        try:
            # Leer solo la columna 'id_negocio' para eficiencia
            df = pd.read_csv(filepath, usecols=['id_negocio'], dtype={'id_negocio': str})
            processed_ids = set(df['id_negocio'])
            logger.info(f"Cargados {len(processed_ids)} IDs ya procesados desde {filepath}")
        except Exception as e:
            logger.warning(f"No se pudo cargar IDs procesados desde {filepath}: {e}")
            # Si hay un error al cargar, comenzamos desde cero para evitar perder datos
            processed_ids = set()
    return processed_ids

if __name__ == "__main__":
    logger.info("="*80)
    logger.info("Iniciando proceso de scraping masivo de Guía Cores por ID")
    logger.info("="*80)

    # Cargar IDs ya procesados para reanudar
    processed_ids = load_processed_ids(OUTPUT_FILE)

    # Generar la lista completa de IDs a intentar
    all_ids_to_attempt = [str(id) for id in range(START_ID, END_ID + 1)]
    logger.info(f"Generados {len(all_ids_to_attempt)} IDs potenciales ({START_ID} a {END_ID})")

    # Filtrar IDs ya procesados
    ids_to_scrape = [id for id in all_ids_to_attempt if id not in processed_ids]
    logger.info(f"IDs a scrapear (excluyendo ya procesados): {len(ids_to_scrape)}")

    if not ids_to_scrape:
        logger.info("No hay nuevos IDs para scrapear. Proceso finalizado.")
        exit()

    # Configurar el número de procesos (workers)
    # Usamos un número razonable de procesos, no necesariamente todos los cores de la CPU,
    # ya que cada uno inicia un navegador. Ajustar según la capacidad del sistema.
    num_processes = max(1, multiprocessing.cpu_count() - 2) # Dejar algunos cores libres
    logger.info(f"Usando {num_processes} procesos (workers) para el scraping")

    # Dividir los IDs a scrapear en chunks para cada worker
    # Aseguramos que cada worker reciba al menos 1 ID si hay IDs disponibles
    chunk_size = max(1, len(ids_to_scrape) // num_processes)
    id_chunks = [ids_to_scrape[i:i + chunk_size] for i in range(0, len(ids_to_scrape), chunk_size)]

    logger.info(f"Divididos IDs en {len(id_chunks)} chunks con tamaño aproximado {chunk_size}")

    all_scraped_data = []
    start_time = datetime.now()

    # Usar ProcessPoolExecutor para ejecutar los workers en paralelo
    try:
        with ProcessPoolExecutor(max_workers=num_processes) as executor:
            # map devuelve los resultados en el mismo orden en que se enviaron los chunks
            results = list(executor.map(scrape_id_range, id_chunks))

        # Combinar los resultados de todos los workers
        for chunk_data in results:
            all_scraped_data.extend(chunk_data)

    except Exception as e:
        logger.error(f"Error durante la ejecución paralela de los workers: {e}")

    end_time = datetime.now()
    duration = end_time - start_time

    logger.info(f"Proceso de scraping masivo completado.")
    logger.info(f"  - IDs intentados en esta ejecución (nuevos): {len(ids_to_scrape)}")
    logger.info(f"  - Negocios con datos extraídos exitosamente: {len(all_scraped_data)}")
    logger.info(f"  - Duración total: {duration}")
    logger.info(f"  - Total de IDs procesados (incluyendo ejecuciones anteriores): {len(processed_ids) + len(all_scraped_data)}")


    # Guardar todos los datos recolectados en un único archivo CSV
    if all_scraped_data:
        try:
            df = pd.DataFrame(all_scraped_data)

            # Si el archivo ya existe, añadir al final sin la cabecera
            if os.path.exists(OUTPUT_FILE):
                df.to_csv(OUTPUT_FILE, mode='a', header=False, index=False, encoding='utf-8')
                logger.info(f"Datos añadidos a {OUTPUT_FILE}")
            else:
                df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
                logger.info(f"Datos guardados en {OUTPUT_FILE}")

        except Exception as e:
            logger.error(f"Error al guardar los datos recolectados en CSV: {e}")
    else:
        logger.info("No se recolectaron nuevos datos para guardar.")


    logger.info("="*80)
    logger.info("Proceso de scraping masivo finalizado.")
    logger.info("="*80)
