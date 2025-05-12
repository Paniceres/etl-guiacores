import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import logging
import re
from datetime import datetime
import os
import subprocess
import sys
import json
import urllib.parse
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
import signal
import atexit
from typing import List, Dict, Any, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data/logs/collector/scraper_guiaCores.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- Helper Functions (Mantener las existentes) ---
def check_chrome_installation():
    """Verifica si Chrome o Chromium está instalado en el sistema"""
    chrome_paths = [
        '/usr/bin/chromium',
        '/usr/bin/chromium-browser',
        '/usr/bin/google-chrome',
        '/usr/bin/google-chrome-stable'
    ]

    for path in chrome_paths:
        if os.path.exists(path):
            logger.info(f"Encontrado navegador en: {path}")
            return path

    return None

def install_chrome():
    """Intenta instalar Chrome o Chromium según el sistema operativo"""
    logger.info("Intentando instalar Chromium...")

    try:
        # Detectar el sistema operativo
        if os.path.exists('/etc/arch-release'):  # Arch Linux/Manjaro
            subprocess.run(['sudo', 'pacman', '-S', '--noconfirm', 'chromium'], check=True)
        elif os.path.exists('/etc/debian_version'):  # Debian/Ubuntu
            subprocess.run(['sudo', 'apt-get', 'update'], check=True)
            subprocess.run(['sudo', 'apt-get', 'install', '-y', 'chromium-browser'], check=True)
        elif os.path.exists('/etc/fedora-release'):  # Fedora
            subprocess.run(['sudo', 'dnf', 'install', '-y', 'chromium'], check=True)
        else:
            logger.error("Sistema operativo no soportado para instalación automática")
            return False

        logger.info("Chromium instalado exitosamente")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Error al instalar Chromium: {e}")
        return False
# --- End Helper Functions ---


class GuiaCoresScraper:
    def __init__(self, start_id=None, end_id=None, resume=True, driver=None):
        self.base_url = "https://www.guiacores.com.ar/index.php"
        self.search_url = f"{self.base_url}?r=search%2Findex&b=&R=&L=&Tm=1" # Esta URL no se usa para scraping detallado por lista
        self.driver = driver # Permite pasar un driver ya existente
        self.start_time = datetime.now()
        self.stats = {
            'pages_scraped': 0, # Estadísticas a nivel de instancia, no por chunk
            'businesses_found': 0,
            'errors': 0,
            'start_time': self.start_time,
            'end_time': None
        }
        self.start_id = start_id # Estos se usarán para el modo bulk si se adapta
        self.end_id = end_id   # Estos se usarán para el modo bulk si se adapta
        self.resume = resume # Esto se usa principalmente para cargar processed_ids

        self.processed_ids = set()

        # Cargar IDs ya procesados si estamos resumiendo
        if self.resume:
            self.load_processed_ids()

        # El driver NO se inicializa automáticamente aquí

    def setup_driver(self):
        """Setup Chrome driver with appropriate options"""
        if self.driver is None:
            try:
                chrome_options = Options()

                # Configuración específica para Chromium en modo headless
                chrome_binary = check_chrome_installation()
                if chrome_binary:
                     chrome_options.binary_location = chrome_binary
                else:
                     logger.warning("Chromium/Chrome no encontrado. Intentando instalar...")
                     if install_chrome():
                         chrome_options.binary_location = check_chrome_installation()
                     else:
                         logger.error("No se pudo instalar Chromium/Chrome. El scraping fallará.")
                         # Decide si levantar una excepción o continuar con None y manejarlo en los métodos de scraping
                         # Por ahora, lanzaremos una excepción ya que el scraping sin driver no es posible
                         raise RuntimeError("Chromium/Chrome necesario para Selenium no está instalado.")


                chrome_options.add_argument('--headless=new')
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

                # Configurar el servicio de Chrome
                # WebDriverManager se encargará de descargar el driver si es necesario
                service = Service(ChromeDriverManager().install())

                # Inicializar el driver
                self.driver = webdriver.Chrome(service=service, options=chrome_options)
                logger.info("Driver de Chrome configurado exitosamente")

                # Verificar que estamos en modo headless
                try:
                    if not self.driver.execute_script("return navigator.webdriver"):
                        logger.info("Modo headless activado correctamente")
                    else:
                        logger.warning("El modo headless podría no estar funcionando correctamente")
                except Exception as e:
                     logger.warning(f"No se pudo verificar el modo headless: {e}")


            except Exception as e:
                logger.error(f"Error al configurar el driver de Chrome: {e}")
                raise

    def quit_driver(self):
        """Quits the Chrome driver if it's active"""
        if self.driver:
            self.driver.quit()
            self.driver = None
            logger.info("Driver de Chrome cerrado exitosamente")


    def load_processed_ids(self):
        """Carga los IDs ya procesados desde el CSV existente"""
        try:
            csv_path = 'data/guiaCores_leads.csv'
            if os.path.exists(csv_path):
                # Leer solo la columna 'id_negocio' y convertir a set de strings
                df = pd.read_csv(csv_path, usecols=['id_negocio'], dtype={'id_negocio': str})
                if 'id_negocio' in df.columns:
                    self.processed_ids = set(df['id_negocio'])
                    logger.info(f"Cargados {len(self.processed_ids)} IDs ya procesados desde {csv_path}")
                else:
                     logger.warning(f"La columna 'id_negocio' no se encontró en {csv_path}. No se cargarán IDs procesados.")
            else:
                logger.info(f"No se encontró el archivo {csv_path}. Se iniciará sin IDs procesados.")

        except pd.errors.EmptyDataError:
             logger.warning(f"El archivo {csv_path} está vacío. No se cargarán IDs procesados.")
        except FileNotFoundError:
             logger.info(f"No se encontró el archivo {csv_path}. Se iniciará sin IDs procesados.")
        except Exception as e:
            logger.error(f"Error al cargar IDs procesados desde {csv_path}: {e}")


    def get_all_business_links(self):
        """
        Obtiene todos los enlaces de negocios haciendo clic en 'Ver más'.
        Este método se usará solo si se decide usar GuiaCoresScraper para la recolección general,
        no para el modo sequential que usa SequentialCollector.
        """
        # Implementación existente... (mantener sin cambios si se va a usar en otro contexto)
        logger.warning("Este método get_all_business_links no se usa en el modo sequential con SequentialCollector.")
        raise NotImplementedError("Este método no está integrado en el flujo actual de Sequential ETL.")


    def extract_detailed_info(self, url):
        """Extract detailed information from a business's detail page"""
        # Mantener la implementación existente, asumiendo que self.driver está disponible
        if self.driver is None:
             raise RuntimeError("Driver de Selenium no inicializado. Llame a setup_driver primero.")

        try:
            logger.info(f"Visitando página de detalle: {url}")
            self.driver.get(url)

            # Esperar a que la página cargue completamente
            WebDriverWait(self.driver, 10).until(\
                EC.presence_of_element_located((By.CLASS_NAME, "search-result-name"))\
            )

            # Esperar un momento adicional para asegurar que todo el contenido dinámico se cargue
            time.sleep(1) # Reducir sleep si es posible

            soup = BeautifulSoup(self.driver.page_source, 'html.parser')

            info = {}

            # ... (Lógica de extracción de datos existente) ...
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
                whatsapp_number = whatsapp_link.get_text(strip=True)
                if not whatsapp_number or not any(char.isdigit() for char in whatsapp_number):
                    if 'href' in whatsapp_link.attrs:
                        try:
                            query_params = urllib.parse.parse_qs(urllib.parse.urlparse(whatsapp_link['href']).query)
                            if 'phone' in query_params and query_params['phone']:
                                whatsapp_number = query_params['phone'][0]
                        except Exception as e:
                            logger.error(f"Error al parsear URL de WhatsApp: {e}")
            info['whatsapp'] = whatsapp_number

            # Extraer sitio web
            website_link = soup.select_one('a[itemprop="url"]')
            if not website_link:
                website_icon = soup.select_one('i.fa.fa-cloud')
                if website_icon:
                    website_link = website_icon.find_next('a', class_='search-result-link')
            info['sitio_web'] = website_link['href'] if website_link and 'href' in website_link.attrs else 'N/A'

            # Extraer email
            email_link = soup.select_one('a[onclick="irContacto()"]')
            if not email_link:
                email_icon = soup.select_one('i.fa.fa-envelope')
                if email_icon:
                    email_link = email_icon.find_next('a', class_='search-result-link')
            info['email'] = email_link.get_text(strip=True) if email_link else 'N/A'

            # Extraer redes sociales
            facebook_link = soup.select_one('a[href*="facebook.com"]')
            info['facebook'] = facebook_link['href'] if facebook_link and 'href' in facebook_link.attrs else 'N/A'

            instagram_link = soup.select_one('a[href*="instagram.com"]')
            info['instagram'] = instagram_link['href'] if instagram_link and 'href' in instagram_link.attrs else 'N/A'

            # Extraer horarios
            horario_icon = soup.select_one('i.far.fa-clock')
            if horario_icon:
                horario_span = horario_icon.find_next('span', class_='search-result-address')
                horarios_text = horario_span.get_text(strip=True) if horario_span else ''
                # Limpiar texto de horarios
                horarios_clean = horarios_text.replace('Cerrado', '').replace('Abierto', '').strip()
                info['horarios'] = horarios_clean if horarios_clean else 'N/A'

            else:
                info['horarios'] = 'N/A'

            # Extraer rubros/categorías
            rubros_div = soup.select_one('div#yw0.list-view div.items')
            if rubros_div:
                rubro_links = rubros_div.find_all('a', class_='search-result-link')
                rubros = [link.get_text(strip=True) for link in rubro_links]
                info['rubros'] = ', '.join(rubros) if rubros else 'N/A'
            else:
                info['rubros'] = 'N/A'

            # Extraer coordenadas si están disponibles
            map_element = soup.find('div', class_='map')
            if map_element:
                info['latitud'] = map_element.get('data-lat', 'N/A')
                info['longitud'] = map_element.get('data-lng', 'N/A')

            logger.info(f"Información detallada extraída para: {info.get('nombre', 'Negocio')}")
            return info

        except Exception as e:
            logger.error(f"Error extracting detailed info from {url}: {str(e)}", exc_info=True)
            # No incrementar self.stats['errors'] aquí si esta función se llama en paralelo,
            # ya que self.stats no es seguro en multiprocessing. Las estadísticas se manejarán en el proceso principal.
            return {}

    def append_to_csv(self, data: List[Dict[str, Any]]):
        """Append a list of dictionaries to the CSV file."""
        if not data:
            return

        df = pd.DataFrame(data)
        csv_path = 'data/guiaCores_leads.csv'
        os.makedirs('data', exist_ok=True)

        try:
            if os.path.exists(csv_path):
                # Ensure header is written only once
                df.to_csv(csv_path, mode='a', header=False, index=False, encoding='utf-8')
            else:
                df.to_csv(csv_path, index=False, encoding='utf-8')
            logger.info(f"Appended {len(data)} records to {csv_path}")
        except Exception as e:
            logger.error(f"Error appending data to CSV {csv_path}: {e}", exc_info=True)
            # Decidir si relanzar la excepción o simplemente loggear. Para resiliencia, loggear es mejor.


    # Modificar process_urls para que sea el método de scraping principal para una lista de URLs
    # Este método será llamado por cada proceso en el multiprocessing.
    def process_urls(self, urls: List[Dict[str, str]]) -> List[Dict[str, Any]]:
        """
        Processes a list of URLs, scrapes detailed info, saves partial results to CSV,
        and returns the scraped data for further processing.
        This method is designed to be called by the multiprocessing pool.
        """
        # Cargar processed_ids dentro de cada proceso para que sea thread/process-safe
        # Esto asume que el archivo CSV no está siendo modificado por *otros* procesos
        # mientras este proceso carga. La lógica de `append_to_csv` maneja bien adjuntar.
        # Sin embargo, el set de processed_ids aquí solo reflejará el estado del CSV
        # al INICIO de la ejecución de este chunk. Esto puede llevar a duplicados mínimos
        # si un ID es procesado y guardado por otro chunk *después* de que este chunk
        # cargó sus processed_ids pero *antes* de que lo intente procesar. Es un compromiso aceptable.
        self.load_processed_ids()


        all_businesses_in_chunk = []
        scraped_count = 0
        skipped_count = 0
        error_count = 0

        # Setup driver for this process/chunk
        self.setup_driver() # Cada proceso tendrá su propio driver

        try:
            for url_data in urls:
                business_id = str(url_data.get('id')) # Asegurar que sea string para la comparación con set
                url = url_data.get('url')

                if not business_id or not url:
                    logger.warning(f"Saltando URL inválida/incompleta en chunk: {url_data}")
                    error_count += 1
                    continue

                # Saltar si ya fue procesado por cualquier proceso
                # Esta verificación se basa en el CSV cargado al inicio del chunk.
                # Para una idempotencia total entre procesos concurrentes, se necesitaría un mecanismo de bloqueo
                # o una base de datos atómica para registrar IDs procesados, lo cual es más complejo.
                # La carga inicial mitiga duplicados obvios.
                if business_id in self.processed_ids:
                    logger.debug(f"Saltando ID ya procesado (local set): {business_id}")
                    skipped_count += 1
                    continue

                logger.info(f"Procesando negocio {business_id} (Chunk)")
                business_data = self.extract_detailed_info(url)

                if business_data:
                    business_data['id_negocio'] = business_id
                    business_data['url'] = url # Añadir URL y fecha de extracción
                    business_data['fecha_extraccion'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                    all_businesses_in_chunk.append(business_data)
                    self.processed_ids.add(business_id) # Añadir al set local del proceso
                    scraped_count += 1

                    # Guardar resultados parciales en CSV después de cada negocio (o agrupar para eficiencia)
                    # Guardar uno a uno puede ser lento. Agruparemos para guardar.
                    # self.append_to_csv([business_data]) # Opción 1: Guardar uno a uno

                else:
                    logger.warning(f"No se obtuvieron datos detallados para URL: {url} (ID: {business_id})")
                    error_count += 1


                time.sleep(0.5) # Ajustar el delay si es necesario y seguro

            # Opción 2: Guardar todos los negocios scrapeados en este chunk al final del chunk
            self.append_to_csv(all_businesses_in_chunk)
            logger.info(f"Chunk processing finished: Scraped {scraped_count}, Skipped {skipped_count}, Errors {error_count}")

        except Exception as e:
            logger.error(f"Critical error processing chunk: {e}", exc_info=True)
            # Registrar el error a nivel de chunk
            error_count += len(urls) - scraped_count - skipped_count # Asumir que el resto falló
            # Relanzar la excepción para que el executor la maneje si es necesario, o simplemente loggearla
            # Para resiliencia, es mejor loggear y dejar que el chunk termine (aunque con errores).
            # raise # Uncomment to let the exception propagate and potentially stop the pool

        finally:
            # Asegurarse de cerrar el driver en este proceso
            self.quit_driver()

        # Retornar los datos scrapeados en este chunk
        return all_businesses_in_chunk


    # El método save_stats puede usarse en el proceso principal después de combinar resultados si es necesario
    def save_stats(self):
         """Guarda las estadísticas del scraping en un archivo JSON"""
         self.stats['end_time'] = datetime.now()
         # Calcular duración correctamente si start_time fue configurado
         if 'start_time' in self.stats and isinstance(self.stats['start_time'], datetime):
            self.stats['duration'] = str(self.stats['end_time'] - self.stats['start_time'])
         else:
             self.stats['duration'] = 'N/A' # O manejar según tu flujo

         stats_file = 'data/scraping_stats.json'
         os.makedirs('data', exist_ok=True)

         try:
             with open(stats_file, 'w') as f:
                 # Usar default=str para serializar datetimes si no son string aún
                 json.dump(self.stats, f, indent=4, default=str)
             logger.info(f"Estadísticas guardadas en {stats_file}")
         except Exception as e:
             logger.error(f"Error al guardar estadísticas en {stats_file}: {e}", exc_info=True)


    # El método save_leads ya no es el principal para guardar resultados detallados
    # se usa append_to_csv ahora
    def save_leads(self):
         """Guarda los leads recolectados en un archivo CSV"""
         logger.warning("El método save_leads ya no es el principal para guardar datos detallados. Use append_to_csv.")
         # Mantener por compatibilidad si es necesario, pero no se usará en el flujo refactorizado de sequential.
         # Suponiendo que self.leads contiene los datos a guardar si este método se llama.
         if hasattr(self, 'leads') and self.leads:
             try:
                 os.makedirs('data/raw/csv', exist_ok=True)
                 output_file = 'data/raw/csv/guiaCores_leads.csv'
                 df = pd.DataFrame(self.leads)
                 # Usar modo 'w' si esto es para guardar leads recolectados *inicialmente*
                 df.to_csv(output_file, index=False, encoding='utf-8')
                 logger.info(f"Leads (iniciales) guardados exitosamente en {output_file}")
             except Exception as e:
                 logger.error(f"Error al guardar leads (iniciales): {e}", exc_info=True)


# Función wrapper para ser usada por ProcessPoolExecutor
# Cada llamada a esta función ocurre en un proceso separado
def process_url_chunk_for_sequential(chunk: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    """
    Wrapper function to initialize scraper and process a chunk of URLs.
    Designed to be run in a separate process.
    """
    logger.info(f"Proceso hijo iniciado para chunk de {len(chunk)} URLs.")
    scraper = None
    try:
        # No pasar resume=True si la carga de processed_ids se hace dentro de process_urls
        # O pasar resume=True y load_processed_ids se llama en __init__ (menos seguro en multiprocessing)
        # Decidimos llamar load_processed_ids dentro de process_urls para más seguridad.
        scraper = GuiaCoresScraper(resume=True) # Carga processed_ids al inicio del proceso/chunk
        # El driver se configura dentro de process_urls ahora
        scraped_data = scraper.process_urls(chunk)
        logger.info(f"Proceso hijo finalizado para chunk. Scrapeados {len(scraped_data)} negocios.")
        return scraped_data
    except Exception as e:
        logger.error(f"Error en proceso hijo procesando chunk: {e}", exc_info=True)
        # Es importante cerrar el driver si hubo un error antes de que process_urls terminara
        if scraper and scraper.driver:
             scraper.quit_driver()
        return [] # Retornar lista vacía o manejar el error según la necesidad del llamador principal
    finally:
        # Asegurarse de que el driver se cierra incluso si no hubo excepción en el try block principal
        if scraper and scraper.driver:
             scraper.quit_driver()
        logger.info("Proceso hijo finalizado.")


# La función main() original ahora representa el flujo CLI con multiprocessing,
# pero podemos adaptarla o crear una similar en src/main.py para ser llamada
# por run_sequential_etl. La lógica de dividir en chunks y usar ProcessPoolExecutor
# se moverá o duplicará en src/main.py.

# Eliminamos el if __name__ == "__main__": aquí para que este archivo sea un módulo importable.
# La lógica de ejecución principal ahora residirá en src/main.py o en scripts de Argo.
