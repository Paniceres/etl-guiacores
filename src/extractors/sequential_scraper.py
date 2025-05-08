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

class GuiaCoresScraper:
    def __init__(self, start_id=None, end_id=None, resume=True):
        self.base_url = "https://www.guiacores.com.ar/index.php"
        self.search_url = f"{self.base_url}?r=search%2Findex&b=&R=&L=&Tm=1"
        self.driver = None
        self.start_time = datetime.now()
        self.stats = {
            'pages_scraped': 0,
            'businesses_found': 0,
            'errors': 0,
            'start_time': self.start_time,
            'end_time': None
        }
        self.start_id = start_id
        self.end_id = end_id
        self.resume = resume
        self.processed_ids = set()
        
        # Cargar IDs ya procesados si estamos resumiendo
        if self.resume:
            self.load_processed_ids()
        
        self.setup_driver()
        
    def load_processed_ids(self):
        """Carga los IDs ya procesados desde el CSV existente"""
        try:
            if os.path.exists('data/guiaCores_leads.csv'):
                df = pd.read_csv('data/guiaCores_leads.csv')
                if 'id_negocio' in df.columns:
                    self.processed_ids = set(df['id_negocio'].astype(str))
                    logger.info(f"Cargados {len(self.processed_ids)} IDs ya procesados")
        except Exception as e:
            logger.error(f"Error al cargar IDs procesados: {e}")

    def setup_driver(self):
        """Setup Chrome driver with appropriate options"""
        try:
            chrome_options = Options()
            
            # Configuración específica para Chromium en modo headless
            chrome_options.binary_location = check_chrome_installation()
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
            service = Service()
            
            # Inicializar el driver
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            logger.info("Driver de Chrome configurado exitosamente")
            
            # Verificar que estamos en modo headless
            if not self.driver.execute_script("return navigator.webdriver"):
                logger.info("Modo headless activado correctamente")
            else:
                logger.warning("El modo headless podría no estar funcionando correctamente")
            
        except Exception as e:
            logger.error(f"Error al configurar el driver de Chrome: {e}")
            raise

    def get_all_business_links(self):
        """Obtiene todos los enlaces de negocios haciendo clic en 'Ver más' hasta que no haya más"""
        logger.info("Iniciando recolección de enlaces de negocios...")
        all_links = []
        unique_business_ids = set()  # Conjunto para rastrear IDs únicos
        page_count = 0
        
        try:
            self.driver.get(self.search_url)
            
            while True:
                page_count += 1
                logger.info(f"Procesando página {page_count}")
                
                # Esperar a que los elementos de negocio se carguen
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "card-mobile"))
                )
                
                # Esperar un momento adicional para asegurar que todo el contenido dinámico se cargue
                time.sleep(2)
                
                # Parsear la página actual
                soup = BeautifulSoup(self.driver.page_source, 'html.parser')
                business_cards = soup.find_all('div', class_='card-mobile gc-item')
                
                if not business_cards:
                    logger.warning(f"No se encontraron negocios en la página {page_count}")
                    break
                
                # Contadores para esta página
                new_businesses = 0
                duplicate_businesses = 0
                
                # Extraer enlaces y IDs
                for card in business_cards:
                    name_link = card.find('span', class_='nombre-comercio').find('a')
                    if name_link and 'href' in name_link.attrs:
                        detail_url = name_link['href']
                        # Asegurarse de que la URL sea absoluta si es relativa
                        if not detail_url.startswith('http'):
                            detail_url = "https://www.guiacores.com.ar" + detail_url

                        # Extraer los parámetros de la URL
                        parsed_url = urllib.parse.urlparse(detail_url)
                        query_params = urllib.parse.parse_qs(parsed_url.query)
                        business_id = query_params.get('id', [None])[0]
                        idb = query_params.get('idb', [None])[0]

                        if business_id:
                            if business_id not in unique_business_ids:
                                unique_business_ids.add(business_id)
                                all_links.append((business_id, detail_url, idb))
                                new_businesses += 1
                                logger.debug(f"Nuevo negocio encontrado - ID: {business_id}, IDB: {idb}")
                            else:
                                duplicate_businesses += 1
                                logger.debug(f"Negocio duplicado encontrado - ID: {business_id}")
                
                logger.info(f"Página {page_count}:")
                logger.info(f"  - Total de negocios en la página: {len(business_cards)}")
                logger.info(f"  - Nuevos negocios: {new_businesses}")
                logger.info(f"  - Negocios duplicados: {duplicate_businesses}")
                logger.info(f"  - Total de negocios únicos hasta ahora: {len(unique_business_ids)}")
                
                # Si no encontramos nuevos negocios en esta página, probablemente estamos en un bucle
                if new_businesses == 0:
                    logger.warning("No se encontraron nuevos negocios en esta página. Deteniendo el scraping.")
                    break
                
                # Intentar hacer clic en "Ver más"
                try:
                    ver_mas_button = WebDriverWait(self.driver, 10).until(
                        EC.element_to_be_clickable((By.ID, "ver-mas"))
                    )
                    
                    # Hacer scroll hasta el botón y hacer clic
                    self.driver.execute_script("arguments[0].scrollIntoView(true);", ver_mas_button)
                    time.sleep(1)
                    ver_mas_button.click()
                    
                    # Esperar a que se cargue el nuevo contenido
                    time.sleep(2)
                    
                    # Verificar si el botón sigue visible (si no, significa que no hay más contenido)
                    try:
                        WebDriverWait(self.driver, 3).until(
                            EC.visibility_of_element_located((By.ID, "ver-mas"))
                        )
                    except:
                        logger.info("No hay más contenido para cargar")
                        break
                        
                except Exception as e:
                    logger.info("No hay más páginas para cargar")
                    break
            
            logger.info(f"Resumen final:")
            logger.info(f"  - Total de páginas procesadas: {page_count}")
            logger.info(f"  - Total de negocios únicos encontrados: {len(unique_business_ids)}")
            logger.info(f"  - Total de enlaces recolectados: {len(all_links)}")
            
            return all_links
            
        except Exception as e:
            logger.error(f"Error al recolectar enlaces: {str(e)}")
            self.stats['errors'] += 1
            return all_links

    def extract_detailed_info(self, url):
        """Extract detailed information from a business's detail page"""
        try:
            logger.info(f"Visitando página de detalle: {url}")
            self.driver.get(url)
            
            # Esperar a que la página cargue completamente
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "search-result-name"))
            )
            
            # Esperar un momento adicional para asegurar que todo el contenido dinámico se cargue
            time.sleep(2)
            
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            info = {}
            
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
                info['horarios'] = horario_span.get_text(strip=True).replace('Cerrado', '').replace('Abierto', '').strip() if horario_span else 'N/A'
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
            logger.error(f"Error extracting detailed info from {url}: {str(e)}")
            self.stats['errors'] += 1
            return {}

    def save_stats(self):
        """Guarda las estadísticas del scraping en un archivo JSON"""
        self.stats['end_time'] = datetime.now()
        self.stats['duration'] = str(self.stats['end_time'] - self.stats['start_time'])
        
        stats_file = 'data/scraping_stats.json'
        os.makedirs('data', exist_ok=True)
        
        with open(stats_file, 'w') as f:
            json.dump(self.stats, f, indent=4, default=str)
        
        logger.info(f"Estadísticas guardadas en {stats_file}")

    def process_urls(self, urls):
        """Procesa un conjunto de URLs y guarda los resultados"""
        all_businesses = []
        
        for url_data in urls:
            business_id = url_data['id']
            url = url_data['url']
            
            # Saltar si ya fue procesado
            if business_id in self.processed_ids:
                logger.info(f"Saltando ID ya procesado: {business_id}")
                continue
            
            # Saltar si está fuera del rango especificado
            if self.start_id and int(business_id) < int(self.start_id):
                continue
            if self.end_id and int(business_id) > int(self.end_id):
                continue
            
            logger.info(f"Procesando negocio {business_id}")
            business_data = self.extract_detailed_info(url)
            
            if business_data:
                business_data['id_negocio'] = business_id
                business_data['url'] = url
                business_data['fecha_extraccion'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                all_businesses.append(business_data)
                self.processed_ids.add(business_id)
                self.stats['businesses_found'] += 1
            
            time.sleep(1)  # Be nice to the server
        
        # Guardar resultados parciales
        if all_businesses:
            df = pd.DataFrame(all_businesses)
            os.makedirs('data', exist_ok=True)
            
            # Si el archivo existe, añadir al final
            csv_path = 'data/guiaCores_leads.csv'
            if os.path.exists(csv_path):
                df.to_csv(csv_path, mode='a', header=False, index=False, encoding='utf-8')
            else:
                df.to_csv(csv_path, index=False, encoding='utf-8')
            
            logger.info(f"Guardados {len(all_businesses)} negocios en {csv_path}")
        
        return all_businesses

    def save_leads(self):
        """Guarda los leads recolectados en un archivo CSV"""
        try:
            os.makedirs('data/raw/csv', exist_ok=True)
            output_file = 'data/raw/csv/guiaCores_leads.csv'
            
            df = pd.DataFrame(self.leads)
            df.to_csv(output_file, index=False)
            logger.info(f"Leads guardados exitosamente en {output_file}")
            
        except Exception as e:
            logger.error(f"Error al guardar leads: {e}")

def process_url_chunk(chunk):
    """Función para procesar un chunk de URLs en un proceso separado"""
    scraper = GuiaCoresScraper()
    try:
        return scraper.process_urls(chunk)
    finally:
        if scraper.driver:
            scraper.driver.quit()

def main():
    # Cargar URLs recolectadas
    try:
        with open('data/collected_urls.json', 'r', encoding='utf-8') as f:
            url_data = json.load(f)
            all_urls = url_data['urls']
    except Exception as e:
        logger.error(f"Error al cargar URLs recolectadas: {e}")
        return
    
    # Configurar el número de procesos
    num_processes = multiprocessing.cpu_count()
    chunk_size = len(all_urls) // num_processes
    
    # Dividir URLs en chunks
    url_chunks = [all_urls[i:i + chunk_size] for i in range(0, len(all_urls), chunk_size)]
    
    logger.info(f"Iniciando scraping con {num_processes} procesos")
    logger.info(f"Total de URLs a procesar: {len(all_urls)}")
    logger.info(f"Tamaño de cada chunk: {chunk_size}")
    
    # Procesar chunks en paralelo
    with ProcessPoolExecutor(max_workers=num_processes) as executor:
        results = list(executor.map(process_url_chunk, url_chunks))
    
    # Combinar resultados
    all_businesses = [business for chunk_result in results for business in chunk_result]
    
    logger.info(f"Proceso completado. Total de negocios procesados: {len(all_businesses)}")

if __name__ == "__main__":
    main() 