import time
import random
import logging
from typing import List, Dict, Optional
from concurrent.futures import ProcessPoolExecutor, as_completed
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup
import urllib.parse
import re
from datetime import datetime

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(processName)s - %(message)s',
    handlers=[
        logging.FileHandler('data/logs/scraper/scraper_guiaCores_bulk.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class BulkScraper:
    """Scraper para el modo bulk que procesa URLs en paralelo"""

    def __init__(self, config: dict):
        self.config = config
        self.bulk_config = self.config['extractor']['bulk']
        self.max_workers = self.bulk_config.get('max_workers', 4)
        self.timeout = self.bulk_config.get('timeout', 30)

    def _setup_driver(self) -> webdriver.Chrome:
        """Configura y retorna un driver de Chrome para el worker"""
        try:
            chrome_options = Options()
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

            service = Service()
            driver = webdriver.Chrome(service=service, options=chrome_options)
            driver.set_page_load_timeout(self.timeout)
            return driver

        except Exception as e:
            logger.error(f"Error al configurar el driver de Chrome: {e}")
            raise

    def _extract_business_info(self, driver: webdriver.Chrome, url: str) -> Optional[Dict]:
        """Extrae información de un negocio desde su URL"""
        try:
            driver.get(url)
            business_id = url.split('id=')[-1] if 'id=' in url else url.split('/')[
                -1]

            # Esperar elementos clave
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'a.search-result-name h1, span.search-result-address')))
            except TimeoutException:
                logger.warning(
                    f"Timeout o elementos clave no encontrados para ID {business_id}")
                return None

            # Pequeña pausa aleatoria
            time.sleep(random.uniform(1, 2))

            soup = BeautifulSoup(driver.page_source, 'html.parser')

            # Extraer información básica
            info = {
                'id_negocio': business_id,
                'url': url,
                'fecha_extraccion': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'nombre': self._extract_text(soup, 'a.search-result-name h1'),
                'direccion': self._extract_text(soup, 'span.search-result-address'),
                'telefonos': self._extract_phones(soup),
                'whatsapp': self._extract_whatsapp(soup),
                'sitio_web': self._extract_website(soup),
                'email': self._extract_email(soup),
                'facebook': self._extract_social(soup, 'facebook.com'),
                'instagram': self._extract_social(soup, 'instagram.com'),
                'horarios': self._extract_hours(soup),
                'rubros': self._extract_categories(soup),
                'descripcion': self._extract_text(soup, 'div.search-result-description'),
                'servicios': 'N/A',  # Placeholder
                'latitud': self._extract_coordinates(soup, 'data-lat'),
                'longitud': self._extract_coordinates(soup, 'data-lng')
            }

            logger.info(f"Información extraída para ID {business_id}: {info['nombre']}")
            return info

        except Exception as e:
            logger.error(f"Error al extraer información de {url}: {e}")
            return None

    def _extract_text(self, soup: BeautifulSoup, selector: str) -> str:
        """Extrae texto de un elemento usando un selector"""
        element = soup.select_one(selector)
        return element.get_text(strip=True) if element else 'N/A'

    def _extract_phones(self, soup: BeautifulSoup) -> str:
        """Extrae números de teléfono"""
        phones = [link.get_text(strip=True) for link in soup.select('a[href^="tel:"]')]
        return ', '.join(phones) if phones else 'N/A'

    def _extract_whatsapp(self, soup: BeautifulSoup) -> str:
        """Extrae número de WhatsApp"""
        whatsapp_link = soup.select_one(
            'a[href^="https://api.whatsapp.com/send?"]')
        if not whatsapp_link:
            return 'N/A'

        try:
            query_params = urllib.parse.parse_qs(
                urllib.parse.urlparse(whatsapp_link['href']).query)
            if 'phone' in query_params:
                return query_params['phone'][0]
            elif 'text' in query_params:
                match = re.search(r'\d+', query_params['text'][0])
                return match.group(0) if match else 'N/A'
        except Exception:
            pass

        return whatsapp_link.get_text(strip=True) if any(char.isdigit() for char in whatsapp_link.get_text(strip=True)) else 'N/A'

    def _extract_website(self, soup: BeautifulSoup) -> str:
        """Extrae sitio web"""
        website_link = soup.select_one('a[itemprop="url"]') or \
            soup.select_one('i.fa.fa-cloud + a.search-result-link')
        return website_link['href'] if website_link and 'href' in website_link.attrs else 'N/A'

    def _extract_email(self, soup: BeautifulSoup) -> str:
        """Extrae email"""
        email_link = soup.select_one('a[onclick="irContacto()"]') or \
            soup.select_one('i.fa.fa-envelope + a.search-result-link')
        if email_link:
            return email_link.get_text(strip=True) if '@' in email_link.get_text(strip=True) else 'N/A'

        email_text = soup.select_one('i.fa.fa-envelope + text')
        return email_text.strip() if email_text and '@' in email_text else 'N/A'

    def _extract_social(self, soup: BeautifulSoup, domain: str) -> str:
        """Extrae enlaces de redes sociales"""
        link = soup.select_one(f'a[href*="{domain}"]')
        return link['href'] if link and 'href' in link.attrs else 'N/A'

    def _extract_hours(self, soup: BeautifulSoup) -> str:
        """Extrae horarios"""
        horario_icon = soup.select_one('i.far.fa-clock')
        if not horario_icon:
            return 'N/A'

        horario_span = horario_icon.find_next(
            ['span', 'div'], class_='search-result-address')
        if not horario_span:
            return 'N/A'

        text = horario_span.get_text(strip=True)
        return text.replace('Cerrado', '').replace('Abierto', '').strip() or 'N/A'

    def _extract_categories(self, soup: BeautifulSoup) -> str:
        """Extrae categorías/rubros"""
        rubros_div = soup.select_one('div#yw0.list-view div.items')
        if rubros_div:
            rubros = [link.get_text(strip=True)
                      for link in rubros_div.find_all('a', class_='search-result-link')]
            return ', '.join(rubros) if rubros else 'N/A'

        rubros_span = soup.select_one('span.search-result-category')
        return rubros_span.get_text(strip=True) if rubros_span else 'N/A'

    def _extract_coordinates(self, soup: BeautifulSoup, attr: str) -> str:
        """Extrae coordenadas del mapa"""
        map_element = soup.find('div', class_='map')
        return map_element.get(attr, 'N/A') if map_element else 'N/A'

    @staticmethod
    def _scrape_single_url_worker(url: str, config: dict) -> Optional[Dict]:
        """Procesa una única URL, extrayendo información y manejando el driver."""
        driver = None
        try:
            # Configurar el driver para este worker
            chrome_options = Options()
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

            service = Service()
            driver = webdriver.Chrome(service=service, options=chrome_options)

            # BulkScraper instance for _extract_business_info
            scraper = BulkScraper(config)  # Pass config to BulkScraper

            info = scraper._extract_business_info(driver, url)
            return info

        except Exception as e:
            logger.error(f"Worker failed to process {url}: {e}", exc_info=True)
            return None
        finally:
            if driver:
                driver.quit()
                logger.info("Worker finished and quit driver")

    def scrape_urls(self, urls: List[str]) -> List[Dict]:
        """
        Procesa una lista de URLs en paralelo usando múltiples workers

        Args:
            urls (List[str]): Lista de URLs a procesar

        Returns:
            List[Dict]: Lista de diccionarios con la información extraída
        """
        try:
            logger.info(
                f"Iniciando scraping de {len(urls)} URLs con {self.max_workers} workers")

            all_results = []
            with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
                # Enviar cada URL individual como una tarea
                futures = {executor.submit(BulkScraper._scrape_single_url_worker, url, self.config): url for url in urls}

                for future in as_completed(futures):
                    url = futures[future]
                    try:
                        result = future.result()
                        if result:
                            all_results.append(result)
                    except Exception as exc:
                        logger.error(
                            f'An exception occurred while processing {url}: {exc}')

            logger.info(
                f"Scraping completado. Se extrajeron {len(all_results)} registros")
            return all_results

        except Exception as e:
            logger.error(f"Error en el proceso de scraping: {e}", exc_info=True)
            return []
