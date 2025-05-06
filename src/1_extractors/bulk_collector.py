import os
import json
import time
import logging
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, 
    NoSuchElementException, 
    WebDriverException,
    StaleElementReferenceException,
    ElementClickInterceptedException,
    ElementNotInteractableException
)
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from ..common.versioning import DataVersioning

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data/logs/collector/url_collector_bulk.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class URLCollector:
    def __init__(self):
        self.base_url = "https://www.guiacores.com.ar/index.php"
        self.search_url = f"{self.base_url}?r=search%2Findex&b=&R=&L=&Tm=1"
        self.driver = None
        self.urls = set()
        self.versioner = DataVersioning(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        self.setup_driver()
        
    def setup_driver(self):
        """Setup Chrome driver with appropriate options"""
        try:
            chrome_options = Options()
            # Comentamos el modo headless temporalmente para debug
            # chrome_options.add_argument('--headless=new')
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
            
            # Añadir opciones para mejor diagnóstico
            chrome_options.add_argument('--enable-logging')
            chrome_options.add_argument('--v=1')
            
            service = Service()
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            self.driver.set_page_load_timeout(3600)
            
        except Exception as e:
            logger.error(f"Error al configurar el driver de Chrome: {str(e)}")
            raise

    def get_page_state(self):
        """Obtiene el estado actual de la página"""
        try:
            # Obtener el HTML de la página para debug
            page_source = self.driver.page_source
            logger.debug(f"HTML de la página: {page_source[:1000]}...")  # Primeros 1000 caracteres
            
            # Intentar diferentes selectores para encontrar elementos
            selectors = {
                "business-card": len(self.driver.find_elements(By.CLASS_NAME, "business-card")),
                "card-mobile": len(self.driver.find_elements(By.CLASS_NAME, "card-mobile")),
                "business-item": len(self.driver.find_elements(By.CLASS_NAME, "business-item")),
                "card": len(self.driver.find_elements(By.CLASS_NAME, "card")),
                "all-cards": len(self.driver.find_elements(By.CSS_SELECTOR, "[class*='card']"))
            }
            
            return {
                'ready_state': self.driver.execute_script("return document.readyState"),
                'jquery_loaded': self.driver.execute_script("return typeof jQuery !== 'undefined'"),
                'jquery_version': self.driver.execute_script("return jQuery.fn.jquery") if self.driver.execute_script("return typeof jQuery !== 'undefined'") else None,
                'url': self.driver.current_url,
                'title': self.driver.title,
                'selectors': selectors,
                'ver_mas_visible': self.driver.find_element(By.ID, "ver-mas").is_displayed() if len(self.driver.find_elements(By.ID, "ver-mas")) > 0 else False,
                'body_length': len(page_source)
            }
        except Exception as e:
            return {'error': str(e)}

    def wait_for_page_load(self, max_attempts=20, check_interval=5):
        """Espera a que la página esté completamente cargada con monitoreo continuo"""
        attempt = 0
        last_element_count = 0
        stable_count = 0
        
        # Espera inicial para la carga dinámica
        logger.info("Esperando carga dinámica inicial...")
        time.sleep(10)
        
        while attempt < max_attempts:
            try:
                # Obtener estado actual
                page_state = self.get_page_state()
                current_elements = sum(page_state.get('selectors', {}).values())
                
                # Log del estado actual
                logger.info(f"Estado de la página (intento {attempt + 1}/{max_attempts}):")
                logger.info(json.dumps(page_state, indent=2))
                
                # Verificar si la página está lista
                if page_state['ready_state'] == 'complete' and page_state['jquery_loaded']:
                    # Verificar si hay elementos de negocio
                    if current_elements > 0:
                        # Verificar si el número de elementos se ha estabilizado
                        if current_elements == last_element_count:
                            stable_count += 1
                            if stable_count >= 2:  # Si el conteo se mantiene estable por 2 checks
                                logger.info("Página cargada y estable")
                                return True
                        else:
                            stable_count = 0
                            logger.info(f"Nuevos elementos detectados: {current_elements - last_element_count}")
                    
                    last_element_count = current_elements
                
                # Esperar antes del siguiente check
                time.sleep(check_interval)
                attempt += 1
                
            except Exception as e:
                logger.error(f"Error durante el monitoreo: {str(e)}")
                attempt += 1
                time.sleep(check_interval)
        
        logger.error("Tiempo máximo de espera alcanzado")
        return False

    def load_initial_page(self):
        """Carga la página inicial y espera a que esté lista"""
        try:
            logger.info(f"Intentando cargar página inicial: {self.search_url}")
            self.driver.get(self.search_url)
            
            # Esperar a que la página esté completamente cargada
            if not self.wait_for_page_load():
                return False
            
            return True
        except Exception as e:
            logger.error(f"Error inesperado al cargar la página inicial: {str(e)}")
            return False

    def get_total_pages(self):
        """Obtiene el total de páginas disponibles usando JavaScript"""
        try:
            # Intentar obtener el total de páginas del JavaScript
            total_pages = self.driver.execute_script("return window.aaa();")
            if total_pages:
                logger.info(f"Total de páginas obtenido vía JavaScript: {total_pages}")
                return int(total_pages)
            
            # Fallback: intentar obtenerlo del DOM
            pagination = self.driver.find_element(By.CLASS_NAME, "pagination")
            if pagination:
                last_page = pagination.find_elements(By.TAG_NAME, "a")[-2]
                total = int(last_page.text)
                logger.info(f"Total de páginas obtenido vía DOM: {total}")
                return total
            
            return 0
        except Exception as e:
            logger.error(f"Error al obtener el total de páginas: {e}")
            return 0

    def click_ver_mas(self):
        """Hace clic en el botón 'Ver más' usando la lógica de la página"""
        try:
            # Verificar que el botón existe y está visible
            ver_mas = self.driver.find_element(By.ID, "ver-mas")
            if not ver_mas.is_displayed():
                logger.warning("El botón 'Ver más' no está visible")
                return False

            # Hacer scroll hasta el botón
            self.driver.execute_script("arguments[0].scrollIntoView(true);", ver_mas)
            time.sleep(1)  # Pequeña pausa para asegurar que el scroll se complete

            # Obtener conteo de elementos antes del clic
            elements_before = len(self.driver.find_elements(By.CLASS_NAME, "business-card"))
            logger.info(f"Elementos antes de hacer clic: {elements_before}")

            # Intentar hacer clic directamente
            try:
                ver_mas.click()
            except ElementClickInterceptedException:
                logger.warning("El clic fue interceptado, intentando con JavaScript")
                self.driver.execute_script("arguments[0].click();", ver_mas)
            except ElementNotInteractableException:
                logger.warning("El elemento no es interactuable, intentando con JavaScript")
                self.driver.execute_script("arguments[0].click();", ver_mas)
            except Exception as e:
                logger.warning(f"Error al hacer clic directo: {str(e)}, intentando con JavaScript")
                self.driver.execute_script("arguments[0].click();", ver_mas)

            # Esperar y verificar que se cargaron nuevos elementos
            max_wait = 30  # segundos máximo de espera
            start_time = time.time()
            while time.time() - start_time < max_wait:
                elements_after = len(self.driver.find_elements(By.CLASS_NAME, "business-card"))
                if elements_after > elements_before:
                    logger.info(f"Nuevos elementos cargados: {elements_after - elements_before}")
                    return True
                time.sleep(1)
            
            logger.warning("No se detectaron nuevos elementos después del clic")
            return False
            
        except Exception as e:
            logger.error(f"Error inesperado al hacer clic: {str(e)}")
            return False

    def get_total_elements(self):
        """Obtiene el total de elementos actuales en la página"""
        try:
            elements = self.driver.find_elements(By.CLASS_NAME, "business-card")
            return len(elements)
        except Exception as e:
            logger.error(f"Error al contar elementos: {e}")
            return 0

    def extract_business_urls(self):
        """Extrae las URLs de los negocios en la página actual"""
        try:
            business_cards = self.driver.find_elements(By.CLASS_NAME, "business-card")
            initial_count = len(self.urls)
            
            for card in business_cards:
                try:
                    link = card.find_element(By.TAG_NAME, "a").get_attribute("href")
                    if link:
                        self.urls.add(link)
                except NoSuchElementException:
                    continue
                    
            new_urls = len(self.urls) - initial_count
            if new_urls > 0:
                logger.info(f"Extraídas {new_urls} nuevas URLs en esta página")
                    
        except Exception as e:
            logger.error(f"Error al extraer URLs: {e}")

    def collect_all_urls(self):
        """Recolecta URLs usando el botón 'Ver más'"""
        logger.info("Iniciando recolección de URLs")
        
        try:
            if not self.load_initial_page():
                return []

            # Obtener el total de páginas inicial
            total_pages = self.get_total_pages()
            if total_pages <= 0:
                logger.error("No se pudo determinar el total de páginas")
                return []

            logger.info(f"Total de páginas detectadas: {total_pages}")
            
            # Contar elementos iniciales
            initial_elements = self.get_total_elements()
            logger.info(f"Elementos iniciales: {initial_elements}")

            current_page = 1
            last_element_count = initial_elements
            stall_count = 0
            max_stalls = 3
            
            while current_page < total_pages:
                # Extraer URLs de la página actual
                self.extract_business_urls()
                
                # Mostrar progreso
                progress = (current_page / total_pages) * 100
                logger.info(f"Progreso: {progress:.1f}% | URLs: {len(self.urls)}")
                
                # Intentar hacer clic en "Ver más"
                if not self.click_ver_mas():
                    stall_count += 1
                    if stall_count >= max_stalls:
                        logger.error("Demasiados intentos fallidos, deteniendo la recolección")
                        break
                    continue
                
                # Verificar que se cargaron nuevos elementos
                current_elements = self.get_total_elements()
                
                if current_elements <= last_element_count:
                    stall_count += 1
                    if stall_count >= max_stalls:
                        logger.error("No se detectaron nuevos elementos después de varios intentos")
                        break
                    continue
                
                last_element_count = current_elements
                current_page += 1
                stall_count = 0  # Resetear el contador de fallos si todo va bien

            # Contar elementos finales
            final_elements = self.get_total_elements()
            logger.info(f"Elementos finales: {final_elements}")
            logger.info(f"Total de elementos cargados: {final_elements - initial_elements}")

            return list(self.urls)

        except Exception as e:
            logger.error(f"Error en la recolección de URLs: {str(e)}")
            return list(self.urls)
        finally:
            if self.driver:
                try:
                    self.driver.quit()
                except:
                    pass

    def save_urls(self):
        """Guarda las URLs recolectadas en un archivo JSON con versionado mensual"""
        if not self.urls:
            logger.warning("No hay URLs para guardar")
            return None

        try:
            # Preparar los datos para guardar
            data = {
                'timestamp': datetime.now().isoformat(),
                'total_urls': len(self.urls),
                'urls': list(self.urls)
            }

            # Usar el versionador para guardar los datos mensualmente
            versioned_path = self.versioner.version_bulk_data(data, filename='bulk_urls')
            
            if versioned_path:
                logger.info(f"URLs guardadas exitosamente en: {versioned_path}")
                return versioned_path
            else:
                logger.error("Error al versionar el archivo de URLs")
                return None

        except Exception as e:
            logger.error(f"Error al guardar las URLs: {e}")
            return None

if __name__ == "__main__":
    collector = URLCollector()
    urls = collector.collect_all_urls()
    collector.save_urls()
    logger.info(f"Recolección completada. Total URLs: {len(urls)}") 