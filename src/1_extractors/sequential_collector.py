import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    ElementClickInterceptedException,
    ElementNotInteractableException,
    StaleElementReferenceException,
    WebDriverException, # Excepción general de Selenium
    SessionNotCreatedException # Para errores al iniciar el driver
)
from bs4 import BeautifulSoup
import logging
import json
import os
from datetime import datetime
import random
from typing import List, Dict, Any, Optional, Union, Tuple
import re
import sys # Para salir del script en caso de error crítico
from ..common.versioning import DataVersioning

# Configuración básica de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data/logs/collector/sequential_collector.log'),
        logging.StreamHandler(sys.stdout) # Asegurar que los logs también vayan a la consola
    ]
)
logger = logging.getLogger(__name__)

# Directorio para guardar los resultados
OUTPUT_DIR = 'data/raw/json_collected_urls'
os.makedirs(OUTPUT_DIR, exist_ok=True)

# URL base de búsqueda avanzada con opción de email (por defecto)
DEFAULT_SEARCH_URL = "https://www.guiacores.com.ar/index.php?r=search%2Findex&b=&R=&L=&Tm=1"

# Selectores CSS (AJUSTADOS SEGÚN EL HTML PROPORCIONADO)
BUSINESS_ITEM_SELECTOR = '.card-mobile, .gc-item' # Selector para cada elemento de negocio
VER_MAS_BUTTON_SELECTOR = '#ver-mas' # Selector para el botón "Ver más"
LOADING_INDICATOR_SELECTOR = '#cargando-pagina' # Selector para el indicador de carga AJAX

# Selectores para la búsqueda avanzada (AJUSTADOS SEGÚN EL HTML PROPORCIONADO)
BUSQUEDA_AVANZADA_BUTTON_SELECTOR = 'a[data-target="#formBusquedaAvazada"]' # Selector para el enlace/botón que abre la búsqueda avanzada
BUSQUEDA_AVANZADA_MODAL_SELECTOR = '#formBusquedaAvazada' # Selector para el modal de búsqueda avanzada
RUBRO_SELECT_SELECTOR = '#ddlRubroFilter' # Selector para el dropdown de rubro
LOCALIDAD_SELECT_SELECTOR = '#ddlLocalidadFilter2' # Selector para el dropdown de localidad
ADVANCED_SEARCH_SUBMIT_BUTTON_SELECTOR = '#botonBuscarAvanzada' # Selector para el botón de buscar dentro del formulario avanzado


class SequentialCollector:
    """Colector para procesar URLs secuencialmente con carga dinámica, con o sin filtros."""

    def __init__(self, rubros: Union[str, List[str]] = None, localidades: Union[str, List[str]] = None):
        self.logger = logger
        self.config = {
            'load_timeout': 30, # Tiempo máximo de espera para cargar elementos o botón
            'click_delay': 1, # Pausa después de hacer clic en "Ver más"
            'search_url': DEFAULT_SEARCH_URL,
            'business_selector': BUSINESS_ITEM_SELECTOR,
            'button_selector': VER_MAS_BUTTON_SELECTOR,
            'loading_indicator_selector': LOADING_INDICATOR_SELECTOR,
        }

        self.rubros = self._normalize_list(rubros)
        self.localidades = self._normalize_list(localidades)
        self.driver: Optional[webdriver.Chrome] = None
        self.collected_urls: Dict[str, str] = {} # Diccionario {id: url} para almacenar URLs únicas
        self.versioner = DataVersioning(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

    def _normalize_list(self, value: Union[str, List[str], None]) -> List[str]:
        """
        Normaliza un valor a lista, eliminando None y strings vacíos después de strip.
        """
        if value is None:
            return []
        if isinstance(value, str):
            return [value]
        return [str(item).strip() for item in value if item is not None and str(item).strip()]

    def setup_driver(self) -> bool:
        """Configura el navegador Selenium en modo headless."""
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
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            self.driver.set_page_load_timeout(self.config['load_timeout'])
            self.logger.info("Driver de Chrome configurado exitosamente.")
            return True

        except SessionNotCreatedException as e:
            self.logger.critical(f"Error CRÍTICO al iniciar la sesión del navegador. Asegúrate de tener Chrome/Chromium instalado y un chromedriver compatible en tu PATH. Error: {e}")
            self.driver = None
            return False
        except Exception as e:
            self.logger.critical(f"Error CRÍTICO al configurar el driver de Chrome: {e}")
            if self.driver:
                try:
                    self.driver.quit()
                except:
                    pass
            self.driver = None
            return False

    def collect_urls(self) -> Dict[str, str]:
        """
        Procesa URLs secuencialmente para las combinaciones de rubro/localidad.
        Retorna un diccionario {id: url} de todas las URLs únicas recolectadas.
        """
        if not self.setup_driver():
            self.logger.error("No se pudo configurar el driver. Abortando recolección.")
            return {}

        all_collected_urls: Dict[str, str] = {} # Diccionario para acumular URLs de todas las combinaciones

        try:
            combinations_to_process: List[Tuple[Optional[str], Optional[str]]] = []

            # Construir la lista de combinaciones a procesar
            if not self.rubros and not self.localidades:
                combinations_to_process.append((None, None))
                self.logger.info("No se especificaron rubros ni localidades. Procesando la búsqueda por defecto.")
            elif self.rubros and not self.localidades:
                 combinations_to_process.extend([(r, None) for r in self.rubros])
                 self.logger.info(f"Procesando los siguientes rubros: {self.rubros}")
            elif not self.rubros and self.localidades:
                 combinations_to_process.extend([(None, l) for l in self.localidades])
                 self.logger.info(f"Procesando las siguientes localidades: {self.localidades}")
            else:
                combinations_to_process.extend([(r, l) for r in self.rubros for l in self.localidades])
                self.logger.info(f"Procesando las siguientes combinaciones (Rubro, Localidad): {combinations_to_process}")

            if not combinations_to_process:
                 self.logger.warning("No hay combinaciones válidas para procesar.")
                 return {}


            # Procesar cada combinación secuencialmente
            for rubro, localidad in combinations_to_process:
                self.logger.info("="*50)
                self.logger.info(f"Iniciando recolección para Rubro: {rubro if rubro else 'Por defecto'}, Localidad: {localidad if localidad else 'Por defecto'}")
                self.logger.info("="*50)

                # Resetear las URLs recolectadas para esta combinación
                self.collected_urls = {}

                try:
                    self._process_search(rubro, localidad)
                    self.logger.info(f"Recolección finalizada para Rubro: {rubro if rubro else 'Por defecto'}, Localidad: {localidad if localidad else 'Por defecto'}.")
                    self.logger.info(f"URLs únicas recolectadas en esta combinación: {len(self.collected_urls)}")

                    # Añadir las URLs recolectadas en esta combinación al acumulado total
                    all_collected_urls.update(self.collected_urls)

                except Exception as e:
                    self.logger.error(f"Error al procesar la combinación (Rubro: {rubro}, Localidad: {localidad}): {e}")
                    # Continuar con la siguiente combinación si hay un error en una

            self.logger.info("="*50)
            self.logger.info(f"Proceso de recolección de todas las combinaciones finalizado.")
            self.logger.info(f"Total de URLs únicas recolectadas en todas las combinaciones: {len(all_collected_urls)}")
            self.logger.info("="*50)

            return all_collected_urls

        except Exception as e:
            self.logger.error(f"Error general durante la gestión de combinaciones: {e}")
            return all_collected_urls # Retornar lo que se haya podido recolectar

        finally:
            self.cleanup()

    def _process_search(self, rubro: Optional[str] = None, localidad: Optional[str] = None):
        """
        Navega a la página de búsqueda (con o sin filtros) y simula clics en 'Ver Más'.
        """
        if not self.driver:
             self.logger.error("Driver no inicializado en _process_search.")
             return

        try:
            self.logger.info("Navegando a la página de búsqueda...")
            self.driver.get(self.config['search_url'])
            self.logger.info(f"Página cargada: {self.config['search_url']}")

            # Si se especificaron filtros, interactuar con la búsqueda avanzada
            if rubro or localidad:
                self._apply_advanced_filters(rubro, localidad)
            else:
                # Si no hay filtros, esperar a que carguen los primeros resultados por defecto
                 self.logger.info("Sin filtros aplicados. Esperando carga inicial de resultados por defecto.")
                 try:
                     WebDriverWait(self.driver, self.config['load_timeout']).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, self.config['business_selector']))
                    )
                     self.logger.info("Resultados iniciales por defecto cargados.")
                 except TimeoutException:
                     self.logger.warning("Tiempo de espera agotado esperando resultados iniciales por defecto. Puede que no haya resultados o la página cargó diferente.")
                 except Exception as e:
                     self.logger.error(f"Error inesperado esperando resultados iniciales por defecto: {e}")


            # Iniciar el bucle de simulación de clics en "Ver Más"
            self._simulate_load_more_clicks()

        except WebDriverException as e:
            self.logger.error(f"Error de WebDriver durante la navegación o proceso de búsqueda: {e}")
            raise # Relanzar para que collect_urls lo gestione
        except Exception as e:
            self.logger.error(f"Error inesperado durante el proceso de búsqueda inicial o aplicación de filtros: {e}")
            raise # Relanzar la excepción para que la gestione collect_urls

    def _apply_advanced_filters(self, rubro: Optional[str], localidad: Optional[str]):
        """
        Interactúa con el formulario de búsqueda avanzada para aplicar filtros.
        Utiliza Select para interactuar con los dropdowns y valida las opciones.
        """
        if not self.driver:
             self.logger.error("Driver no inicializado en _apply_advanced_filters.")
             return # Salir si el driver no está listo

        self.logger.info(f"Aplicando filtros avanzados: Rubro='{rubro}', Localidad='{localidad}'")
        applied_filters_count = 0
        try:
            # 1. Esperar y hacer clic en el botón/enlace de Búsqueda Avanzada para abrir el formulario modal
            self.logger.info(f"Buscando botón de búsqueda avanzada con selector: {BUSQUEDA_AVANZADA_BUTTON_SELECTOR}")
            advanced_button = WebDriverWait(self.driver, self.config['load_timeout']).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, BUSQUEDA_AVANZADA_BUTTON_SELECTOR))
            )
            self.logger.info("Botón/enlace de Búsqueda Avanzada encontrado. Haciendo clic.")
            advanced_button.click()

            # 2. Esperar a que el modal de búsqueda avanzada sea visible
            self.logger.info(f"Esperando modal de búsqueda avanzada con selector: {BUSQUEDA_AVANZADA_MODAL_SELECTOR}")
            WebDriverWait(self.driver, self.config['load_timeout']).until(
                 EC.visibility_of_element_located((By.CSS_SELECTOR, BUSQUEDA_AVANZADA_MODAL_SELECTOR))
            )
            self.logger.info("Modal de Búsqueda Avanzada visible.")


            # 3. Interactuar con el campo de Rubro si se especificó
            if rubro:
                try:
                    self.logger.info(f"Buscando dropdown de Rubro con selector: {RUBRO_SELECT_SELECTOR}")
                    rubro_select_element = WebDriverWait(self.driver, self.config['load_timeout']).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, RUBRO_SELECT_SELECTOR))
                    )
                    rubro_select = Select(rubro_select_element)
                    self.logger.info(f"Dropdown de Rubro encontrado. Buscando opción para: '{rubro}'")

                    rubro_cleaned = re.sub(r'\s*\(.*\)\s*$', '', rubro).strip().lower()
                    found_option_value = None
                    for option in rubro_select.options:
                        option_text_cleaned = re.sub(r'\s*\(.*\)\s*$', '', option.text).strip().lower()
                        if option_text_cleaned == rubro_cleaned:
                            found_option_value = option.get_attribute("value")
                            self.logger.info(f"Coincidencia exacta encontrada para Rubro: '{rubro}' -> '{option.text}' (Valor: {found_option_value})")
                            break

                    if found_option_value is not None:
                        rubro_select.select_by_value(found_option_value)
                        self.logger.info(f"Opción de Rubro seleccionada por valor: {found_option_value}")
                        applied_filters_count += 1
                    else:
                        self.logger.warning(f"No se encontró una opción coincidente para el Rubro: '{rubro}'. Este filtro no se aplicará.")

                except NoSuchElementException:
                    self.logger.error(f"Dropdown de Rubro no encontrado con selector {RUBRO_SELECT_SELECTOR}.")
                except TimeoutException:
                    self.logger.error(f"Tiempo de espera agotado buscando el dropdown de Rubro con selector {RUBRO_SELECT_SELECTOR}.")
                except Exception as e:
                    self.logger.error(f"Error inesperado al interactuar con el dropdown de Rubro para '{rubro}': {e}")


            # 4. Interactuar con el campo de Localidad si se especificó
            if localidad:
                try:
                    self.logger.info(f"Buscando dropdown de Localidad con selector: {LOCALIDAD_SELECT_SELECTOR}")
                    localidad_select_element = WebDriverWait(self.driver, self.config['load_timeout']).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, LOCALIDAD_SELECT_SELECTOR))
                    )
                    localidad_select = Select(localidad_select_element)
                    self.logger.info(f"Dropdown de Localidad encontrado. Buscando opción para: '{localidad}'")

                    localidad_cleaned = re.sub(r'\s*\(.*\)\s*$', '', localidad).strip().lower()
                    found_option_value = None
                    for option in localidad_select.options:
                         option_text_cleaned = re.sub(r'\s*\(.*\)\s*$', '', option.text).strip().lower()
                         if option_text_cleaned == localidad_cleaned:
                             found_option_value = option.get_attribute("value")
                             self.logger.info(f"Coincidencia exacta encontrada para Localidad: '{localidad}' -> '{option.text}' (Valor: {found_option_value})")
                             break

                    if found_option_value is not None:
                        localidad_select.select_by_value(found_option_value)
                        self.logger.info(f"Opción de Localidad seleccionada por valor: {found_option_value}")
                        applied_filters_count += 1
                    else:
                        self.logger.warning(f"No se encontró una opción coincidente para la Localidad: '{localidad}'. Este filtro no se aplicará.")

                except NoSuchElementException:
                    self.logger.error(f"Dropdown de Localidad no encontrado con selector {LOCALIDAD_SELECT_SELECTOR}.")
                except TimeoutException:
                    self.logger.error(f"Tiempo de espera agotado buscando el dropdown de Localidad con selector {LOCALIDAD_SELECT_SELECTOR}.")
                except Exception as e:
                    self.logger.error(f"Error inesperado al interactuar con el dropdown de Localidad para '{localidad}': {e}")


            # 5. Hacer clic en el botón de búsqueda dentro del formulario avanzado
            if applied_filters_count > 0: # Solo hacemos clic si se aplicó al menos un filtro con éxito
                try:
                    self.logger.info(f"Buscando botón de búsqueda avanzada con selector: {ADVANCED_SEARCH_SUBMIT_BUTTON_SELECTOR}")
                    search_button = WebDriverWait(self.driver, self.config['load_timeout']).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, ADVANCED_SEARCH_SUBMIT_BUTTON_SELECTOR))
                    )
                    self.logger.info("Botón de búsqueda avanzada encontrado. Haciendo clic para aplicar filtros.")
                    search_button.click()

                    # Esperar a que el modal desaparezca después de hacer clic en buscar
                    self.logger.info(f"Esperando que el modal {BUSQUEDA_AVANZADA_MODAL_SELECTOR} desaparezca.")
                    WebDriverWait(self.driver, self.config['load_timeout']).until(
                         EC.invisibility_of_element_located((By.CSS_SELECTOR, BUSQUEDA_AVANZADA_MODAL_SELECTOR))
                    )
                    self.logger.info("Modal de Búsqueda Avanzada cerrado.")

                    # Esperar a que los resultados de la búsqueda filtrada carguen en la página principal
                    self.logger.info("Esperando a que carguen los resultados filtrados en la página principal.")
                    try:
                         WebDriverWait(self.driver, self.config['load_timeout']).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, self.config['business_selector']))
                        )
                         self.logger.info("Resultados filtrados cargados.")
                    except TimeoutException:
                         self.logger.warning("Tiempo de espera agotado esperando los resultados filtrados. Puede que no haya resultados para esta combinación.")
                    except Exception as e:
                         self.logger.error(f"Error inesperado esperando resultados filtrados: {e}")


                except NoSuchElementException:
                    self.logger.error(f"Botón de búsqueda avanzada no encontrado con selector {ADVANCED_SEARCH_SUBMIT_BUTTON_SELECTOR}.")
                    # Intentar cerrar el modal si sigue abierto
                    self._close_advanced_search_modal()
                    raise # Relanzar para detener el proceso de esta combinación
                except TimeoutException:
                    self.logger.error(f"Tiempo de espera agotado buscando el botón de búsqueda avanzada con selector {ADVANCED_SEARCH_SUBMIT_BUTTON_SELECTOR} o esperando que el modal desaparezca.")
                    # Intentar cerrar el modal si sigue abierto
                    self._close_advanced_search_modal()
                    raise # Relanzar para detener el proceso de esta combinación
                except Exception as e:
                    self.logger.error(f"Error inesperado al hacer clic en el botón de búsqueda avanzada: {e}")
                    # Intentar cerrar el modal si sigue abierto
                    self._close_advanced_search_modal()
                    raise # Relanzar para detener el proceso de esta combinación

            else:
                 self.logger.warning("No se aplicaron filtros válidos. Procediendo con la búsqueda sin filtros (si aplica).")
                 # Si no se seleccionó nada, el modal puede seguir abierto.
                 # Intentar cerrarlo si está visible para no interferir.
                 self._close_advanced_search_modal()


        except NoSuchElementException:
            self.logger.error(f"Botón/enlace de Búsqueda Avanzada no encontrado con selector {BUSQUEDA_AVANZADA_BUTTON_SELECTOR}.")
            raise # Este es un error crítico para aplicar filtros, relanzar
        except TimeoutException:
            self.logger.error(f"Tiempo de espera agotado buscando el botón/enlace de Búsqueda Avanzada con selector {BUSQUEDA_AVANZADA_BUTTON_SELECTOR} o esperando el modal.")
            raise # Este es un error crítico para aplicar filtros, relanzar
        except Exception as e:
            self.logger.error(f"Error general al intentar abrir el modal de búsqueda avanzada: {e}")
            raise # Relanzar la excepción

    def _close_advanced_search_modal(self):
        """Intenta cerrar el modal de búsqueda avanzada si está visible."""
        try:
            modal_element = self.driver.find_element(By.CSS_SELECTOR, BUSQUEDA_AVANZADA_MODAL_SELECTOR)
            if modal_element.is_displayed():
                 self.logger.info("Intentando cerrar modal de búsqueda avanzada.")
                 # Intentar encontrar un botón de cerrar (ej. con clase 'close') o presionar ESC
                 try:
                     close_button = modal_element.find_element(By.CSS_SELECTOR, '.modal-header .close') # Selector común para botón de cerrar en modales Bootstrap
                     if close_button.is_displayed():
                          close_button.click()
                          self.logger.info("Clic en botón de cerrar modal.")
                     else:
                          # Si el botón de cerrar no está visible, intentar presionar ESC
                          self.driver.find_element(By.TAG_NAME, 'body').send_keys(webdriver.common.keys.Keys.ESCAPE)
                          self.logger.info("Presionando ESC para cerrar modal.")
                 except NoSuchElementException:
                      # Si no se encuentra el botón de cerrar, intentar presionar ESC
                      self.driver.find_element(By.TAG_NAME, 'body').send_keys(webdriver.common.keys.Keys.ESCAPE)
                      self.logger.info("Presionando ESC para cerrar modal (botón no encontrado).")
                 except Exception as e:
                      self.logger.warning(f"Error al intentar interactuar con el botón de cerrar modal o presionar ESC: {e}")

                 # Esperar a que el modal no sea visible
                 WebDriverWait(self.driver, 5).until(
                     EC.invisibility_of_element_located((By.CSS_SELECTOR, BUSQUEDA_AVANZADA_MODAL_SELECTOR))
                 )
                 self.logger.info("Modal de búsqueda avanzada cerrado.")
            else:
                 self.logger.debug("Modal de búsqueda avanzada no visible para cerrar.")
        except NoSuchElementException:
            self.logger.debug("Modal de búsqueda avanzada no encontrado en el DOM para cerrar.")
        except TimeoutException:
             self.logger.warning("Tiempo de espera agotado esperando que el modal de búsqueda avanzada desaparezca.")
        except Exception as e:
            self.logger.error(f"Error inesperado al intentar cerrar el modal de búsqueda avanzada: {e}")


    def _simulate_load_more_clicks(self):
        """
        Simula clics en el botón 'Ver Más' y extrae URLs iterativamente.
        """
        if not self.driver:
             self.logger.error("Driver no inicializado en _simulate_load_more_clicks.")
             return

        page_count = 0
        last_element_count = 0
        consecutive_same_count = 0 # Contador para detectar si no se añaden nuevos elementos

        self.logger.info("Iniciando simulación de clics en 'Ver Más'.")

        while True:
            page_count += 1
            self.logger.info(f"Procesando carga {page_count}...")

            # Extraer URLs de los elementos actualmente visibles en el DOM
            self._extract_urls_from_current_page()
            current_element_count = len(self.collected_urls) # Contar elementos únicos recolectados hasta ahora

            self.logger.info(f"Elementos únicos recolectados hasta ahora: {current_element_count}")

            # Condición de parada 1: Si no se añadieron nuevos elementos en esta carga
            if current_element_count > 0 and page_count > 1 and current_element_count == last_element_count:
                 consecutive_same_count += 1
                 self.logger.warning(f"No se detectaron nuevos elementos únicos en esta carga (consecutivo: {consecutive_same_count}).")
                 if consecutive_same_count >= 3:
                     self.logger.info("Demasiadas cargas sin nuevos elementos. Asumiendo fin de resultados.")
                     break
            elif current_element_count == 0 and page_count > 1:
                 # Si no hay elementos en absoluto después de la primera carga, puede que no haya resultados
                 self.logger.info("No se encontraron elementos en esta carga. Asumiendo fin de resultados o búsqueda sin resultados.")
                 break
            else:
                consecutive_same_count = 0 # Resetear contador si se encuentran nuevos elementos

            last_element_count = current_element_count

            try:
                # Esperar a que el indicador de carga esté oculto antes de buscar el botón
                WebDriverWait(self.driver, self.config['load_timeout']).until(
                     EC.invisibility_of_element_located((By.CSS_SELECTOR, self.config['loading_indicator_selector']))
                )
                # self.logger.debug("Indicador de carga oculto.")

                # Intentar encontrar el botón 'Ver Más' y verificar si es clickeable
                ver_mas_button = WebDriverWait(self.driver, 5).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, self.config['button_selector']))
                )
                # self.logger.debug("Botón 'Ver Más' encontrado y clickeable.")

                # Hacer scroll hasta el botón
                self.driver.execute_script("arguments[0].scrollIntoView(true);", ver_mas_button)
                time.sleep(0.5)

                # Hacer clic en el botón
                ver_mas_button.click()
                self.logger.info("Clic en 'Ver Más' realizado. Esperando nueva carga...")

                # Esperar a que el indicador de carga aparezca y luego desaparezca
                try:
                    WebDriverWait(self.driver, 5).until(
                        EC.visibility_of_element_located((By.CSS_SELECTOR, self.config['loading_indicator_selector']))
                    )
                    # self.logger.debug("Indicador de carga visible.")

                    WebDriverWait(self.driver, self.config['load_timeout']).until(
                        EC.invisibility_of_element_located((By.CSS_SELECTOR, self.config['loading_indicator_selector']))
                    )
                    self.logger.info("Contenido cargado (indicador de carga oculto).")

                except TimeoutException:
                    self.logger.warning("Tiempo de espera agotado esperando el ciclo de carga (indicador). Puede que no haya más contenido o un problema. Terminando bucle de clics.")
                    # Si el indicador no aparece/desaparece, intentar extraer y salir del bucle
                    self._extract_urls_from_current_page()
                    break
                except Exception as e:
                    self.logger.error(f"Error inesperado esperando el ciclo de carga (indicador): {e}. Terminando bucle de clics.")
                    self._extract_urls_from_current_page()
                    break


                # Pausa aleatoria después de la carga exitosa
                time.sleep(random.uniform(1, 2))

            except (NoSuchElementException, ElementNotInteractableException, ElementClickInterceptedException):
                self.logger.info("Botón 'Ver Más' no encontrado o no interactuable. Asumiendo fin de resultados.")
                break
            except TimeoutException:
                self.logger.warning("Tiempo de espera agotado buscando el botón 'Ver Más'. Asumiendo fin de resultados o problema.")
                break
            except StaleElementReferenceException:
                self.logger.warning("StaleElementReferenceException: El botón 'Ver Más' se volvió obsoleto. Reintentando en la próxima iteración.")
                time.sleep(random.uniform(2, 4))
                continue
            except Exception as e:
                self.logger.error(f"Ocurrió un error inesperado durante el bucle de clics: {e}. Terminando la recolección para esta combinación.")
                break

        self.logger.info("Bucle de simulación de clics finalizado.")
        # Asegurarse de extraer los últimos elementos cargados
        self._extract_urls_from_current_page()


    def _extract_urls_from_current_page(self):
        """
        Extrae URLs de negocios del contenido HTML actualmente cargado y las añade al diccionario.
        """
        if not self.driver:
             self.logger.error("Driver no inicializado en _extract_urls_from_current_page.")
             return

        try:
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')

            business_elements = soup.select(self.config['business_selector'])
            # self.logger.debug(f"Encontrados {len(business_elements)} elementos de negocio en el DOM actual para extracción.")

            newly_added_count = 0
            for business_elem in business_elements:
                try: # Añadir try-except alrededor de la extracción de cada elemento por si alguno falla
                    detail_link_tag = business_elem.select_one('a[href*="r=search/detail"]')
                    if detail_link_tag and 'href' in detail_link_tag.attrs:
                        detail_url = detail_link_tag['href']
                        if not detail_url.startswith('http'):
                             detail_url = f"https://www.guiacores.com.ar/{detail_url}"

                        from urllib.parse import urlparse, parse_qs
                        parsed_detail_url = urlparse(detail_url)
                        detail_query_params = parse_qs(parsed_detail_url.query)
                        business_id = detail_query_params.get('id', [None])[0]

                        if business_id:
                            if business_id not in self.collected_urls:
                                self.collected_urls[business_id] = detail_url
                                newly_added_count += 1
                            # else: self.logger.debug(f"ID duplicado encontrado y omitido: {business_id}")
                except Exception as e:
                    self.logger.warning(f"Error al procesar un elemento de negocio en la página actual: {e}. Saltando este elemento.")


            # self.logger.debug(f"Nuevas URLs únicas añadidas al diccionario en esta extracción: {newly_added_count}")

        except Exception as e:
            self.logger.error(f"Error general al extraer URLs de la página actual: {e}")


    def save_urls(self, filename_suffix: str = "") -> Optional[str]:
        """
        Guarda las URLs recolectadas en un archivo JSON con versionado.
        """
        if not self.collected_urls:
            self.logger.warning("No hay URLs para guardar.")
            return None

        try:
            # Crear el nombre base del archivo
            base_filename = f"sequential_urls{f'_{filename_suffix}' if filename_suffix else ''}"
            
            # Versionar los datos usando el versionador
            versioned_path = self.versioner.version_json_file(
                os.path.join(OUTPUT_DIR, f"{base_filename}.json"),
                is_raw=True
            )

            if versioned_path:
                self.logger.info(f"URLs guardadas exitosamente en: {versioned_path}")
                return versioned_path
            else:
                self.logger.error("Error al versionar el archivo de URLs.")
                return None

        except Exception as e:
            self.logger.error(f"Error al guardar las URLs: {e}")
            return None

    def cleanup(self) -> None:
        """Cierra el driver de Selenium al finalizar."""
        if self.driver:
            try:
                self.driver.quit()
                self.logger.info("Driver de Chrome cerrado.")
            except Exception as e:
                self.logger.debug(f"Error al cerrar el driver: {e}")
            self.driver = None

# Ejemplo de uso
if __name__ == "__main__":
    # --- Ejemplo 1: Recolección por defecto (sin filtros) ---
    # logger.info("--- Ejecutando SequentialCollector por defecto ---")
    # collector_default = SequentialCollector()
    # all_urls_default = collector_default.collect_urls()
    # if all_urls_default:
    #     collector_default.save_urls(filename_suffix="_default")
    # else:
    #     logger.warning("No se recolectaron URLs en la ejecución por defecto.")

    # --- Ejemplo 2: Recolección con rubros y localidades específicos ---
    # Simula que el usuario ingresa estos filtros
    # Usar textos que coincidan lo más posible con las opciones del dropdown,
    # sin el conteo entre paréntesis.
    rubros_ejemplo = ["Farmacias", "Supermercados", "Rubro Inexistente"]
    localidades_ejemplo = ["Neuquén", "General Roca", "Localidad Inexistente"]

    logger.info("--- Ejecutando SequentialCollector con filtros ---")
    # El colector procesará secuencialmente cada combinación:
    # (Farmacias, Neuquén), (Farmacias, General Roca), (Farmacias, Localidad Inexistente),
    # (Supermercados, Neuquén), (Supermercados, General Roca), (Supermercados, Localidad Inexistente),
    # (Rubro Inexistente, Neuquén), (Rubro Inexistente, General Roca), (Rubro Inexistente, Localidad Inexistente)
    collector_filtered = SequentialCollector(rubros=rubros_ejemplo, localidades=localidades_ejemplo)
    all_urls_filtered = collector_filtered.collect_urls()

    if all_urls_filtered:
        collector_filtered.save_urls(filename_suffix="_filtered")
    else:
        logger.warning("No se recolectaron URLs en la ejecución con filtros.")

    # --- Ejemplo 3: Recolección solo con rubros ---
    # logger.info("--- Ejecutando SequentialCollector solo con rubros ---")
    # rubros_solo = ["Restaurantes Y Parrillas"] # Usar texto exacto o cercano
    # collector_rubros = SequentialCollector(rubros=rubros_solo)
    # all_urls_rubros = collector_rubros.collect_urls()
    # if all_urls_rubros:
    #      collector_rubros.save_urls(filename_suffix="_rubros")
    # else:
    #      logger.warning("No se recolectaron URLs en la ejecución solo con rubros.")

    # --- Ejemplo 4: Recolección solo con localidades ---
    # logger.info("--- Ejecutando SequentialCollector solo con localidades ---")
    # localidades_solo = ["Cipolletti"]
    # collector_localidades = SequentialCollector(localidades=localidades_solo)
    # all_urls_localidades = collector_localidades.collect_urls()
    # if all_urls_localidades:
    #      collector_localidades.save_urls(filename_suffix="_localidades")
    # else:
    #      logger.warning("No se recolectaron URLs en la ejecución solo con localidades.")

