# Directorio `src` - Lógica Principal del ETL

Este directorio contiene el código fuente principal para el proceso ETL (Extracción, Transformación y Carga) diseñado para obtener información de Guia Cores. La lógica está organizada en módulos que representan las diferentes etapas del pipeline ETL y las funcionalidades comunes.

## Estructura del Directorio

```
src/
├── api/                      # Código relacionado con la API (si aplica)
├── common/                   # Módulos de utilidad compartidos
│   ├── __init__.py
│   ├── base.py               # Clases base o utilidades comunes (si existen)
│   ├── config.py             # Carga y gestión de la configuración
│   ├── logger.py             # Configuración centralizada de logging
│   ├── utils.py              # Funciones de utilidad general
│   └── versioning.py         # Lógica para versionado de datos/archivos
├── extractors/               # Módulos de Extracción (Collectors y Scrapers)
│   ├── __init__.py
│   ├── bulk_collector.py     # Colector para el modo Bulk
│   ├── bulk_scraper.py       # Scraper para el modo Bulk
│   ├── manual_collector.py   # Colector para el modo Manual (si aplica)
│   ├── manual_scraper.py   # Scraper para el modo Manual
│   ├── sequential_collector.py # Colector para el modo Sequential
│   └── sequential_scraper.py # Scraper para el modo Sequential
├── loaders/                  # Módulos de Carga
│   ├── __init__.py
│   ├── cache_loader.py       # Cargador a caché (si aplica)
│   └── file_loader.py        # Cargador a archivos locales (CSV, etc.)
├── transformers/             # Módulos de Transformación
│   ├── __init__.py
│   ├── business_transformer.py # Lógica de transformación de negocio
│   ├── data_cleaner.py       # Lógica de limpieza de datos (si es separada)
│   └── url_transformer.py    # Transformación relacionada con URLs (si aplica)
├── ui/                       # Código relacionado con la UI (si aplica)
├── __init__.py               # Permite tratar src como un paquete Python
└── main.py                   # Punto de entrada principal y orquestador
```

## Proceso ETL General

El proceso ETL sigue un flujo estándar de **Extracción (E)**, **Transformación (T)** y **Carga (L)**. La ejecución es orquestada principalmente por el archivo `main.py`, que selecciona el flujo adecuado según el modo de ejecución (Bulk, Manual o Sequential). Los datos transformados son guardados en archivos locales (CSV).

### Módulos Clave y su Rol:

1.  **`common/config.py`**: Este módulo es fundamental al inicio de cualquier ejecución ETL. Se encarga de cargar la configuración de la aplicación, típicamente desde variables de entorno o un archivo `.env`. Proporciona acceso a parámetros como rutas de archivos, tamaños de chunk, etc.

2.  **Módulos de Extracción (`extractors/`)**:
    *   Aquí residen los componentes encargados de obtener los datos brutos de la fuente (Guia Cores). Se dividen en **Collectors** y **Scrapers**.
    *   **Collectors** (`*_collector.py`): Son responsables de identificar las URLs o los identificadores de los datos que se deben extraer. Por ejemplo, `bulk_collector.py` genera URLs basándose en un rango de IDs, mientras que `sequential_collector.py` podría navegar por categorías o localidades para encontrar URLs.
    *   **Scrapers** (`*_scraper.py`): Se encargan de visitar las fuentes de datos (URLs) y extraer la información relevante de su contenido (usando herramientas como Selenium para interactuar con la página y BeautifulSoup para parsear el HTML). `bulk_scraper.py` y `sequential_scraper.py` están diseñados para hacer esto de forma paralela para mejorar el rendimiento, mientras que `manual_scraper.py` puede procesar una única URL o contenido HTML local.

3.  **Módulos de Transformación (`transformers/`)**:
    *   Una vez extraídos los datos brutos, los módulos de transformación se encargan de limpiarlos, enriquecerlos, reestructurarlos y validarlos para prepararlos para la carga.
    *   `business_transformer.py`: Contiene la lógica central de transformación que aplica reglas de negocio para dar forma final a los datos antes de ser guardados.

4.  **Módulos de Carga (`loaders/`)**:
    *   Estos módulos son responsables de tomar los datos transformados y persistirlos en uno o varios destinos.
    *   `file_loader.py`: Se encarga de guardar los datos en archivos locales, como CSV, JSON Lines, etc., en rutas especificadas.

5.  **Módulos Comunes (`common/`)**:
    *   Este directorio agrupa utilidades y funcionalidades que son transversales a las diferentes etapas del ETL.
    *   Incluye la configuración (`config.py`), la configuración del sistema de logging (`logger.py`), funciones de ayuda generales (`utils.py`), y lógica para versionado de datos/archivos (`versioning.py`).

6.  **`main.py`**:
    *   Este es el punto de entrada principal cuando se ejecuta el ETL desde la línea de comandos o se llama desde la API.
    *   Contiene la lógica de orquestación: parsea los argumentos de entrada (modo de ejecución, parámetros específicos), inicializa los módulos necesarios (Collector, Scraper, Transformer, Loaders), y coordina la ejecución secuencial de las etapas E, T y L para el modo seleccionado.
    *   Define las funciones `run_bulk_etl`, `process_manual_input` y `run_sequential_etl` para encapsular la lógica de cada modo.

## Orden de Ejecución (Ejemplo para Modo Bulk)

Aunque `main.py` orquesta el flujo, internamente, para un modo como "Bulk", la secuencia típica de uso de los módulos es:

1.  `main.py` inicia el proceso llamando a `run_bulk_etl`.
2.  `run_bulk_etl` lee la configuración usando `common/config.py`.
3.  `run_bulk_etl` instancia `extractors/bulk_collector.py`.
4.  Llama a `bulk_collector.collect_urls` para obtener la lista de URLs y sus chunks.
5.  `run_bulk_etl` instancia `extractors/bulk_scraper.py`.
6.  Llama a `bulk_scraper.scrape_urls`, pasándole la lista de URLs (el scraper gestiona los chunks internamente usando ProcessPoolExecutor y workers que usan `_setup_driver`, `_extract_business_info` y las funciones de parsing).
7.  `bulk_scraper` devuelve la lista de datos scrapeados.
8.  `run_bulk_etl` instancia `transformers/business_transformer.py`.
9.  Llama a `business_transformer.transform` con los datos scrapeados.
10. `business_transformer` devuelve los datos transformados.
11. `run_bulk_etl` determina los loaders necesarios (`loaders/file_loader.py`) usando `_get_loaders`.
12. Itera sobre la lista de loaders y llama a `loader.load` para cada uno, guardando los datos transformados en archivos.
13. El proceso finaliza.

Los modos Manual y Sequential seguirían un flujo similar, pero utilizando sus respectivos collectors y scrapers (`manual_scraper.py`, `sequential_collector.py`, `sequential_scraper.py`). Las utilidades en `common/` son accedidas según sea necesario por los otros módulos a lo largo de todo el proceso.

## Orden de Ejecución (Ejemplo para Modo Sequential)

El flujo para el modo "Sequential" se centra en la extracción de datos basados en criterios de búsqueda (rubros y/o localidades) navegando el sitio web, y está diseñado para procesar las URLs encontradas de forma paralela.

1.  `main.py` inicia el proceso llamando a `run_sequential_etl`, pasando los `rubros` y `localidades` especificados.
2.  `run_sequential_etl` lee la configuración usando `common/config.py`.
3.  `run_sequential_etl` instancia `extractors/sequential_collector.py`, pasándole los criterios de búsqueda.
4.  Llama a `sequential_collector.collect_urls`. Este collector utiliza Selenium para navegar por el sitio, realizar búsquedas por rubro y/o localidad, e identificar las URLs de las páginas de detalle de los negocios encontrados.
5.  Una vez recolectadas las URLs, se llama a `collector.cleanup()` para cerrar el driver de Selenium utilizado por el collector.
6.  Las URLs recolectadas se preparan para ser procesadas por el scraper.
7.  `run_sequential_etl` está diseñado para utilizar `concurrent.futures.ProcessPoolExecutor` para procesar las URLs en paralelo. Divide las URLs en chunks y distribuye estos chunks a múltiples procesos worker.
8.  Cada proceso worker instancia `extractors/sequential_scraper.py` (específicamente, usa lógica que podría residir en `GuiaCoresScraper` y una función de procesamiento de chunks como `process_url_chunk_for_sequential`) para visitar las URLs asignadas a su chunk y extraer los datos relevantes de cada página de detalle. Cada worker gestiona su propia instancia de Selenium driver (`_setup_driver`).
9.  Los datos scrapeados de todos los procesos worker se recolectan y consolidan en una lista única.
10. `run_sequential_etl` instancia `transformers/business_transformer.py`.
11. Llama a `business_transformer.transform` con los datos scrapeados consolidados.
12. `business_transformer` devuelve los datos transformados.
13. `run_sequential_etl` determina los loaders necesarios (`loaders/file_loader.py`) usando `_get_loaders`.
14. Itera sobre la lista de loaders y llama a `loader.load` para cada uno, guardando los datos transformados en archivos.
15. El proceso finaliza, incluyendo una limpieza de emergencia del collector si ocurrió algún error antes de su limpieza normal.
