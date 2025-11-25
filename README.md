# Guia Cores ETL Service

Este repositorio contiene un pipeline ETL (Extracción, Transformación y Carga) para recopilar, limpiar y exportar información desde Guia Cores. La solución se centra en un núcleo Python mantenible, con interfaz gráfica en Streamlit y una CLI simple para automatizar ejecuciones.

## Características principales

* **Modos ETL**
  * **Bulk**: procesa un rango de IDs numéricos de Guia Cores.
  * **Manual**: permite indicar una URL puntual o una carpeta con HTMLs descargados.
  * **Sequential**: navega rubros y/o localidades para descubrir negocios.
* **Interfaz Streamlit** para usuarios no técnicos.
* **CLI** (`python src/main.py ...`) ideal para scripts o cron jobs.
* **Salida en CSV** dentro de `data/processed`.
* **Configuración centralizada** vía variables de entorno (archivo `.env`).
* **Logging detallado** en `data/logs/etl_api.log` más salida estándar.

## Requisitos

* Python 3.11 o superior.
* Navegador Chrome (o compatible con Selenium) y su WebDriver correspondiente.
* Dependencias listadas en `requirements.txt` (Selenium, Streamlit, BeautifulSoup, etc.).

## Estructura del proyecto

```
etl_guiaCores/
├── data/
│   ├── logs/
│   │   └── etl_api.log
│   └── processed/        # CSV de salida (se crea en la primera ejecución)
├── src/
│   ├── common/           # Config, logger, utilidades compartidas
│   ├── extractors/       # Scrapers y collectors para cada modo
│   ├── loaders/          # Persistencia en archivos
│   ├── transformers/     # Limpieza y normalización
│   └── main.py           # Punto de entrada/CLI
├── streamlit_app.py      # UI en Streamlit
├── requirements.txt
├── exampleEnv            # Plantilla para `.env`
└── README.md
```

## Instalación rápida

```bash
git clone https://github.com/Paniceres/etl_guiaCores.git
cd etl_guiaCores

# Crear entorno virtual (opcional pero recomendado)
python -m venv .venv
# Linux/macOS
source .venv/bin/activate
# Windows
.venv\Scripts\activate

pip install -r requirements.txt
```

## Configuración

1. Copia `exampleEnv` a `.env`.
2. Completa las variables necesarias (credenciales, rutas de ChromeDriver, timeouts, etc.).
3. Verifica que `chromedriver` o el driver elegido esté accesible en el PATH y sea compatible con la versión del navegador.

## Formas de uso

### Interfaz Streamlit

```bash
streamlit run streamlit_app.py
```

1. Elegí el modo (Bulk, Manual o Sequential).
2. Completa los parámetros solicitados (IDs, URL, carpeta HTML, rubros/localidades).
3. Ejecuta y aguarda el estado en pantalla. Los archivos generados aparecerán listados al final dentro de `data/processed`.

### CLI

Ejecutá desde la raíz del repo:

* **Bulk**
  ```bash
  python src/main.py bulk --start_id <inicio> --end_id <fin>
  ```
* **Manual (URL)**
  ```bash
  python src/main.py manual --url "<https://www.guiacores.com/...>"
  ```
* **Manual (carpeta HTML)**
  ```bash
  python src/main.py manual --file "C:\ruta\htmls"
  ```
* **Sequential**
  ```bash
  python src/main.py sequential --rubros "Panaderías,Heladerías" --localidades "Rosario"
  ```

Todos los modos aceptan argumentos adicionales como `--output file` si deseas extender el pipeline (ver `src/main.py`).

## Logs y resultados

* CSVs finales: `data/processed/*.csv`.
* Logs estructurados: `data/logs/etl_api.log`.
* Mensajes en consola/Streamlit informan progreso, errores y cantidad de registros procesados.

## Resolución de problemas

* **El driver de Selenium falla**: asegurate de que la versión del WebDriver coincide con tu navegador y que la ruta está configurada en el `.env`.
* **No se generan archivos**: revisa el log y valida credenciales, URLs y permisos de escritura sobre `data/`.
* **Streamlit no encuentra módulos**: confirma que se ejecuta desde la raíz del proyecto y que `src` está en el `PYTHONPATH` (lo maneja `streamlit_app.py` si corres desde la raíz).

## Contribuciones

Los PRs son bienvenidos. Crea un fork, abrí una rama descriptiva, documentá cambios en el README si impactan al usuario final y enviá el Pull Request.
