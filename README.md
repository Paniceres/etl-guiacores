# Servicio ETL y API de Guia Cores

Este proyecto proporciona un robusto pipeline ETL (Extraer, Transformar, Cargar) para procesar datos de Guia Cores y expone una API RESTful para activar y gestionar estos procesos ETL. Está diseñado para ser flexible, soportando varios modos de extracción, procesamiento paralelo y múltiples destinos de salida. El sistema está contenedorizado usando Docker para facilitar su despliegue y escalabilidad, especialmente en entornos Kubernetes integrado con orquestadores como Argo Workflows.

## Características Principales

*   **Modos ETL Flexibles:**
    *   **Modo Bulk (Masivo):** Extrae datos basados en un rango de IDs de negocio.
    *   **Modo Manual:** Extrae datos de una URL específica de Guia Cores.
    *   **Modo Sequential (Secuencial):** Extrae datos basados en 'rubros' (categorías) y/o 'localidades'. Este modo está diseñado para realizar el scraping de las URLs recolectadas en paralelo, utilizando `concurrent.futures.ProcessPoolExecutor` para mejorar significativamente el rendimiento en grandes volúmenes de datos.
*   **API RESTful con FastAPI:**
    *   Endpoints dedicados para activar cada modo ETL (`/etl/bulk`, `/etl/manual`, `/etl/sequential`), aceptando parámetros relevantes vía JSON.
    *   Documentación interactiva de la API generada automáticamente (Swagger UI en `/docs` y ReDoc en `/redoc`).
    *   Validación de datos de entrada utilizando modelos Pydantic.
    *   Endpoint de chequeo de salud (`/health`).
*   **Múltiples Opciones de Salida:**
    *   Guardar datos procesados en archivos locales (ej. JSON, a través de `FileLoader`).
    *   Cargar datos en una base de datos PostgreSQL (a través de `DatabaseLoader`).
    *   Opción para utilizar ambas salidas simultáneamente ("both").
*   **Configuración Centralizada:** Gestión de la configuración de la aplicación, incluyendo credenciales de base de datos y parámetros de ejecución, mediante variables de entorno (archivo `.env`).
*   **Contenerización con Docker:** Incluye `Dockerfile` y `docker-compose.yml` para desarrollo local simplificado y despliegues consistentes.
*   **Logging Detallado:** Registro integral de eventos para los procesos ETL y las interacciones de la API, facilitando el seguimiento y la depuración. Los logs se guardan en `data/logs/etl_api.log` y se emiten a la consola.
*   **Diseñado para Orquestación y Kubernetes:** El enfoque basado en API y la contenerización lo hacen ideal para el despliegue en Kubernetes y la integración con herramientas de orquestación de flujos de trabajo como Argo Workflows.

## Prerrequisitos

*   Python 3.11+
*   Motor Docker (Docker Engine)
*   Docker Compose (para desarrollo local y ejecución de servicios)
*   Acceso a una instancia de PostgreSQL (puede ser local, contenedorizada como parte del `docker-compose.yml`, o un servicio en la nube).

## Estructura del Proyecto

```
etl_guiaCores/
├── data/                 # Directorio para salidas de archivos locales y logs
│   └── logs/
│       └── etl_api.log
├── src/                  # Código fuente de la aplicación
│   ├── api/              # Lógica de la aplicación FastAPI (endpoints, modelos)
│   │   └── app.py
│   ├── common/           # Módulos comunes (config, db, logger, utils)
│   ├── extractors/       # Colectores y scrapers para diferentes modos ETL
│   ├── loaders/          # Loaders para diferentes destinos (BD, archivo)
│   ├── transformers/     # Transformadores de datos
│   ├── __init__.py
│   └── main.py           # Funciones ETL principales y punto de entrada CLI
├── tests/                # Pruebas unitarias e de integración
├── .env                  # Variables de entorno (ignorado por git, usar exampleEnv como plantilla)
├── exampleEnv            # Plantilla para el archivo .env
├── .dockerignore         # Archivos a ignorar por Docker
├── docker-compose.yml    # Definición de servicios para Docker Compose (API, ETL, DB)
├── Dockerfile            # Instrucciones para construir la imagen Docker de la aplicación
├── requirements.txt      # Dependencias de Python
└── README.md             # Este archivo
```

## Configuración e Instalación

1.  **Clonar el Repositorio:**
    ```bash
    git clone https://github.com/tu-usuario/etl_guiaCores.git # Reemplaza con la URL de tu repo
    cd etl_guiaCores
    ```

2.  **Configurar Variables de Entorno:**
    Copia el archivo de plantilla `exampleEnv` a `.env` y personalízalo con tu configuración, especialmente las credenciales de la base de datos PostgreSQL.
    ```bash
    cp exampleEnv .env
    nano .env  # O usa tu editor preferido (ej. vim, code)
    ```
    Asegúrate de que `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, y `DB_PASSWORD` estén correctamente configurados. Si usas la base de datos del `docker-compose.yml`, `DB_HOST` debe ser el nombre del servicio de la base de datos (por defecto, `db`).

3.  **Construir y Ejecutar con Docker Compose (Recomendado para Desarrollo y Producción Local):**
    Este comando iniciará la API, la base de datos PostgreSQL (si está definida en `docker-compose.yml`) y cualquier otro servicio.
    ```bash
    sudo docker-compose up --build
    ```
    *   La bandera `--build` fuerza la reconstrucción de las imágenes Docker si hay cambios en el `Dockerfile` o el código fuente.
    *   La API estará disponible en `http://localhost:8000` (o el puerto que hayas configurado).
    *   La documentación interactiva de la API (Swagger UI) se encontrará en `http://localhost:8000/docs`.

## Usando el Sistema

Una vez que los servicios estén en ejecución (vía `docker-compose up`), puedes interactuar con el sistema ETL de las siguientes maneras:

### 1. A través de la API REST (Método Principal)

Este es el método preferido para la interacción programática y la integración con otros sistemas. Utiliza cualquier cliente HTTP (Postman, Insomnia, curl, Python `requests`, etc.) para enviar solicitudes `POST` a los endpoints ETL.

**Endpoints Principales:**

*   `POST /etl/bulk`: Activa el ETL en modo masivo.
    *   Cuerpo JSON esperado: `{"min_id": <int>, "max_id": <int>, "output": "<file|database|both>"}`
*   `POST /etl/manual`: Activa el ETL para una URL específica.
    *   Cuerpo JSON esperado: `{"url": "<string_url>", "output": "<file|database|both>"}`
*   `POST /etl/sequential`: Activa el ETL en modo secuencial por rubros y/o localidades.
    *   Cuerpo JSON esperado: `{"rubros": "<rubro1,rubro2|null>", "localidades": "<loc1,loc2|null>", "output": "<file|database|both>"}` (las cadenas de rubros/localidades son listas separadas por comas).

**Ejemplos con `curl`:**

*   **ETL Bulk:**
    ```bash
    curl -X POST "http://localhost:8000/etl/bulk" \
    -H "Content-Type: application/json" \
    -d '{
      "min_id": 1,
      "max_id": 50,
      "output": "both"
    }'
    ```

*   **ETL Manual:**
    ```bash
    curl -X POST "http://localhost:8000/etl/manual" \
    -H "Content-Type: application/json" \
    -d '{
      "url": "https://www.guiacores.com.ar/empresas/12345/nombre-empresa",
      "output": "database"
    }'
    ```

*   **ETL Sequential (por rubros):**
    ```bash
    curl -X POST "http://localhost:8000/etl/sequential" \
    -H "Content-Type: application/json" \
    -d '{
      "rubros": "restaurantes,hoteles",
      "localidades": null,
      "output": "file"
    }'
    ```

### 2. Flujo de Trabajo del Modo Sequential (Detalle)

El modo sequential está diseñado para una extracción más específica y potencialmente extensa basada en categorías de negocios (rubros) y, opcionalmente, localidades.

1.  **Recolección de URLs:** El `SequentialCollector` primero navega Guia Cores para identificar las URLs de los negocios que coinciden con los rubros/localidades especificados. Utiliza Selenium para esta fase.
2.  **Scraping Paralelo:** Una vez recolectadas las URLs, la función `run_sequential_etl` en `src/main.py` está diseñada para distribuir estas URLs entre múltiples procesos trabajadores usando `concurrent.futures.ProcessPoolExecutor`. Cada proceso trabajador utiliza una instancia de `GuiaCoresScraper` (a través de la función `process_url_chunk_for_sequential` de `src/extractors/sequential_scraper.py`) para realizar el scraping de un subconjunto de las URLs. Este enfoque en paralelo acelera drásticamente la fase de scraping.
3.  **Transformación y Carga:** Los datos scrapeados de todos los procesos se consolidan, se transforman y luego se cargan al destino especificado (archivo y/o base de datos).

### 3. A través de la Línea de Comandos (CLI) con `docker-compose run`

Para ejecuciones ad-hoc, depuración, o si se integra con sistemas como Argo Workflows que pueden ejecutar comandos de Docker directamente, se puede invocar `src/main.py`.

**Importante:** Cuando se utiliza `docker-compose run`, el comando a ejecutar dentro del contenedor debe seguir la estructura de `python src/main.py <modo> [argumentos_del_modo]`. Los argumentos disponibles para cada modo se definen en el `ArgumentParser` dentro de `src/main.py`.

**Ejemplos:**

```bash
# Ejecutar ETL Bulk vía CLI:
sudo docker-compose run --rm etl python src/main.py bulk --start_id 1 --end_id 20 --output database

# Ejecutar ETL Manual vía CLI:
sudo docker-compose run --rm etl python src/main.py manual --url "https://www.guiacores.com.ar/negocio/ejemplo" --output file

# Ejecutar ETL Secuencial vía CLI (por rubros y localidades):
sudo docker-compose run --rm etl python src/main.py sequential --rubros "panaderias,veterinarias" --localidades "rosario" --output both
```
*   `etl` es el nombre del servicio de aplicación definido en `docker-compose.yml` (puede variar según tu configuración).
*   `--rm` elimina el contenedor después de que el comando finaliza.

## Logging

*   **Logs de la Aplicación (API y ETL):** Todos los logs relevantes, tanto de la API FastAPI como de los procesos ETL que esta dispara, se consolidan en `data/logs/etl_api.log` dentro del contenedor. También se muestran en la salida estándar de Docker.
*   **Visualización de Logs con Docker:** Para ver los logs en tiempo real de un servicio:
    ```bash
    sudo docker-compose logs -f api  # Para el servicio de la API/ETL
    sudo docker-compose logs -f db   # Para el servicio de la base de datos (si aplica)
    ```

## Desarrollo

1.  Asegúrate de tener Python 3.11+ y haber configurado un entorno virtual (ej. `python -m venv .venv && source .venv/bin/activate`).
2.  Instala las dependencias: `pip install -r requirements.txt`.
3.  Crea y configura tu archivo `.env` como se describió anteriormente.
4.  Para el desarrollo de la API, puedes ejecutar la aplicación FastAPI directamente con Uvicorn (esto permite recarga automática en cambios de código):
    ```bash
    uvicorn src.api.app:app --reload --host 0.0.0.0 --port 8000
    ```
    La API estará accesible en `http://localhost:8000`.

## Despliegue a Kubernetes

1.  **Construir y Publicar Imagen Docker:**
    Construye tu imagen Docker y publícala en un registro de contenedores accesible por tu clúster Kubernetes (ej. Docker Hub, Google Container Registry (GCR), Amazon Elastic Container Registry (ECR)).
    ```bash
    sudo docker build -t tu-registro/etl-guia-cores:latest .
    sudo docker push tu-registro/etl-guia-cores:latest
    ```
2.  **Crear Manifiestos de Kubernetes:**
    Define los recursos de Kubernetes necesarios:
    *   `Deployment` o `StatefulSet`: Para ejecutar la aplicación API.
    *   `Service`: Para exponer la API dentro del clúster o externamente (ej. con un `LoadBalancer` o `Ingress`).
    *   `ConfigMap` y `Secret`: Para gestionar la configuración de la aplicación y las credenciales de la base de datos de forma segura.
    *   `PersistentVolume` y `PersistentVolumeClaim`: Si necesitas persistencia para los datos de la base de datos PostgreSQL (si la despliegas en Kubernetes) o para los archivos de log/salida.
3.  **Integración con Argo Workflows (Opcional):**
    *   Define `WorkflowTemplates` o `Workflows` en Argo para orquestar tus pipelines ETL.
    *   Estos flujos de trabajo pueden consistir en pasos que:
        *   Realizan llamadas HTTP a los endpoints de tu API ETL desplegada.
        *   Ejecutan `Kubernetes Jobs` o `Pods` usando tu imagen Docker, invocando `src/main.py` con los argumentos CLI apropiados para cada modo ETL.

## Mejoras Futuras / Consideraciones

*   **Implementación Completa de `FileLoader`:** Detallar y robustecer `FileLoader` para manejar diferentes formatos de archivo (CSV, JSON Lines, Parquet) y configuraciones de salida.
*   **Refinamiento del Scraping Paralelo en Modo Sequential:** Asegurar que la implementación de `ProcessPoolExecutor` con `GuiaCoresScraper` en `run_sequential_etl` sea robusta, maneje errores por proceso, y gestione eficientemente los recursos (drivers de Selenium, etc.).
*   **Idempotencia en Operaciones ETL:** Diseñar los pasos de carga para que sean idempotentes, evitando duplicados o estados incorrectos si un trabajo se reintenta.
*   **Manejo Avanzado de Errores y Reintentos:** Implementar estrategias de reintentos más sofisticadas (ej. con backoff exponencial) para operaciones de red o scraping, tanto en la API como en los trabajos CLI.
*   **Tareas Asíncronas para la API:** Para procesos ETL de larga duración iniciados vía API, considerar un patrón de respuesta inmediata (con un ID de tarea) y ejecución asíncrona del ETL (ej. usando `BackgroundTasks` de FastAPI para tareas simples, o Celery/RQ para sistemas más complejos).
*   **Métricas y Monitorización:** Integrar Prometheus y Grafana para monitorizar el rendimiento de la API, la duración de los trabajos ETL, tasas de error, y el uso de recursos.
*   **Seguridad de la API:** Implementar autenticación (ej. OAuth2, API Keys) y autorización para proteger los endpoints de la API si se exponen fuera de un entorno controlado.
*   **Pruebas Exhaustivas:** Ampliar la cobertura de pruebas unitarias y de integración, especialmente para los flujos ETL y la lógica de scraping.

## Contribuciones

Las contribuciones son bienvenidas. Por favor, realiza un fork del repositorio, crea una rama para tu nueva funcionalidad o corrección (`git checkout -b nombre-feature`), y envía un Pull Request detallando tus cambios.
