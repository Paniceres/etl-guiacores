# Servicio ETL y API de Guia Cores

Este proyecto proporciona un robusto pipeline ETL (Extraer, Transformar, Cargar) para procesar datos de Guia Cores y expone una API para activar y gestionar estos procesos ETL. Está diseñado para ser flexible, soportando varios modos de extracción y destinos de salida, y está contenedorizado usando Docker para facilitar su despliegue y escalabilidad, especialmente en entornos Kubernetes.

## Características Principales

*   **Modos ETL Flexibles:**
    *   **Modo Bulk (Masivo):** Extrae datos basados en un rango de IDs.
    *   **Modo Manual:** Extrae datos de una URL específica.
    *   **Modo Sequential (Secuencial):** Extrae datos basados en 'rubros' (categorías).
*   **API RESTful:**
    *   Endpoints para activar cada modo ETL (`/etl/bulk`, `/etl/manual`, `/etl/sequential`).
    *   Construida con FastAPI, proporcionando documentación interactiva automática (vía `/docs`).
*   **Múltiples Opciones de Salida:**
    *   Guardar datos en archivos locales (ej. CSV, JSON - el formato específico depende de la implementación de `FileLoader`).
    *   Cargar datos en una base de datos PostgreSQL.
    *   Opción para enviar la salida tanto a archivo como a base de datos simultáneamente.
*   **Configuración mediante Variables de Entorno:** Gestiona de forma segura las credenciales de la base de datos y otras configuraciones.
*   **Contenedorizado con Docker:** Incluye `Dockerfile` y `docker-compose.yml` para un fácil desarrollo local y despliegue.
*   **Logging Detallado:** Registro completo para los procesos ETL y las interacciones de la API.
*   **Diseñado para Kubernetes:** El enfoque basado en API y la contenedorización lo hacen adecuado para el despliegue en Kubernetes y la integración con orquestadores de flujos de trabajo como Argo Workflows.

## Prerrequisitos

*   Python 3.11+
*   Motor Docker (Docker Engine)
*   Docker Compose (para desarrollo local)
*   Acceso a una instancia de PostgreSQL (local, contenedorizada o en la nube)

## Estructura del Proyecto

```
etl_guiaCores/
├── data/                 # Directorio por defecto para salidas de archivos locales
├── logs/                 # Logs de la aplicación y la API
├── src/                  # Código fuente
│   ├── api/              # Aplicación FastAPI
│   │   └── app.py
│   ├── common/           # Utilidades compartidas (config, conexión db, etc.)
│   ├── extractors/       # Lógica de extracción de datos
│   ├── loaders/          # Lógica de carga de datos (a BD, archivo)
│   ├── transformers/     # Lógica de transformación de datos
│   ├── __init__.py
│   └── main.py           # Funciones ETL principales y punto de entrada CLI
├── .env                  # Variables de entorno (ignorado por git)
├── exampleEnv            # Plantilla para el archivo .env
├── .dockerignore
├── docker-compose.yml    # Configuración de Docker Compose
├── Dockerfile            # Definición de la imagen Docker
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
    Copia el archivo de entorno de ejemplo y personalízalo con tu configuración, especialmente las credenciales de la base de datos.
    ```bash
    cp exampleEnv .env
    nano .env  # O usa tu editor preferido
    ```
    Asegúrate de que `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER` y `DB_PASSWORD` estén configurados correctamente. Para la configuración local con `docker-compose`, `DB_HOST` típicamente debería ser el nombre de tu servicio de base de datos (ej. `db`).

3.  **Construir y Ejecutar con Docker Compose (Recomendado para Desarrollo Local):**
    Esta es la forma más fácil de tener todo el sistema (API, ejecutores ETL y base de datos PostgreSQL) funcionando.
    ```bash
    sudo docker-compose up --build
    ```
    *   La bandera `--build` asegura que la imagen Docker se reconstruya si hay cambios (ej. en `requirements.txt` o el código fuente).
    *   La API estará típicamente disponible en `http://localhost:8000` (o el puerto configurado en `docker-compose.yml` y `src/api/app.py`).
    *   La documentación interactiva de la API (Swagger UI) estará en `http://localhost:8000/docs`.
    *   También se iniciará un servicio PostgreSQL, y los datos se persistirán en un volumen Docker.

## Usando el Sistema

Una vez que el sistema esté funcionando mediante `docker-compose up`, puedes interactuar con él de las siguientes maneras:

### 1. A través de la API REST (Método Principal)

Usa cualquier cliente API (como Postman, curl o un nodo HTTP Request de n8n) para enviar solicitudes POST a los endpoints disponibles. La documentación de la API en `http://localhost:8000/docs` proporciona una lista detallada de endpoints, cuerpos de solicitud esperados y formatos de respuesta.

**Ejemplos de Llamadas API (usando curl):**

*   **Activar ETL Bulk (Masivo):**
    ```bash
    curl -X POST "http://localhost:8000/etl/bulk" \
    -H "Content-Type: application/json" \
    -d '{
      "min_id": 1,
      "max_id": 100,
      "output": "both"
    }'
    ```

*   **Activar ETL Manual:**
    ```bash
    curl -X POST "http://localhost:8000/etl/manual" \
    -H "Content-Type: application/json" \
    -d '{
      "url": "https://www.guiacores.com.ar/alguna/pagina/especifica",
      "output": "database"
    }'
    ```

*   **Activar ETL Sequential (Secuencial):**
    ```bash
    curl -X POST "http://localhost:8000/etl/sequential" \
    -H "Content-Type: application/json" \
    -d '{
      "rubros": "restaurantes,hoteles",
      "output": "file"
    }'
    ```
    (Si `rubros` es `null` o una cadena vacía, podría procesar todos o un conjunto por defecto, dependiendo de tu implementación de `run_sequential_etl`).

### 2. A través de la Línea de Comandos (para Argo Workflows o Ejecución Directa)

El script `src/main.py` aún puede ejecutarse directamente (ej. dentro de un contenedor Docker gestionado por Argo Workflows) si necesitas una interfaz de línea de comandos.

**Ejecutando ETL vía CLI con Docker Compose:**
Esto es útil para tareas puntuales o si Argo gestiona directamente la ejecución de contenedores.

```bash
# Ejemplo: Ejecutar ETL Bulk
sudo docker-compose run --rm etl python src/main.py bulk --start_id 1 --end_id 50 --output database

# Ejemplo: Ejecutar ETL Manual
sudo docker-compose run --rm etl python src/main.py manual --url "https://www.guiacores.com.ar/alguna/pagina" --output file

# Ejemplo: Ejecutar ETL Secuencial
sudo docker-compose run --rm etl python src/main.py sequential --rubros "servicios,tiendas" --output both
```
*   Reemplaza `etl` con el nombre de tu servicio de aplicación en `docker-compose.yml` si es diferente.
*   La bandera `--rm` elimina el contenedor después de la ejecución.

## Logging

*   **Logs de API:** Los logs generados por la aplicación FastAPI se pueden encontrar en `logs/etl_api.log` (si el logging a archivo está configurado así en `setup_logging_if_not_configured` de `src/main.py` y `src/api/app.py` lo llama).
*   **Logs ETL:** Los logs de los procesos ETL también se dirigen típicamente a `logs/etl_api.log` cuando se activan mediante la API, o `data/logs/main.log` si se ejecutan mediante el antiguo punto de entrada CLI en `src/main.py` antes de la refactorización (esto podría necesitar consolidación).
*   **Logs de Docker:** Puedes ver los logs del contenedor usando `sudo docker-compose logs <nombre_del_servicio>` (ej. `sudo docker-compose logs api` o `sudo docker-compose logs db`).

## Desarrollo

1.  Asegúrate de tener Python 3.11+ y haber creado un entorno virtual.
2.  Instala las dependencias: `pip install -r requirements.txt`
3.  Para el desarrollo de la API, puedes ejecutar la aplicación FastAPI directamente usando Uvicorn:
    ```bash
    uvicorn src.api.app:app --reload --host 0.0.0.0 --port 8000
    ```
    Esto habilita la recarga automática cuando cambia el código. Asegúrate de que tu archivo `.env` esté presente en la raíz del proyecto para las conexiones a la base de datos.

## Despliegue a Kubernetes

1.  **Construir y Subir Imagen Docker:** Construye tu imagen Docker y súbela a un registro de contenedores (ej. Docker Hub, GCR, ECR).
    ```bash
    sudo docker build -t tu-registro/etl-guia-cores:latest .
    sudo docker push tu-registro/etl-guia-cores:latest
    ```
2.  **Crear Manifiestos de Kubernetes:**
    *   **Deployment/StatefulSet:** Para la aplicación API y la base de datos PostgreSQL (o usa un servicio de base de datos gestionado en la nube).
    *   **Service:** Para exponer la API interna o externamente (ej. mediante un Ingress).
    *   **ConfigMap/Secret:** Para variables de entorno y credenciales de base de datos.
3.  **Argo Workflows:** Define plantillas de Argo Workflow para orquestar tus trabajos ETL. Estos flujos de trabajo pueden hacer solicitudes HTTP a tu servicio API desplegado o ejecutar Kubernetes Jobs usando tu imagen Docker y los comandos CLI.

## Mejoras Futuras / Consideraciones

*   **Implementar Collectors/Scrapers Específicos:** Asegurar que `ManualCollector`, `SequentialCollector`, y sus correspondientes scrapers estén completamente implementados para los modos ETL manual y secuencial.
*   **Implementar FileLoader:** Asegurar que `FileLoader` en `src/loaders/file_loader.py` esté implementado para manejar el guardado de datos en archivos en el formato deseado (CSV, JSON, etc.).
*   **Idempotencia:** Hacer los trabajos ETL idempotentes siempre que sea posible, especialmente si pudieran reintentarse.
*   **Manejo de Errores y Reintentos:** Mejorar el manejo de errores dentro de los pasos ETL y considerar mecanismos de reintentos más sofisticados (ej. con estrategias de backoff, posiblemente gestionadas por Argo Workflows).
*   **Tareas ETL Asíncronas:** Para trabajos ETL de larga duración activados vía API, considera devolver una respuesta inmediata (ej. un ID de tarea) y ejecutar el proceso ETL de forma asíncrona (ej. usando Celery, `BackgroundTasks` de FastAPI, o dejando que Argo gestione la ejecución del trabajo).
*   **Métricas y Monitorización:** Integrar herramientas de monitorización (ej. Prometheus, Grafana) para rastrear el rendimiento de la API y el estado de los trabajos ETL.
*   **Seguridad:** Asegurar aún más la API (autenticación, autorización) si se expone públicamente.

## Contribuciones

¡Las contribuciones son bienvenidas! Por favor, haz un fork del repositorio, crea una rama para tu funcionalidad y abre un Pull Request.
