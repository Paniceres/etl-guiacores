# ETL Guía Cores

Proyecto de extracción, transformación y carga (ETL) de datos de negocios desde Guía Cores.

## Descripción

Este proyecto automatiza la extracción de información de negocios desde [Guía Cores](https://www.guiacores.com.ar), incluyendo:
- Información básica (nombre, teléfono, dirección)
- Información detallada (email, sitio web, redes sociales)
- Información adicional (horarios, servicios, descripción)
- Coordenadas geográficas

## Estructura del Proyecto

```
etl_guiaCores/
├── data/                  # Directorio para archivos de datos
├── html_samples/         # Muestras HTML para pruebas
├── src/                  # Código fuente
│   ├── scraper_guiaCores.py    # Script de extracción
│   ├── cleaner_guiaCores.py    # Script de limpieza
│   └── test_cleaner_guiaCores.py # Tests unitarios
├── requirements.txt      # Dependencias del proyecto
├── Dockerfile           # Configuración de Docker
└── exampleEnv           # Ejemplo de variables de entorno
```

## Requisitos

- Python 3.8+
- PostgreSQL
- Chrome/Chromium (para el scraper)
- Variables de entorno configuradas (ver exampleEnv)

## Instalación

1. Clonar el repositorio:
```bash
git clone [url-del-repositorio]
cd etl_guiaCores
```

2. Crear y activar entorno virtual:
```bash
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# o
.venv\Scripts\activate  # Windows
```

3. Instalar dependencias:
```bash
pip install -r requirements.txt
```

4. Configurar variables de entorno:
```bash
cp exampleEnv .env
# Editar .env con los valores correctos
```

## Uso

1. Ejecutar el scraper:
```bash
python src/scraper_guiaCores.py
```

2. Ejecutar el cleaner:
```bash
python src/cleaner_guiaCores.py
```

3. Ejecutar tests:
```bash
python -m unittest src/test_cleaner_guiaCores.py
```

## Características

### Scraper
- Extracción automática de datos de negocios
- Manejo de paginación
- Extracción de información detallada
- Logging detallado
- Manejo de errores robusto

### Cleaner
- Limpieza y normalización de datos
- Validación de campos
- Deduplicación de registros
- Almacenamiento en PostgreSQL
- Generación de CSV limpio
- Logging detallado

## Base de Datos

El proyecto utiliza PostgreSQL con las siguientes tablas principales:
- `raw_leads`: Almacena los datos crudos extraídos
- `data_sources`: Registra las fuentes de datos

## Docker

Para ejecutar el proyecto en Docker:

```bash
docker build -t etl-guiaCores .
docker run -it --env-file .env etl-guiaCores
```

## Licencia

Este proyecto está bajo la Licencia MIT. Ver el archivo `LICENSE` para más detalles. 
