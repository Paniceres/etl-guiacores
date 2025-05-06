# ETL Guía Cores

Sistema ETL para procesar datos de la Guía Cores, con soporte para extracción masiva, secuencial y manual.

## Estructura del Proyecto

```
etl_guiaCores/
├── src/
│   ├── 0_common/           # Módulos comunes y utilidades
│   ├── 1_extractors/       # Extractores de datos
│   │   ├── bulk/          # Extracción masiva
│   │   ├── sequential/    # Extracción secuencial
│   │   └── manual/        # Extracción manual
│   ├── 2_transformers/    # Transformadores de datos
│   └── 3_loaders/         # Cargadores de datos
├── tests/                 # Tests unitarios
├── data/                  # Datos procesados
│   ├── json/             # Versiones JSON
│   └── csv/              # Versiones CSV
└── main.py               # Punto de entrada principal
```

## Requisitos

- Python 3.8+
- PostgreSQL 12+
- Chrome/Chromium (para web scraping)

## Instalación

1. Clonar el repositorio:
```bash
git clone https://github.com/tu-usuario/etl_guiaCores.git
cd etl_guiaCores
```

2. Crear y activar entorno virtual:
```bash
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# o
.venv\Scripts\activate     # Windows
```

3. Instalar dependencias:
```bash
pip install -r requirements.txt
```

4. Configurar variables de entorno:
```bash
cp exampleEnv .env
# Editar .env con tus configuraciones
```

## Uso

La aplicación se ejecuta a través de `main.py` y soporta diferentes modos de operación:

### 1. Modo Masivo (Bulk)

Para procesar grandes cantidades de datos de una vez:

```bash
python main.py --mode bulk --start-id 1 --end-id 1000 --output database
```

Opciones:
- `--start-id`: ID inicial (default: 1)
- `--end-id`: ID final (default: 1000)
- `--output`: Tipo de salida (database/file/both)

### 2. Modo Secuencial

Para procesar datos por rubro y localidad:

```bash
python main.py --mode sequential --rubros "Farmacias,Supermercados" --localidades "Neuquén,Cipolletti" --output file
```

Opciones:
- `--rubros`: Lista de rubros separados por coma
- `--localidades`: Lista de localidades separadas por coma
- `--output`: Tipo de salida (database/file/both)

### 3. Modo Manual

Para procesar datos de una página específica:

```bash
python main.py --mode manual --url "https://guiacores.com/detalle/123" --output both
```

Opciones:
- `--url`: URL de la página a procesar
- `--output`: Tipo de salida (database/file/both)

### Opciones Generales

Todos los modos soportan estas opciones adicionales:

- `--output`: Tipo de salida
  - `database`: Guarda en PostgreSQL
  - `file`: Guarda en CSV/JSON
  - `both`: Guarda en ambos
- `--version`: Versión de los datos (default: YYYY-MM)
- `--log-level`: Nivel de logging (DEBUG/INFO/WARNING/ERROR)

### Ejemplos de Uso

1. Procesar todos los comercios de Neuquén:
```bash
python main.py --mode sequential --localidades "Neuquén" --output both
```

2. Actualizar datos de farmacias:
```bash
python main.py --mode sequential --rubros "Farmacias" --output database
```

3. Procesar un rango específico de IDs:
```bash
python main.py --mode bulk --start-id 1000 --end-id 2000 --output file
```

4. Procesar una página específica:
```bash
python main.py --mode manual --url "https://guiacores.com/detalle/123" --output both
```

## Estructura de Datos

### Base de Datos

Los datos se almacenan en las siguientes tablas:

- `businesses`: Información de comercios
- `locations`: Ubicaciones
- `categories`: Categorías/rubros
- `business_categories`: Relación comercio-categoría

### Archivos

Los datos se guardan en:

- `data/json/YYYY-MM/businesses.json`: Datos en formato JSON
- `data/csv/YYYY-MM/businesses.csv`: Datos en formato CSV

## Tests

Ejecutar los tests:

```bash
python -m unittest discover tests
```

## Docker

Construir y ejecutar con Docker:

```bash
docker build -t etl_guiaCores .
docker run -it --env-file .env etl_guiaCores
```

## Contribuir

1. Fork el repositorio
2. Crear una rama para tu feature (`git checkout -b feature/AmazingFeature`)
3. Commit tus cambios (`git commit -m 'Add some AmazingFeature'`)
4. Push a la rama (`git push origin feature/AmazingFeature`)
5. Abrir un Pull Request

## Licencia

Este proyecto está bajo la Licencia MIT - ver el archivo [LICENSE](LICENSE) para más detalles. 
