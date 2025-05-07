# ETL Guía Cores

Sistema ETL para procesar datos de la [Guía Cores](https://www.guiacores.com.ar), con soporte para extracción masiva, secuencial y manual.

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

## Ciclo de Vida de los Datos

### 1. Modo Masivo (Bulk)

Este modo está diseñado para procesar grandes cantidades de datos de una vez:

1. **Extracción**:
   - Genera URLs para un rango de IDs
   - Procesa cada URL en paralelo
   - Almacena resultados temporalmente

2. **Transformación** (automática):
   - Normaliza URLs
   - Limpia datos de comercios
   - Estructura información

3. **Carga**:
   - Guarda en base de datos
   - Genera archivos CSV/JSON
   - Mantiene versiones mensuales

### 2. Modo Secuencial

Ideal para actualizaciones incrementales por rubro y localidad:

1. **Extracción**:
   - Navega por categorías
   - Procesa resultados paginados
   - Maneja carga dinámica

2. **Transformación** (automática):
   - Valida datos por rubro
   - Normaliza ubicaciones
   - Actualiza relaciones

3. **Carga**:
   - Actualiza registros existentes
   - Inserta nuevos registros
   - Mantiene historial de cambios

### 3. Modo Manual

Para procesar datos específicos de dos formas:

#### A. Usando archivo HTML guardado:

1. **Preparación**:
   - Realizar búsqueda en [Guía Cores](https://www.guiacores.com.ar)
   - Hacer clic en "Ver más" hasta cargar todos los resultados
   - Guardar página completa como HTML
   - Colocar archivo en `data/html_samples/`

2. **Procesamiento**:
```bash
python main.py --mode manual --html data/html_samples/mi_busqueda.html --output both
```

#### B. Usando URL de búsqueda:

1. **Preparación**:
   - Realizar búsqueda avanzada en [Guía Cores](https://www.guiacores.com.ar)
   - Copiar URL de resultados
   - Ejecutar con la URL

2. **Procesamiento**:
```bash
python main.py --mode manual --url "https://www.guiacores.com.ar/index.php?r=search%2Findex&b=&R=&L=10&Tm=1" --output both
```

## Transformaciones Automáticas

El sistema aplica automáticamente las siguientes transformaciones:

1. **URLs**:
   - Normalización de formatos
   - Validación de enlaces
   - Extracción de parámetros

2. **Datos de Comercios**:
   - Limpieza de nombres
   - Normalización de direcciones
   - Formateo de teléfonos
   - Validación de emails

3. **Categorías y Ubicaciones**:
   - Normalización de rubros
   - Geocodificación de direcciones
   - Validación de localidades

## Uso

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
# Usando archivo HTML
python main.py --mode manual --html data/html_samples/mi_busqueda.html --output both

# Usando URL
python main.py --mode manual --url "https://www.guiacores.com.ar/detalle/123" --output both
```

Opciones:
- `--html`: Ruta al archivo HTML guardado
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
