# ETL Guía Cores

Proyecto ETL para extraer, transformar y cargar datos de Guía Cores.

## 🚀 Características

- Extracción de datos mediante web scraping
- Soporte para múltiples modos de operación:
  - Manual: Extracción de una URL específica
  - Secuencial: Extracción por rubros
  - Bulk: Extracción masiva por rangos de IDs
- Almacenamiento flexible:
  - Archivos locales (CSV, JSON)
  - Base de datos PostgreSQL
- Configuración mediante variables de entorno
- Logging detallado
- Manejo de errores y reintentos
- Dockerizado para fácil despliegue

## 📋 Prerrequisitos

- Python 3.11+
- Docker y Docker Compose (opcional, para ejecución en contenedor)
- PostgreSQL (opcional, para almacenamiento en base de datos)

## 🔧 Instalación

### Instalación de Docker y Docker Compose

1. Instalar Docker:
```bash
# Para Arch Linux (Manjaro)
sudo pacman -S docker

# Para Ubuntu/Debian
sudo apt-get update
sudo apt-get install docker.io

# Para Fedora
sudo dnf install docker
```

2. Iniciar y habilitar el servicio de Docker:
```bash
sudo systemctl start docker
sudo systemctl enable docker
```

3. Agregar tu usuario al grupo docker:
```bash
sudo usermod -aG docker $USER
# Cerrar sesión y volver a iniciar para que los cambios surtan efecto
```

4. Instalar Docker Compose:
```bash
# Para Arch Linux (Manjaro)
sudo pacman -S docker-compose

# Para Ubuntu/Debian
sudo apt-get install docker-compose

# Para Fedora
sudo dnf install docker-compose
```

5. Verificar la instalación:
```bash
docker --version
docker-compose --version
```

### Instalación Local del Proyecto

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
.venv\Scripts\activate  # Windows
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

### Instalación con Docker

1. Construir la imagen:
```bash
./build_and_run.sh
```

## 🚀 Uso

### Ejecución Local

1. Modo Manual:
```bash
python src/main.py manual --url "https://www.guiacores.com.ar/index.php?r=search%2Findex" --output file
```

2. Modo Secuencial:
```bash
python src/main.py sequential --rubros "rubro1,rubro2" --output file
```

3. Modo Bulk:
```bash
python src/main.py bulk --start-id 1 --end-id 100 --output file
```

### Ejecución con Docker

1. Usando docker-compose:
```bash
# Asegúrate de que docker-compose está instalado
docker-compose up
```

2. Usando Docker directamente:
```bash
# Modo Manual
docker run -it --rm \
    -v $(pwd)/data:/app/data \
    -v $(pwd)/logs:/app/logs \
    etl_guia_cores manual \
    --url "https://www.guiacores.com.ar/index.php?r=search%2Findex" \
    --output file

# Modo Secuencial
docker run -it --rm \
    -v $(pwd)/data:/app/data \
    -v $(pwd)/logs:/app/logs \
    etl_guia_cores sequential \
    --rubros "rubro1,rubro2" \
    --output file

# Modo Bulk
docker run -it --rm \
    -v $(pwd)/data:/app/data \
    -v $(pwd)/logs:/app/logs \
    etl_guia_cores bulk \
    --start-id 1 \
    --end-id 100 \
    --output file
```

## 📁 Estructura del Proyecto

```
etl_guiaCores/
├── data/               # Directorio para datos
│   ├── raw/           # Datos sin procesar
│   └── processed/     # Datos procesados
├── logs/              # Logs de la aplicación
├── src/               # Código fuente
│   ├── common/        # Utilidades comunes
│   ├── manual/        # Modo manual
│   ├── sequential/    # Modo secuencial
│   └── bulk/          # Modo bulk
├── tests/             # Tests unitarios
├── .env               # Variables de entorno
├── .dockerignore      # Archivos ignorados por Docker
├── docker-compose.yml # Configuración de Docker Compose
├── Dockerfile         # Configuración de Docker
├── requirements.txt   # Dependencias de Python
└── README.md         # Este archivo
```

## ⚙️ Configuración

### Variables de Entorno

Crear un archivo `.env` basado en `exampleEnv`:

```env
# Base de datos
DB_HOST=localhost
DB_PORT=5432
DB_NAME=etl_guia_cores
DB_USER=postgres
DB_PASSWORD=your_password

# Configuración de la aplicación
LOG_LEVEL=INFO
OUTPUT_DIR=data/processed
```

### Opciones de Comando

- `--mode`: Modo de operación (manual, sequential, bulk)
- `--url`: URL para extracción (modo manual)
- `--rubros`: Lista de rubros separados por coma (modo sequential)
- `--start-id`: ID inicial (modo bulk)
- `--end-id`: ID final (modo bulk)
- `--output`: Formato de salida (file, database, both)

## 🧪 Tests

Ejecutar tests:
```bash
pytest tests/
```

## 📝 Logs

Los logs se almacenan en el directorio `logs/` con el siguiente formato:
- `etl_YYYY-MM-DD.log`: Logs diarios
- `error_YYYY-MM-DD.log`: Logs de error

## 🤝 Contribución

1. Fork el proyecto
2. Crear una rama para tu feature (`git checkout -b feature/AmazingFeature`)
3. Commit tus cambios (`git commit -m 'Add some AmazingFeature'`)
4. Push a la rama (`git push origin feature/AmazingFeature`)
5. Abrir un Pull Request

## 📄 Licencia

Este proyecto está bajo la Licencia MIT - ver el archivo [LICENSE](LICENSE) para más detalles.

## ✨ Características Adicionales

- [ ] Soporte para más formatos de salida
- [ ] Interfaz web para monitoreo
- [ ] API REST para consultas
- [ ] Dashboard de métricas
- [ ] Sistema de notificaciones 