#!/bin/bash

# Colores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

# Función para mostrar mensajes de error
error() {
    echo -e "${RED}Error: $1${NC}" >&2
    exit 1
}

# Función para mostrar mensajes de éxito
success() {
    echo -e "${GREEN}$1${NC}"
}

# Verificar que estamos en el directorio correcto
if [ ! -f "Dockerfile" ]; then
    error "Debes ejecutar este script desde el directorio raíz del proyecto"
fi

# Construir la imagen
echo "Construyendo imagen con Podman buildx..."
podman buildx build --platform linux/amd64 -t etl_guia_cores_podman . || error "Error al construir la imagen con Podman buildx"

success "Imagen construida exitosamente con Podman buildx"

# Mostrar ayuda
echo "
Uso:
1. Ejecutar con docker-compose:
   docker-compose up

2. Ejecutar manualmente:
   docker run -it --rm \\
       -v \$(pwd)/data:/app/data \\
       -v \$(pwd)/logs:/app/logs \\
       etl_guia_cores_podman [modo] [opciones]

Ejemplos:
   # Modo manual
   docker run -it --rm \\
       -v \$(pwd)/data:/app/data \\
       -v \$(pwd)/logs:/app/logs \\
       etl_guia_cores_podman manual --url \"https://www.guiacores.com.ar/index.php?r=search%2Findex\" --output file

   # Modo secuencial
   docker run -it --rm \\
       -v \$(pwd)/data:/app/data \\
       -v \$(pwd)/logs:/app/logs \\
       etl_guia_cores_podman sequential --rubros \"rubro1,rubro2\" --output file

   # Modo bulk
   docker run -it --rm \\
       -v \$(pwd)/data:/app/data \\
       -v \$(pwd)/logs:/app/logs \\
       etl_guia_cores_podman bulk --start-id 1 --end-id 100 --output file
" 