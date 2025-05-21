#!/bin/bash

# Colores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

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

# Inicializar Buildx si no existe un builder
docker buildx create --name etl-guiacores-builder

# Usar Buildx para construir la imagen
echo "Construyendo imagen Docker con Buildx..."
docker buildx build --tag etl-guiacores --builder etl-guiacores_builder . --load

success "Imagen construida exitosamente con Buildx"

# Mostrar ayuda
echo "
Uso:
1. Ejecutar con docker-compose:
   docker-compose up

2. Ejecutar manualmente:
   docker run -it --rm \\
       -v \$(pwd)/data:/app/data \\
       -v \$(pwd)/logs:/app/logs \\
       etl-guiacores [modo] [opciones]
"