#!/bin/bash

# Verificar que se está en el directorio raíz
if [ ! -f "Dockerfile" ]; then
    echo "Error: ejecutar este script desde el directorio raíz del proyecto"
    exit 1
fi

set -e

# Colores
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Verificar que la imagen ETL exista
if [ -z "$(docker images -q etl-guiacores)" ]; then
    echo -e "${RED}No se creó aún el contenedor ETL. Ejecute ./build.sh primero${NC}"
    exit 1
fi

# Detectar la ubicación del volumen main.py (en la base de la imagen)
ARGS=("docker" "run" "--rm" "-v" "$(pwd)/data:/app/data" "-v" "$(pwd)/logs:/app/logs" "etl-guiacores" "python" "src/main.py")

# Asignar argumentos según el modo ETL
MODE=$1
shift
show_help(){
    echo -e "${GREEN}Uso:${NC}"
    echo "  ./run.sh [modo] [opciones]"
    echo
    echo -e "${GREEN}Modos:${NC}"
    echo "  bulk         - Procesar múltiples contratos CSV/JSON"
    echo "  sequential   - Procesar manualmente con archivo HTML"
    echo "  manual       - Procesar manuales por rubro"
    echo
    echo -e "${GREEN}Parámetros Comunes:${NC}"
    echo "  --output     Tipo de base de datos (file, database, o both)"
    echo "  --start-id   Número inicial para procesar ids"
    echo "  --end-id     Número final para procesar ids"
    echo "  --rubros     Campo para procesado secuencial"
    echo "  --url        URL ya construida"
    echo "  --file       Archivo con datos"
    echo
}

docker run --rm -v $(pwd)/data:/app/data -v $(pwd)/logs:/app/logs etl-guiacores python src/main.py "$@"