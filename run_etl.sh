#!/bin/bash

# Colores para mensajes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Función de ayuda
show_help() {
    echo -e "${GREEN}Uso:${NC}"
    echo "  ./run_etl.sh [modo] [opciones]"
    echo
    echo -e "${GREEN}Modos disponibles:${NC}"
    echo "  bulk        - Modo bulk (por defecto)"
    echo "  sequential  - Modo secuencial"
    echo "  manual      - Modo manual con argumentos específicos"
    echo
    echo -e "${GREEN}Opciones para modo manual:${NC}"
    echo "  --start-id N    - ID inicial (requerido para modo manual)"
    echo "  --end-id N      - ID final (requerido para modo manual)"
    echo
    echo -e "${GREEN}Ejemplos:${NC}"
    echo "  ./run_etl.sh bulk"
    echo "  ./run_etl.sh sequential"
    echo "  ./run_etl.sh manual --start-id 1000 --end-id 2000"
}

# Verificar si se proporcionó un modo
if [ $# -eq 0 ]; then
    MODE="bulk"
else
    MODE=$1
    shift
fi

# Procesar el modo y sus argumentos
case $MODE in
    "bulk")
        ETL_COMMAND="python -m src.main bulk"
        ;;
    "sequential")
        ETL_COMMAND="python -m src.main sequential"
        ;;
    "manual")
        START_ID=""
        END_ID=""
        
        # Procesar argumentos adicionales
        while [[ $# -gt 0 ]]; do
            case $1 in
                --start-id)
                    START_ID=$2
                    shift 2
                    ;;
                --end-id)
                    END_ID=$2
                    shift 2
                    ;;
                *)
                    echo -e "${RED}Error: Argumento desconocido $1${NC}"
                    show_help
                    exit 1
                    ;;
            esac
        done
        
        # Verificar argumentos requeridos
        if [ -z "$START_ID" ] || [ -z "$END_ID" ]; then
            echo -e "${RED}Error: Modo manual requiere --start-id y --end-id${NC}"
            show_help
            exit 1
        fi
        
        ETL_COMMAND="python -m src.main manual --start-id $START_ID --end-id $END_ID"
        ;;
    *)
        echo -e "${RED}Error: Modo desconocido '$MODE'${NC}"
        show_help
        exit 1
        ;;
esac

# Ejecutar docker-compose con el comando apropiado
echo -e "${YELLOW}Ejecutando ETL en modo $MODE con podman-compose...${NC}\n"
ETL_COMMAND="$ETL_COMMAND" podman-compose up