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
    echo "  --url URL       - URL de la búsqueda avanzada (obligatorio si no se usa --file)"
    echo "  --file NOMBRE_ARCHIVO - Archivo HTML en /data/html_samples (obligatorio si no se usa --url)"
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
        BULK_ID_MIN="1"
        BULK_ID_MAX="99999"
        while [[ $# -gt 0 ]]; do
            case $1 in
                --id_min)
                    BULK_ID_MIN=$2
                    shift 2
                    ;;
                --id_max)
                    BULK_ID_MAX=$2
                    shift 2
                    ;;
                *)
                    echo -e "${RED}Error: Argumento desconocido $1${NC}"
                    show_help
                    exit 1
                    ;;
            esac
        done
        ETL_COMMAND="python -m src.main bulk --id_min=$BULK_ID_MIN --id_max=$BULK_ID_MAX"
        ;;
    "sequential")
        SEQ_RUBRO=""
        while [[ $# -gt 0 ]]; do
            case $1 in
                --rubro)
                    SEQ_RUBRO=$2
                    shift 2
                    ;;
                *)
                    echo -e "${RED}Error: Argumento desconocido $1${NC}"
                    show_help
                    exit 1
                    ;;
            esac
        done
        if [ -z "$SEQ_RUBRO" ]; then
            ETL_COMMAND="python -m src.main sequential"
        else
            ETL_COMMAND="python -m src.main sequential --rubro=$SEQ_RUBRO"
        fi
        ;;
    "manual")
        MANUAL_URL=""
        MANUAL_FILE=""
        
        # Procesar argumentos adicionales
        while [[ $# -gt 0 ]]; do
            case $1 in
                --url)
                    MANUAL_URL=$2
                    shift 2
                    ;;
                --file)
                    MANUAL_FILE=$2
                    shift 2
                    ;;
                *)
                    echo -e "${RED}Error: Argumento desconocido $1${NC}"
                    show_help
                    exit 1
                    ;;
            esac
        done
        
        # Verificar que se proporciona una URL o un archivo
        if [ -z "$MANUAL_URL" ] && [ -z "$MANUAL_FILE" ]; then
            echo -e "${RED}Error: Modo manual requiere --url o --file${NC}"
            show_help
            exit 1
        fi
        
        if [ -n "$MANUAL_FILE" ] && [ ! -f "/data/html_samples/$MANUAL_FILE" ]; then
            echo -e "${RED}Error: Archivo $MANUAL_FILE no encontrado en /data/html_samples${NC}"
            show_help
            exit 1
        fi
        
        if [ -n "$MANUAL_URL" ]; then
            ETL_COMMAND="python -m src.main manual --url $MANUAL_URL"
        else
            ETL_COMMAND="python -m src.main manual --file $MANUAL_FILE"
        fi
        ;;
    *)
        echo -e "${RED}Error: Modo desconocido '$MODE'${NC}"
        show_help
        exit 1
        ;;
esac

# Ejecutar docker-compose con el comando apropiado
echo -e "${YELLOW}Ejecutando ETL en modo $MODE...${NC}"
ETL_COMMAND="$ETL_COMMAND" docker-compose up 