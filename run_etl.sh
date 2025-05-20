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
    echo -e "${GREEN}Opciones comunes:${NC}"
    echo "  --output [file|database|both] - Especifica el destino de la salida (por defecto: database)"
    echo
    echo -e "${GREEN}Opciones para modo manual:${NC}"
    echo "  --url URL       - URL para scraping manual (exclusivo con --file)"
    echo "  --file PATH     - Ruta al archivo para scraping manual (exclusivo con --url)"
    echo
    echo -e "${GREEN}Opciones para modo bulk:${NC}"
    echo "  --min-id N      - ID mínimo para el rango (opcional)"
    echo "  --max-id N      - ID máximo para el rango (opcional)"
    echo
    echo -e "${GREEN}Opciones para modo sequential:${NC}"
    echo "  --rubros RUBROS - Lista de rubros separados por coma (opcional)"
    echo
    echo -e "${GREEN}Ejemplos:${NC}"
    echo "  ./run_etl.sh bulk"
    echo "  ./run_etl.sh bulk --min-id 1000 --max-id 2000"
    echo "  ./run_etl.sh sequential"
    echo "  ./run_etl.sh sequential --rubros 'alimentacion,tecnologia'"
    echo "  ./run_etl.sh manual --url 'http://example.com/page'"
    echo "  ./run_etl.sh manual --file './data/input.html'"
    echo "  ./run_etl.sh manual --url 'http://example.com/page' --output file"
}

# Definir variables por defecto
MODE="bulk"
OUTPUT="database"

# Parsear argumentos generales (modo y opciones comunes)
while [[ $# -gt 0 ]]; do
    case $1 in
        bulk|sequential|manual)
            MODE=$1
            shift
            ;;
        --output)
            if [ -n "$2" ] && ([[ "$2" == "file" ]] || [[ "$2" == "database" ]] || [[ "$2" == "both" ]]); then
                OUTPUT=$2
                shift 2
            else
                echo -e "${RED}Error: Valor inválido para --output. Use 'file', 'database', o 'both'.${NC}"
                show_help
                exit 1
            fi
            ;;
        *)
            # Si no es un modo o una opción común, lo dejamos para el case específico del modo
            break
            ;;
    esac
fi
# Procesar el modo y sus argumentos
case $MODE in
    "bulk")
        MIN_ID=""
        MAX_ID=""
        while [[ $# -gt 0 ]]; do
            case $1 in
                --min-id)
                    MIN_ID=$2
                    shift 2
                    ;;
                --max-id)
                    MAX_ID=$2
                    shift 2
                    ;;
                *)
                    echo -e "${RED}Error: Argumento desconocido para modo bulk: $1${NC}"
                    show_help
                    exit 1
                    ;;
            esac
        done
        ETL_COMMAND="python -m src.main bulk --output $OUTPUT"
        [ -n "$MIN_ID" ] && ETL_COMMAND="$ETL_COMMAND --start_id $MIN_ID"
        [ -n "$MAX_ID" ] && ETL_COMMAND="$ETL_COMMAND --end_id $MAX_ID"
        ;;
    "sequential")
        RUBROS=""
        while [[ $# -gt 0 ]]; do
            case $1 in
                --rubros)
                    RUBROS=$2
                    shift 2
                    ;;
                *)
                    echo -e "${RED}Error: Argumento desconocido para modo sequential: $1${NC}"
                    show_help
                    exit 1
                    ;;
            esac
        done
        ETL_COMMAND="python -m src.main sequential --output $OUTPUT"
        [ -n "$RUBROS" ] && ETL_COMMAND="$ETL_COMMAND --rubros $RUBROS"
        ;;
    "manual")
        URL=""
        FILE=""
        # Procesar argumentos adicionales
        while [[ $# -gt 0 ]]; do
            case $1 in
                --url)
                    URL=$2
                    shift 2
                    ;;
                --file)
                    FILE=$2
                    shift 2
                    ;;
                *)
                    echo -e "${RED}Error: Argumento desconocido $1${NC}"
                    show_help
                    exit 1
                    ;;
            esac
        done

        # Verificar argumentos
        if [ -n "$URL" ] && [ -n "$FILE" ]; then
            echo -e "${RED}Error: Modo manual solo acepta --url o --file, no ambos.${NC}"
            show_help
            exit 1
        elif [ -z "$URL" ] && [ -z "$FILE" ]; then
            echo -e "${RED}Error: Modo manual requiere --url o --file.${NC}"
            show_help
            exit 1
        fi
        ETL_COMMAND="python -m src.main manual --output $OUTPUT ${URL:+-url '$URL'} ${FILE:+-file '$FILE'}"

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