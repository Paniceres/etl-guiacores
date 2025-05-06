# Estructura de Directorios de Datos

Este directorio contiene todos los datos generados y procesados por el sistema ETL de Guía Cores.

## Estructura de Carpetas

```
data/
├── logs/           # Archivos de registro (logs)
│   ├── cleaner/    # Logs del proceso de limpieza
│   └── collector/  # Logs del proceso de recolección
│
├── raw/           # Datos crudos sin procesar
│   ├── json/      # Archivos JSON de datos crudos
│   └── csv/       # Archivos CSV de datos crudos
│
├── processed/     # Datos procesados y limpios
│   ├── json/      # Archivos JSON procesados
│   └── csv/       # Archivos CSV procesados
│
└── temp/          # Archivos temporales
```

## Descripción de Carpetas

### logs/
Contiene todos los archivos de registro del sistema:
- `cleaner/`: Logs del proceso de limpieza de datos
- `collector/`: Logs del proceso de recolección de URLs y datos

### raw/
Almacena los datos crudos antes de cualquier procesamiento:
- `json/`: Archivos JSON con datos crudos (ej: `guiaCores_urls_bulk.json`)
- `csv/`: Archivos CSV con datos crudos (ej: `guiaCores_leads.csv`)

### processed/
Contiene los datos ya procesados y limpios:
- `json/`: Archivos JSON procesados
- `csv/`: Archivos CSV procesados (ej: `guiaCores_leads_cleaned.csv`)

### temp/
Directorio para archivos temporales que se pueden eliminar en cualquier momento.

## Convenciones de Nombres

- Archivos de URLs: `guiaCores_urls_*.json`
- Archivos de leads: `guiaCores_leads_*.csv`
- Archivos de leads limpios: `guiaCores_leads_cleaned.csv`
- Archivos de log: `*_YYYYMMDD.log`

## Mantenimiento

- Los archivos en `temp/` pueden ser eliminados en cualquier momento
- Los logs se rotan automáticamente
- Se recomienda hacer backup de los datos en `processed/` periódicamente 