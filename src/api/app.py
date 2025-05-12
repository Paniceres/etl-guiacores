import logging
from typing import Optional, List

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# Configurar logging para la API
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)

# Importar las funciones ETL desde main.py
try:
    from src.main import run_bulk_etl, run_manual_etl, run_sequential_etl, setup_logging_if_not_configured
    setup_logging_if_not_configured() # Asegurar que el logging en main también esté configurado
except ImportError as e:
    logger.error(f"Error importing ETL functions from src.main: {e}")
    # Esto puede ocurrir si los paths no están bien configurados en el entorno de ejecución.
    # En un contenedor Docker, el PYTHONPATH debería manejarlo.
    # Considerar manejar este error de forma robusta si la ejecución fuera de Docker es común.
    raise

app = FastAPI(
    title="ETL Process API",
    description="API to trigger different ETL processes (Bulk, Manual, Sequential)",
    version="1.0.0",
)

# Modelos para los cuerpos de solicitud
class BulkRequest(BaseModel):
    start_id: int
    end_id: int
    output: str = "both" # choices: "file", "database", "both"

class ManualRequest(BaseModel):
    url: str
    output: str = "both" # choices: "file", "database", "both"

class SequentialRequest(BaseModel):
    rubros: Optional[List[str]] = None
    localidades: Optional[List[str]] = None
    output: str = "both" # choices: "file", "database", "both"

@app.post("/etl/bulk")
async def trigger_bulk_etl(request: BulkRequest):
    """
    Triggers the Bulk ETL process for a given range of IDs.
    """
    logger.info(f"Received /etl/bulk request: {request}")
    try:
        result = run_bulk_etl(request.start_id, request.end_id, request.output)
        if result.get("status") == "error":
            raise HTTPException(status_code=500, detail=result.get("message", "Unknown error"))
        return result
    except Exception as e:
        logger.error(f"Error in /etl/bulk endpoint: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/etl/manual")
async def trigger_manual_etl(request: ManualRequest):
    """
    Triggers the Manual ETL process for a single URL.
    """
    logger.info(f"Received /etl/manual request: {request}")
    try:
        result = run_manual_etl(request.url, request.output)
        if result.get("status") == "error":
            raise HTTPException(status_code=500, detail=result.get("message", "Unknown error"))
        # Considerar un status 404 o similar si no se encuentran datos para la URL
        if result.get("status") == "warning" and result.get("records_processed", 0) == 0:
             raise HTTPException(status_code=404, detail=result.get("message", "No data found for the provided URL"))
        return result
    except Exception as e:
        logger.error(f"Error in /etl/manual endpoint: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/etl/sequential")
async def trigger_sequential_etl(request: SequentialRequest):
    """
    Triggers the Sequential ETL process based on optional rubros and localities.
    """
    logger.info(f"Received /etl/sequential request: {request}")
    try:
        result = run_sequential_etl(request.rubros, request.localidades, request.output)
        if result.get("status") == "error":
            raise HTTPException(status_code=500, detail=result.get("message", "Unknown error"))
        if result.get("status") == "warning" and result.get("records_processed", 0) == 0:
             # Si no se recolectaron URLs, podría considerarse un 404 o simplemente un warning exitoso con 0 registros
             # Lo dejaremos como 200 OK con el mensaje de warning y 0 registros.
             pass # No levantar HTTPException para warnings con 0 records
        return result
    except Exception as e:
        logger.error(f"Error in /etl/sequential endpoint: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    """
    Health check endpoint.
    """
    return {"status": "ok"}

# Puedes añadir más endpoints aquí según sea necesario (ej. /status, /logs, etc.)

# Para correr con uvicorn, el comando sería:
# uvicorn src.api.app:app --host 0.0.0.0 --port 8000
