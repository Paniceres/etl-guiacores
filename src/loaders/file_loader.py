from typing import List, Dict, Any
import json
import csv
from pathlib import Path
from datetime import datetime
from ..common.base import BaseLoader
from ..common.config import get_config
from ..common.versioning import DataVersioning  # Import versioning

class FileLoader(BaseLoader):
    """Cargador para archivos"""

    def __init__(self, format: str = 'csv', max_rows: int = 900):
        super().__init__('file_loader')
        self.config = get_config()['loader']['file']
        self.format = format
        self.max_rows = max_rows  # Máximo de registros por archivo

    def load(self, data: List[Dict[str, Any]]) -> None:
        """
        Guarda los datos en archivos, divididos en chunks de 900 registros por archivo.
        
        Args:
            data: Lista de diccionarios con datos.
        """
        if not data:
            return

        output_dir = Path(self.config['output_dir'])
        output_dir.mkdir(parents=True, exist_ok=True)

        for i, chunk in enumerate(DataChunker(data, chunk_size=self.max_rows)):
            # Nominar archivo por timestamp + número de chunk
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"output_{timestamp}_{i+1}.{self.format}"
            filepath = output_dir / filename

            if self.format == 'csv':
                self._save_csv(chunk, filepath)
            elif self.format == 'json':
                self._save_json(chunk, filepath)

            # Versionado del archivo saliente
            dv = DataVersioning(str(Path(__file__).parent.parent.parent))
            dv.version_csv_file(str(filepath))

    def _save_csv(self, data: List[Dict[str, Any]], filepath: Path) -> None:
        """
        Guarda datos en CSV con filas limitadas (máximo 900 contacto x archivo).
        
        Args:
            data: Lista de contacto.
            filepath: Ruta del archivo de salida.
        """
        if not data:
            return

        # Define campos (columnas) en orden explícito si necesitas control
        headers = [
            "id_negocio", "Email", "Facebook", "Instagram",
            "fecha_extraccion", "fecha_actualizacion", "nombre", "direccion", 
            "telefonos", "whatsapp", "sitio_web", "horarios", "rubros", 
            "id_bloque", "url", "fecha_inicio_extraccion", "fecha_fin_extraccion"
        ]

        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(data)

    def _save_json(self, data: List[Dict[str, Any]], filepath: Path) -> None:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)


class DataChunker:
    """Helper para dividir listas en chunks de tamaño constante."""

    def __init__(self, iterable, chunk_size=900):
        self.iterable = iterable
        self.chunk_size = chunk_size

    def __iter__(self):
        for i in range(0, len(self.iterable), self.chunk_size):
            yield self.iterable[i:i + self.chunk_size]