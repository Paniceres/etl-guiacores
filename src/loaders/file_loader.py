from typing import List, Dict, Any
import json
import csv
from pathlib import Path
from datetime import datetime
from ..common.base import BaseLoader
from ..common.config import get_config

class FileLoader(BaseLoader):
    """Cargador para archivos"""
    
    def __init__(self, format: str = 'json'):
        super().__init__('file_loader')
        self.config = get_config()['loader']['file']
        self.format = format
        
    def load(self, data: List[Dict[str, Any]]) -> None:
        """
        Guarda los datos en un archivo
        
        Args:
            data: Lista de diccionarios con datos
        """
        if not data:
            return
            
        # Crear directorio si no existe
        output_dir = Path(self.config['output_dir'])
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Generar nombre de archivo
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"businesses_{timestamp}.{self.format}"
        filepath = output_dir / filename
        
        # Guardar datos
        if self.format == 'json':
            self._save_json(data, filepath)
        elif self.format == 'csv':
            self._save_csv(data, filepath)
            
    def _save_json(self, data: List[Dict[str, Any]], filepath: Path) -> None:
        """
        Guarda los datos en formato JSON
        
        Args:
            data: Lista de diccionarios con datos
            filepath: Ruta del archivo
        """
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            
    def _save_csv(self, data: List[Dict[str, Any]], filepath: Path) -> None:
        """
        Guarda los datos en formato CSV
        
        Args:
            data: Lista de diccionarios con datos
            filepath: Ruta del archivo
        """
        if not data:
            return
            
        # Obtener columnas
        columns = list(data[0].keys())
        
        # Escribir archivo
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=columns)
            writer.writeheader()
            writer.writerows(data) 