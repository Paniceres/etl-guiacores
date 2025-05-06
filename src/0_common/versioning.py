import os
import shutil
from datetime import datetime
import json
import pandas as pd

class DataVersioning:
    def __init__(self, base_path):
        self.base_path = base_path
        self.raw_json_path = os.path.join(base_path, 'data/raw/json')
        self.processed_json_path = os.path.join(base_path, 'data/processed/json')
        self.processed_csv_path = os.path.join(base_path, 'data/processed/csv')

    def _get_version_name(self, prefix='version'):
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        return f"{prefix}_{timestamp}"

    def _get_monthly_version_name(self):
        """Obtiene el nombre de versión mensual en formato YYYYMM"""
        return datetime.now().strftime('%Y%m')

    def version_json_file(self, file_path, is_raw=True):
        """Versiona un archivo JSON y lo mueve a la carpeta de versiones correspondiente"""
        if not os.path.exists(file_path):
            return None

        version_name = self._get_version_name()
        target_dir = os.path.join(self.raw_json_path, 'versions') if is_raw else os.path.join(self.processed_json_path, 'versions')
        target_path = os.path.join(target_dir, f"{version_name}.json")

        # Crear directorio de versiones si no existe
        os.makedirs(target_dir, exist_ok=True)

        # Copiar archivo a la versión
        shutil.copy2(file_path, target_path)
        return target_path

    def version_csv_file(self, file_path):
        """Versiona un archivo CSV y lo mueve a la carpeta de versiones correspondiente"""
        if not os.path.exists(file_path):
            return None

        version_name = self._get_version_name()
        target_dir = os.path.join(self.processed_csv_path, 'versions')
        target_path = os.path.join(target_dir, f"{version_name}.csv")

        # Crear directorio de versiones si no existe
        os.makedirs(target_dir, exist_ok=True)

        # Copiar archivo a la versión
        shutil.copy2(file_path, target_path)
        return target_path

    def version_bulk_data(self, data, filename='bulk_data'):
        """Versiona datos bulk mensualmente, manteniendo solo la última versión del mes"""
        monthly_version = self._get_monthly_version_name()
        target_path = os.path.join(self.raw_json_path, f"{filename}_{monthly_version}.json")
        
        # Si existe una versión anterior del mismo mes, la eliminamos
        if os.path.exists(target_path):
            os.remove(target_path)
        
        # Guardamos la nueva versión
        with open(target_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        return target_path

    def get_latest_bulk_version(self, filename='bulk_data'):
        """Obtiene la última versión mensual de los datos bulk"""
        files = [f for f in os.listdir(self.raw_json_path) if f.startswith(f"{filename}_")]
        if not files:
            return None
        
        # Ordenamos por nombre de archivo (que incluye el timestamp mensual)
        latest_file = sorted(files)[-1]
        return os.path.join(self.raw_json_path, latest_file)

    def get_latest_version(self, file_type='json', is_raw=True):
        """Obtiene la última versión de un archivo"""
        if file_type == 'json':
            base_dir = self.raw_json_path if is_raw else self.processed_json_path
        else:
            base_dir = self.processed_csv_path

        versions_dir = os.path.join(base_dir, 'versions')
        if not os.path.exists(versions_dir):
            return None

        versions = [f for f in os.listdir(versions_dir) if f.startswith('version_')]
        if not versions:
            return None

        return os.path.join(versions_dir, sorted(versions)[-1])

    def clean_old_versions(self, keep_last_n=5):
        """Limpia versiones antiguas, manteniendo solo las últimas N versiones"""
        for base_dir in [self.raw_json_path, self.processed_json_path, self.processed_csv_path]:
            versions_dir = os.path.join(base_dir, 'versions')
            if not os.path.exists(versions_dir):
                continue

            versions = sorted([f for f in os.listdir(versions_dir) if f.startswith('version_')])
            if len(versions) <= keep_last_n:
                continue

            for old_version in versions[:-keep_last_n]:
                os.remove(os.path.join(versions_dir, old_version)) 