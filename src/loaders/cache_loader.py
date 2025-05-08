from typing import List, Dict, Any
import json
from pathlib import Path
from datetime import datetime, timedelta
from ..common.base import BaseLoader
from ..common.config import get_config

class CacheLoader(BaseLoader):
    """Cargador para caché de datos"""
    
    def __init__(self):
        super().__init__('cache_loader')
        self.config = get_config()['loader']['cache']
        self.cache_dir = Path('data/cache')
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
    def load(self, data: List[Dict[str, Any]]) -> None:
        """
        Guarda los datos en caché
        
        Args:
            data: Lista de diccionarios con datos
        """
        if not self.config['enabled'] or not data:
            return
            
        # Limpiar caché antigua
        self._clean_old_cache()
        
        # Guardar datos en caché
        timestamp = datetime.now()
        cache_file = self.cache_dir / f"cache_{timestamp.strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump({
                'timestamp': timestamp.isoformat(),
                'data': data
            }, f, ensure_ascii=False, indent=2)
            
    def _clean_old_cache(self) -> None:
        """Limpia la caché antigua"""
        if not self.config['enabled']:
            return
            
        # Calcular fecha límite
        limit = datetime.now() - timedelta(seconds=self.config['ttl'])
        
        # Eliminar archivos antiguos
        for cache_file in self.cache_dir.glob('cache_*.json'):
            try:
                # Leer timestamp
                with open(cache_file, 'r', encoding='utf-8') as f:
                    cache_data = json.load(f)
                    cache_time = datetime.fromisoformat(cache_data['timestamp'])
                    
                # Eliminar si es antiguo
                if cache_time < limit:
                    cache_file.unlink()
                    
            except Exception:
                # Si hay error, eliminar el archivo
                cache_file.unlink()
                
    def get_cached_data(self) -> List[Dict[str, Any]]:
        """
        Obtiene los datos de la caché más reciente
        
        Returns:
            Lista de diccionarios con datos en caché
        """
        if not self.config['enabled']:
            return []
            
        # Buscar archivo más reciente
        cache_files = list(self.cache_dir.glob('cache_*.json'))
        if not cache_files:
            return []
            
        latest_file = max(cache_files, key=lambda x: x.stat().st_mtime)
        
        try:
            # Leer datos
            with open(latest_file, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
                cache_time = datetime.fromisoformat(cache_data['timestamp'])
                
            # Verificar si está expirado
            if datetime.now() - cache_time > timedelta(seconds=self.config['ttl']):
                return []
                
            return cache_data['data']
            
        except Exception:
            return [] 