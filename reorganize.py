import os
import shutil
from pathlib import Path

def create_directory(path):
    """Crea un directorio si no existe"""
    os.makedirs(path, exist_ok=True)

def move_file(src, dst):
    """Mueve un archivo de src a dst"""
    if os.path.exists(src):
        shutil.move(src, dst)

def reorganize_structure():
    # Directorio base
    base_dir = Path(__file__).parent / 'etl_guiaCores'
    src_dir = base_dir / 'src'

    # Crear nueva estructura
    new_dirs = {
        '0_common': src_dir / '0_common',
        '1_extractors': {
            'bulk': src_dir / '1_extractors' / 'bulk',
            'sequential': src_dir / '1_extractors' / 'sequential',
            'manual': src_dir / '1_extractors' / 'manual'
        },
        '2_transformers': src_dir / '2_transformers',
        '3_loaders': src_dir / '3_loaders'
    }

    # Crear directorios
    for dir_path in [new_dirs['0_common'], new_dirs['2_transformers'], new_dirs['3_loaders']]:
        create_directory(dir_path)
    
    for extractor_dir in new_dirs['1_extractors'].values():
        create_directory(extractor_dir)

    # Mover archivos comunes
    common_files = [
        'base.py',
        'config.py',
        'db.py',
        'utils.py',
        'versioning.py'
    ]
    for file in common_files:
        src = src_dir / 'common' / file
        dst = new_dirs['0_common'] / file
        move_file(src, dst)

    # Mover extractores manteniendo su estructura
    extractor_files = {
        'bulk': ['collector.py', 'scraper.py'],
        'sequential': ['collector.py', 'scraper.py'],
        'manual': ['scraper.py']
    }
    for extractor_type, files in extractor_files.items():
        for file in files:
            src = src_dir / extractor_type / file
            dst = new_dirs['1_extractors'][extractor_type] / file
            move_file(src, dst)

    # Mover transformadores
    transformer_files = [
        'business_transformer.py',
        'data_cleaner.py',
        'url_transformer.py'
    ]
    for file in transformer_files:
        src = src_dir / 'transformers' / file
        dst = new_dirs['2_transformers'] / file
        move_file(src, dst)

    # Mover loaders
    loader_files = [
        'database_loader.py',
        'file_loader.py',
        'cache_loader.py'
    ]
    for file in loader_files:
        src = src_dir / 'loaders' / file
        dst = new_dirs['3_loaders'] / file
        move_file(src, dst)

    # Eliminar directorios antiguos
    old_dirs = [
        'common',
        'bulk',
        'sequential',
        'manual',
        'transformers',
        'loaders',
        'collectors',
        'extractors'
    ]
    for dir_name in old_dirs:
        old_dir = src_dir / dir_name
        if old_dir.exists():
            shutil.rmtree(old_dir)

if __name__ == '__main__':
    reorganize_structure() 