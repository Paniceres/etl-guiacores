import json
import logging
import os
from pathlib import Path
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

class FileLoader:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        # Ensure data/processed directory exists, configure DATA_OUTPUT_DIR in .env or config
        self.output_dir = Path(config.get("OUTPUT_DIR", "data/processed"))
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"FileLoader initialized. Output directory: {self.output_dir}")

    def load(self, data: List[Dict[str, Any]], filename_prefix: str = "data") -> None:
        if not data:
            logger.info("No data to load into file.")
            return

        # Create a unique filename, e.g., using a timestamp or a specific identifier from the data
        # For simplicity, using a generic name here. You might want to make this more specific.
        output_file = self.output_dir / f"{filename_prefix}_{Path(data[0].get('url', 'generic_data') if data and isinstance(data[0], dict) else 'generic_data').stem}.json"

        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
            logger.info(f"Successfully saved {len(data)} records to {output_file}")
        except IOError as e:
            logger.error(f"Error writing data to file {output_file}: {e}", exc_info=True)
            # Potentially raise the exception or handle it as per your app's error strategy
            raise
        except TypeError as e:
            logger.error(f"Error serializing data to JSON for file {output_file}: {e}", exc_info=True)
            # Potentially raise the exception
            raise

    # You might want to add methods for different file formats, e.g., to_csv, etc.
