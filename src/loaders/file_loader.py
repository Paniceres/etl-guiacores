import logging
import os
from pathlib import Path
from typing import List, Dict, Any
import pandas as pd

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

        try:
            # Convert list of dictionaries to pandas DataFrame
            df = pd.DataFrame(data)

            # Create a unique filename, e.g., using a timestamp or a specific identifier
            # For simplicity, using a generic name with timestamp.
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = self.output_dir / f"{filename_prefix}_{timestamp}.csv"

            # Save DataFrame to CSV
            df.to_csv(output_file, index=False, encoding='utf-8')

            logger.info(f"Successfully saved {len(data)} records to {output_file} as CSV")
        except Exception as e:
            logger.error(f"Error writing data to CSV file: {e}", exc_info=True)
            raise

from datetime import datetime # Import datetime here since it's used in the load method
