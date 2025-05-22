# Guia Cores ETL Service

This project provides an ETL (Extract, Transform, Load) pipeline for processing data from Guia Cores. It supports multiple extraction modes and outputs processed data as CSV files. The project has been simplified to remove Docker, database dependencies, Terraform, and testing infrastructure, focusing on a core Python ETL pipeline with a Streamlit GUI and command-line interface (CLI).

## Key Features

*   **ETL Modes:**
    *   **Bulk Mode:** Extracts data based on a range of business IDs.
    *   **Manual Mode:** Extracts data from a specific Guia Cores URL or a directory of HTML files.
    *   **Sequential Mode:** Extracts data based on 'rubros' (categories) and/or 'localidades'.
*   **Streamlit GUI:** A graphical user interface for easy interaction with the ETL pipeline.
*   **Command Line Interface (CLI):** Allows running ETL processes directly from the terminal.
*   **CSV Output:** Processed data is saved as CSV files in the `data/processed` directory.
*   **Centralized Configuration:** Application settings, including Selenium configurations, are managed through environment variables (in the `.env` file).
*   **Detailed Logging:** Comprehensive logging of ETL processes and API interactions to facilitate monitoring and debugging. Logs are saved in `data/logs/etl_api.log` and output to the console.

## Prerequisites

*   Python 3.11+
*   Selenium
*   Chrome browser (or other browser compatible with Selenium)
*   ChromeDriver (or other browser driver compatible with Selenium and your browser)

## Project Structure

```
etl_guiaCores/
├── data/                 # Directory for local file outputs and logs
│   └── logs/
│       └── etl_api.log
├── src/                  # Source code
│   ├── common/           # Common modules (config, logger, utils)
│   ├── extractors/       # Collectors and scrapers for different ETL modes
│   ├── loaders/          # Loaders for file output (CSV)
│   ├── transformers/     # Data transformers
│   ├── __init__.py
│   └── main.py           # Main ETL functions and CLI entry point
├── .env                  # Environment variables (ignored by git, use exampleEnv as template)
├── exampleEnv            # Template for the .env file
├── requirements.txt      # Python dependencies
├── README.md             # This file
└── streamlit_app.py      # Streamlit GUI application
```

## Setup

1.  **Clone the Repository:**
    ```bash
    git clone https://github.com/Paniceres/etl_guiaCores.git
    cd etl_guiaCores
    ```

2.  **Configure Environment Variables:**
    Copy the template file `exampleEnv` to `.env` and adjust the settings as needed.
    ```bash
    cp exampleEnv .env
    vim .env
    ```

3.  **Install Dependencies:**
    Create a virtual environment (optional but recommended) and install the required Python packages.
    ```bash
    python -m venv .venv
    source .venv/bin/activate  # On Linux/macOS
    .venv\Scripts\activate  # On Windows
    pip install -r requirements.txt
    ```

## Usage

You can interact with the ETL pipeline using either the Streamlit GUI or the command-line interface (CLI).

### 1. Streamlit GUI

1.  Open your terminal in the project's root directory.
2.  Run the command: `streamlit run streamlit_app.py`
3.  This will open the Streamlit application in your web browser. You can then select the ETL mode, provide the necessary inputs, and start the ETL process.
4.  The generated CSV files will be saved in the `data/processed` directory.

### 2. Command Line Interface (CLI)

1.  Open your terminal in the project's root directory.
2.  Use the following commands based on the desired ETL mode:

    *   **Bulk Mode:**
        ```bash
        python src/main.py bulk --start_id <start_id> --end_id <end_id>
        ```
    *   **Manual Mode (URL):**
        ```bash
        python src/main.py manual --url <url>
        ```
    *   **Manual Mode (HTML File Directory):**
        ```bash
        python src/main.py manual --file <path_to_html_directory>
        ```
    *   **Sequential Mode:**
        ```bash
        python src/main.py sequential [--rubros <rubros_comma_separated>] [--localidades <localidades_comma_separated>]
        ```

    The processed data will be saved as CSV files in the `data/processed` directory.

## Logging

ETL process logs are saved in `data/logs/etl_api.log`.

## Contributing

Contributions are welcome! Please fork the repository, create a branch for your changes, and submit a pull request.
