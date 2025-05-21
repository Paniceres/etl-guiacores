# docker/extractor/Dockerfile
FROM python:3.9-slim

# Instalar un navegador (como Chrome) y sus dependencias si el scraping requiere Selenium con un navegador real
# Esto puede variar dependiendo de si usas un headless browser como headless-chrome-selenium
# Ejemplo para Chrome (requiere ajustes finos dependiendo de la imagen base y el navegador específico):
# RUN apt-get update && apt-get install -y gnupg curl && \
#   curl https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - && \
#   echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list && \
#   apt-get update && apt-get install -y google-chrome-stable

# Si usas selenium-headless-chrome, la imagen base ya podría incluir el navegador y el driver.
# Revisa la documentación de la imagen que elijas.

WORKDIR /app

# Copiar requirements.txt e instalar dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código de la aplicación
COPY src/common /app/src/common
COPY src/extractors /app/src/extractors

# Copiar el script de entrada
COPY src/extractors/run_extraction.py /app/src/extractors/

# Si get_config() lee un archivo específico, asegúrate de copiarlo
# Ejemplo: COPY config.yaml /app/config.yaml

# Comando para ejecutar el script de extracción
ENTRYPOINT ["python", "/app/src/extractors/run_extraction.py"]
