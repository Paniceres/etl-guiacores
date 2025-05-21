# docker/loader/Dockerfile
FROM python:3.9-slim

WORKDIR /app

# Copiar requirements.txt e instalar dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código de la aplicación
# Ajusta las rutas según la estructura real de tu proyecto
COPY src/common /app/src/common
COPY src/loaders /app/src/loaders

# Copiar el script de entrada
COPY src/loaders/run_loading.py /app/src/loaders/

# Si get_config() lee un archivo específico o si necesitas credenciales de DB,
# asegúrate de copiarlos o configurar cómo se proporcionarán (ej. ConfigMap, Secrets).
# Ejemplo: COPY config.yaml /app/config.yaml

# Comando para ejecutar el script de carga
ENTRYPOINT ["python", "/app/src/loaders/run_loading.py"]
