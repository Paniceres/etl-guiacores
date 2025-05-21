# docker/transformer/Dockerfile
FROM python:3.9-slim

WORKDIR /app

# Copiar requirements.txt e instalar dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código de la aplicación
# Ajusta las rutas según la estructura real de tu proyecto
COPY src/common /app/src/common
COPY src/transformers /app/src/transformers

# Copiar el script de entrada
COPY src/transformers/run_transformation.py /app/src/transformers/

# Si get_config() lee un archivo específico (ej. config.yaml o similar),
# asegúrate de copiarlo también o configurar cómo se proporcionará (ej. ConfigMap).
# Ejemplo: COPY config.yaml /app/config.yaml

# Comando para ejecutar el script de transformación
ENTRYPOINT ["python", "/app/src/transformers/run_transformation.py"]
