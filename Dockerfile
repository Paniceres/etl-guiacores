FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    chromium \
    chromium-driver \
    # Dependencies for Chrome
    libglib2.0-0 \
    libnss3 \
    libgconf-2-4 \
    libfontconfig1 \
    # Additional dependencies
    wget \
    gnupg \
    unzip \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create necessary directories
RUN mkdir -p /app/data /app/logs

# Copy the rest of the application
COPY src/ ./src/
COPY data/ ./data/

# Create a non-root user
RUN useradd -m etluser && \
    chown -R etluser:etluser /app
USER etluser

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    CHROME_BIN=/usr/bin/chromium \
    CHROME_PATH=/usr/lib/chromium/ \
    DEBIAN_FRONTEND=noninteractive

# Config Chrome for headless mode
ENV CHROME_OPTIONS="--headless --no-sandbox --disable-dev-shm-usage --disable-gpu"

# Health check - verifica que el proceso ETL esté funcionando
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD ps aux | grep "[p]ython src/main.py" || exit 1

# Command to run the ETL process
ENTRYPOINT ["python", "src/main.py"] 