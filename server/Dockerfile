# Dockerfile para server (SOAP)
FROM python:3.11-slim

WORKDIR /app

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copiar requirements
COPY requirements.txt .

# Instalar dependencias Python
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código fuente
COPY . .

# Regenerar archivos protobuf con la versión correcta
WORKDIR /app/grpc_client/protos
RUN python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. image_processing.proto
WORKDIR /app

# Crear directorio de salida
RUN mkdir -p output

# Exponer puerto
EXPOSE 8000

# Variables de entorno
ENV SOAP_HOST=0.0.0.0
ENV SOAP_PORT=8000

# Comando de inicio
CMD ["python", "simple_soap_server.py"]
