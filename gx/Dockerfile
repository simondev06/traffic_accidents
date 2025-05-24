# gx/Dockerfile
FROM python:3.11-slim

WORKDIR /app

# Instalamos great_expectations y aseguramos el PATH
RUN pip install --upgrade pip \
 && pip install great_expectations

# ENTRYPOINT para que great_expectations funcione al usar docker run
ENTRYPOINT ["great_expectations"]
