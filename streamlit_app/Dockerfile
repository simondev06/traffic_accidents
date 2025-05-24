FROM python:3.11

# Crear directorio de trabajo
WORKDIR /app

# Copiar archivos necesarios
COPY requirements.txt .
COPY dashboard.py .

# Instalar dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Exponer el puerto donde correrá Streamlit
EXPOSE 8501

# Comando para ejecutar Streamlit
CMD ["streamlit", "run", "dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]
