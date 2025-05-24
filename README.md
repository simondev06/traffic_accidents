# Proyecto ETL y Streaming de Accidentes de Tránsito

Este proyecto implementa un pipeline ETL completo y automatizado que integra datos de accidentes de tránsito con datos geográficos de vías. 

## Objetivos

* Integrar dos fuentes de datos: accidentes históricos (CSV) y datos de vías (API Overpass).
* Transformar y cargar la información en un modelo dimensional relacional.
* Automatizar todo el proceso con Airflow.
* Transmitir los datos a través de Kafka.
* Visualizarlos en tiempo real con Streamlit.

## Herramientas y Tecnologías

* Apache Airflow – Orquestación del pipeline.
* Kafka + Redis – Transmisión y almacenamiento temporal de datos en tiempo real.
* MariaDB – Almacenamiento del modelo dimensional.
* Streamlit – Dashboard interactivo en tiempo real.
* Docker Compose – Contenerización de todos los servicios.
* Python + Pandas – Lógica de transformación de datos.

## Estructura del Proyecto

```
project/
├── .env
├── 04_eda_ovp.ipynb
├── DASHBOARD_ETL_PROJECT.pbit
├── docker-compose.yml
├── requirements.txt
├── vias.py
├── airflow/
│ ├── dags/
│ │ ├── scripts/
│ │ ├── utils/
│ │ ├── init.py
│ │ └── traffic_accidents_etl.py
│ ├── logs/
│ ├── plugins/
│ ├── Dockerfile
│ └── requirements.txt
├── data/
│ ├── processed/
│ └── raw/
│ ├── traffic_accidents.csv
│ └── vias.csv
├── gx/
│ ├── Dockerfile
```

## Pipeline ETL

### Extracción

* Lectura del archivo `traffic_accidents.csv`.
* Consulta a la API de Overpass para obtener información de las vías.

### Transformación

* Limpieza y estandarización de variables categóricas.
* Agrupación por franjas horarias, normalización de nombres de vías.
* Asignación aleatoria de una vía a cada accidente para realizar el merge.

### Carga

* Creación del modelo dimensional en MariaDB.
* Inserción de datos en tablas de hechos y dimensiones con SQLAlchemy + Pandas.

## Modelo Dimensional

* Tabla de hechos: `hechos_accidentes`
* Tablas dimensión: `dim_lesion`, `dim_causa`, `dim_trafico`, `dim_clima`, `dim_tiempo`, `dim_via`

## Ejecución del Proyecto

1. Construir y levantar contenedores:

```bash
docker-compose up --build
```

2. Inicializar Airflow (una vez):

```bash
docker-compose run airflow-init
```

3. Interfaz de Airflow: [http://localhost:8080](http://localhost:8080) 

4. Ejecutar el DAG manualmente o dejarlo programado.

5. Acceder al dashboard Streamlit en: [http://localhost:8501](http://localhost:8501)

## Visualización en Tiempo Real

El dashboard desarrollado con Streamlit se conecta a Redis y consume los datos transmitidos por Kafka, actualizándose automáticamente conforme se reciben nuevos mensajes.

## Requisitos

* Docker & Docker Compose
* Python 3.10+
* Git
