import pandas as pd
import random
from dotenv import load_dotenv
import os

def transform_dataset():
    # Cargar variables de entorno
    load_dotenv(dotenv_path="/opt/airflow/.env")
    # Obtener rutas desde .env
    input_file = os.getenv("DATASET_PATH")
    vias_path = os.getenv("API_VIAS_PATH")
    output_dir = os.getenv("OUTPUT")

    if not input_file or not vias_path or not output_dir:
        raise ValueError("Las rutas DATASET y API_VIAS_PATH deben estar definidas en el archivo .env")

    output_file = os.path.join(output_dir, "dataset_transformed.csv")

    try:
        df = pd.read_csv(input_file)
        df_vias = pd.read_csv(vias_path)
        vias = df_vias['road_name'].dropna().unique().tolist()

        random.seed(42)
        df['road_name'] = [random.choice(vias) for _ in range(len(df))]

        df['crash_date'] = pd.to_datetime(df['crash_date'], errors='coerce')
        df['crash_year'] = df['crash_date'].dt.year
        df['crash_month_num'] = df['crash_date'].dt.month
        df['crash_day'] = df['crash_date'].dt.day
        df.drop(columns='crash_date', inplace=True)

        text_columns = [
            'weather_condition',
            'traffic_control_device',
            'first_crash_type',
            'prim_contributory_cause',
            'most_severe_injury'
        ]
        for col in text_columns:
            df[col] = df[col].astype(str).str.strip().str.upper()

        df.to_csv(output_file, index=False)
        print(f"Transformación completada y guardada en {output_file}")

    except Exception as e:
        print(f"Error durante la transformación del dataset: {e}")
