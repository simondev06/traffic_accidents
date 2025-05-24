import pandas as pd
from dotenv import load_dotenv
import os

def extract_dataset():
    # Cargar variables de entorno
    load_dotenv(dotenv_path="/opt/airflow/.env")
    # Obtener rutas desde .env
    csv_path = os.getenv("DATASET_PATH")
    output_dir = os.getenv("OUTPUT")

    if not csv_path or not output_dir:
        raise ValueError("Las rutas DATASET_PATH y OUTPUT deben estar definidas en el archivo .env")

    selected_columns = [
        'crash_date',
        'crash_hour',
        'crash_day_of_week',
        'weather_condition',
        'traffic_control_device',
        'first_crash_type',
        'prim_contributory_cause',
        'num_units',
        'most_severe_injury'
    ]

    try:
        df = pd.read_csv(csv_path)
        missing_cols = [col for col in selected_columns if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Las siguientes columnas no están en el archivo: {missing_cols}")

        df_selected = df[selected_columns]
        output_file = os.path.join(output_dir, "dataset_extracted.csv")
        df_selected.to_csv(output_file, index=False)
        print(f"Archivo guardado exitosamente en: {output_file}")

    except Exception as e:
        print(f"Error durante la extracción del dataset: {e}")
