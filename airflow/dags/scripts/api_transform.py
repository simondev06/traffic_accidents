import pandas as pd
import re
import os
from dotenv import load_dotenv

def transform_vias_task():
    load_dotenv(dotenv_path="/opt/airflow/.env")
    output_dir = os.getenv("OUTPUT")

    if not output_dir:
        raise ValueError("Las rutas API_VIAS_PATH y OUTPUT deben estar definidas en el archivo .env")
    input_file = os.path.join(output_dir, "api_extracted.csv")
    output_file = os.path.join(output_dir, "vias_transformed.csv")

    def convert_maxspeed(value):
        if pd.isna(value):
            return None
        match = re.search(r'\d+', str(value))
        if not match:
            return None
        speed = int(match.group())
        if 'mph' in str(value).lower():
            return round(speed * 1.609, 1)
        return speed

    df = pd.read_csv(input_file)
    df['maxspeed_kmh'] = df['maxspeed'].apply(convert_maxspeed)
    df['lit_bin'] = df['lit'].apply(lambda x: 1 if x == 'yes' else 0)
    df['oneway_bin'] = df['oneway'].apply(lambda x: 1 if x == 'yes' else (0 if x == 'no' else None))
    df = df.loc[:, (df != 0).any(axis=0)]

    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    df.to_csv(output_file, index=False)
    print(f"Transformación guardada en: {output_file}")
