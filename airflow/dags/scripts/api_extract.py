import pandas as pd
import requests
import time
import os
from dotenv import load_dotenv

def extract_api_data():
    # Cargar variables de entorno
    load_dotenv(dotenv_path="/opt/airflow/.env")
    input_csv_path = os.getenv("API_VIAS_PATH")
    output_dir = os.getenv("OUTPUT")
    radius_meters = int(os.getenv("RADIUS", 51))
    top_features = ['highway', 'surface', 'lit', 'oneway', 'maxspeed']

    if not input_csv_path or not output_dir:
        raise ValueError("Las rutas API_INPUT_PATH y OUTPUT deben estar definidas en el archivo .env")

    output_csv_path = os.path.join(output_dir, "api_extracted.csv")

    def fetch_from_overpass(lat, lon, radius):
        query = f"""
        [out:json][timeout:25];
        way(around:{radius},{lat},{lon});
        out tags;
        """
        url = "https://overpass-api.de/api/interpreter"
        try:
            response = requests.post(url, data={'data': query})
            if response.status_code == 200:
                data = response.json()
                if data.get('elements'):
                    return data['elements'][0].get('tags', {})
        except Exception as e:
            print(f"Error al consultar {lat}, {lon}: {e}")
        return {}

    if not os.path.exists(input_csv_path):
        raise FileNotFoundError(f"No se encontró el archivo de entrada: {input_csv_path}")

    vias = pd.read_csv(input_csv_path)
    records = []
    for _, row in vias.iterrows():
        tags = fetch_from_overpass(row['latitude'], row['longitude'], radius_meters)
        record = {"road_name": row['road_name']}
        for feature in top_features:
            record[feature] = tags.get(feature)
        records.append(record)
        time.sleep(1)

    df_result = pd.DataFrame(records)
    df_result.to_csv(output_csv_path, index=False)
    print(f"Guardado: {output_csv_path}")
