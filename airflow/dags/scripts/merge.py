import pandas as pd
import os
from dotenv import load_dotenv

def merge_datasets():
    load_dotenv(dotenv_path="/opt/airflow/.env")
    accidentes_path = os.path.join(os.getenv("OUTPUT"), "dataset_transformed.csv")
    vias_path = os.path.join(os.getenv("OUTPUT"), "vias_transformed.csv")
    output_path = os.path.join(os.getenv("OUTPUT"), "merged.csv")

    if not os.path.exists(accidentes_path):
        raise FileNotFoundError(f"Archivo no encontrado: {accidentes_path}")
    if not os.path.exists(vias_path):
        raise FileNotFoundError(f"Archivo no encontrado: {vias_path}")

    accidentes_df = pd.read_csv(accidentes_path)
    vias_df = pd.read_csv(vias_path)

    merged_df = accidentes_df.merge(vias_df, how='left', on='road_name')

    print("Registros antes del merge:", len(accidentes_df))
    print("Registros después del merge:", len(merged_df))
    print("Columnas nuevas agregadas:", set(merged_df.columns) - set(accidentes_df.columns))

    merged_df.to_csv(output_path, index=False)
    print(f"Merged guardado en {output_path}")

if __name__ == "__main__":
    merge_datasets()
