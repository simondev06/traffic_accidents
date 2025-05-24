import pandas as pd
import numpy as np
import os
import sys
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Ajustar sys.path para importar módulos desde utils
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.connection_db import get_mysql_connection

def run_load_task():
    load_dotenv(dotenv_path="/opt/airflow/.env")
    merged_path = os.path.join(os.getenv("OUTPUT"), "merged.csv")
    if not os.path.exists(merged_path):
        raise FileNotFoundError(f"No se encontró el archivo: {merged_path}")

    def load_merged_csv(path: str) -> pd.DataFrame:
        return pd.read_csv(path)

    def insert_dim_road(df, engine):
        df_unique = df[["road_name", "highway", "surface", "maxspeed_kmh", "lit_bin", "oneway_bin"]].drop_duplicates()
        df_unique = df_unique.replace({np.nan: None})
        with engine.begin() as conn:
            for _, row in df_unique.iterrows():
                conn.execute(text("""
                    INSERT IGNORE INTO dim_road (road_name, highway_type, surface, maxspeed_kmh, lit, oneway)
                    VALUES (:road_name, :highway, :surface, :maxspeed, :lit, :oneway)
                """), {
                    "road_name": row["road_name"],
                    "highway": row["highway"],
                    "surface": row["surface"],
                    "maxspeed": row["maxspeed_kmh"],
                    "lit": row["lit_bin"],
                    "oneway": row["oneway_bin"]
                })
        dim_road_df = pd.read_sql("SELECT * FROM dim_road", engine)
        dim_road_df.rename(columns={"highway_type": "highway", "lit": "lit_bin", "oneway": "oneway_bin"}, inplace=True)
        dim_road_df["lit_bin"] = pd.to_numeric(dim_road_df["lit_bin"], errors="coerce").fillna(0).astype(int)
        dim_road_df["oneway_bin"] = pd.to_numeric(dim_road_df["oneway_bin"], errors="coerce").fillna(0).astype(int)
        return dim_road_df

    def create_dimensional_model_tables(engine):
        statements = [
            "DROP TABLE IF EXISTS fact_accidents",
            "DROP TABLE IF EXISTS dim_time",
            "DROP TABLE IF EXISTS dim_road",
            "DROP TABLE IF EXISTS dim_prim_contributory_cause",
            "DROP TABLE IF EXISTS dim_first_crash_type",
            "DROP TABLE IF EXISTS dim_traffic_control_device",
            "DROP TABLE IF EXISTS dim_weather_condition",
            "DROP TABLE IF EXISTS dim_injury_severity",

            """CREATE TABLE dim_injury_severity (
            id INT AUTO_INCREMENT PRIMARY KEY,
            severity_label VARCHAR(100) UNIQUE
            )""",

            """CREATE TABLE dim_weather_condition (
            id INT AUTO_INCREMENT PRIMARY KEY,
            weather_condition VARCHAR(100) UNIQUE
            )""",

            """CREATE TABLE dim_traffic_control_device (
            id INT AUTO_INCREMENT PRIMARY KEY,
            traffic_control_device VARCHAR(100) UNIQUE
            )""",

            """CREATE TABLE dim_first_crash_type (
            id INT AUTO_INCREMENT PRIMARY KEY,
            first_crash_type VARCHAR(100) UNIQUE
            )""",

            """CREATE TABLE dim_prim_contributory_cause (
            id INT AUTO_INCREMENT PRIMARY KEY,
            prim_contributory_cause VARCHAR(255) UNIQUE
            )""",

            """CREATE TABLE dim_road (
            id INT AUTO_INCREMENT PRIMARY KEY,
            road_name VARCHAR(255),
            highway_type VARCHAR(50),
            surface VARCHAR(50),
            maxspeed_kmh FLOAT,
            lit BOOLEAN,
            oneway BOOLEAN,
            UNIQUE(road_name, highway_type, surface, maxspeed_kmh, lit, oneway)
            )""",

            """CREATE TABLE dim_time (
            id INT AUTO_INCREMENT PRIMARY KEY,
            year INT,
            month INT,
            day INT,
            hour INT,
            day_of_week INT,
            UNIQUE(year, month, day, hour, day_of_week)
            )""",

            """CREATE TABLE fact_accidents (
            id_fact INT AUTO_INCREMENT PRIMARY KEY,
            crash_date DATETIME,
            num_units INT,
            injury_severity_id INT,
            weather_id INT,
            traffic_control_id INT,
            crash_type_id INT,
            cause_id INT,
            road_id INT,
            time_id INT,
            FOREIGN KEY (injury_severity_id) REFERENCES dim_injury_severity(id),
            FOREIGN KEY (weather_id) REFERENCES dim_weather_condition(id),
            FOREIGN KEY (traffic_control_id) REFERENCES dim_traffic_control_device(id),
            FOREIGN KEY (crash_type_id) REFERENCES dim_first_crash_type(id),
            FOREIGN KEY (cause_id) REFERENCES dim_prim_contributory_cause(id),
            FOREIGN KEY (road_id) REFERENCES dim_road(id),
            FOREIGN KEY (time_id) REFERENCES dim_time(id)
            )"""
        ]

        with engine.begin() as conn:
            for stmt in statements:
                conn.execute(text(stmt))


    def insert_dimension_and_get_id(df_value, column_name, table_name, engine):
        with engine.begin() as conn:
            conn.execute(text(f"INSERT IGNORE INTO {table_name} ({column_name}) VALUES (:val)"), [{"val": v} for v in df_value.dropna().unique()])
        return pd.read_sql(f"SELECT id, {column_name} FROM {table_name}", engine)

    def insert_dim_time(df, engine):
        df_time = df[["crash_year", "crash_month_num", "crash_day", "crash_hour", "crash_day_of_week"]].drop_duplicates()
        df_time.columns = ["year", "month", "day", "hour", "day_of_week"]
        with engine.begin() as conn:
            for _, row in df_time.iterrows():
                conn.execute(text("""
                    INSERT IGNORE INTO dim_time (year, month, day, hour, day_of_week)
                    VALUES (:year, :month, :day, :hour, :day_of_week)
                """), row.to_dict())
        return pd.read_sql("SELECT * FROM dim_time", engine)

    def load_fact_table(df, engine):
        dims = {
            "injury_severity": insert_dimension_and_get_id(df["most_severe_injury"].astype(str), "severity_label", "dim_injury_severity", engine),
            "weather": insert_dimension_and_get_id(df["weather_condition"], "weather_condition", "dim_weather_condition", engine),
            "traffic": insert_dimension_and_get_id(df["traffic_control_device"], "traffic_control_device", "dim_traffic_control_device", engine),
            "crash_type": insert_dimension_and_get_id(df["first_crash_type"], "first_crash_type", "dim_first_crash_type", engine),
            "cause": insert_dimension_and_get_id(df["prim_contributory_cause"], "prim_contributory_cause", "dim_prim_contributory_cause", engine),
            "road": insert_dim_road(df, engine),
            "time": insert_dim_time(df, engine)
        }

        df_fact = df.merge(dims["injury_severity"], left_on="most_severe_injury", right_on="severity_label")
        df_fact.rename(columns={"id": "id_injury"}, inplace=True)
        df_fact = df_fact.merge(dims["weather"], on="weather_condition")
        df_fact.rename(columns={"id": "id_weather"}, inplace=True)
        df_fact = df_fact.merge(dims["traffic"], on="traffic_control_device")
        df_fact.rename(columns={"id": "id_traffic"}, inplace=True)
        df_fact = df_fact.merge(dims["crash_type"], on="first_crash_type")
        df_fact.rename(columns={"id": "id_crash"}, inplace=True)
        df_fact = df_fact.merge(dims["cause"], on="prim_contributory_cause")
        df_fact.rename(columns={"id": "id_cause"}, inplace=True)
        df_fact = df_fact.merge(dims["road"], on=["road_name", "highway", "surface", "maxspeed_kmh", "lit_bin", "oneway_bin"])
        df_fact.rename(columns={"id": "id_road"}, inplace=True)
        df_fact = df_fact.merge(dims["time"], left_on=["crash_year", "crash_month_num", "crash_day", "crash_hour", "crash_day_of_week"], right_on=["year", "month", "day", "hour", "day_of_week"])
        df_fact.rename(columns={"id": "id_time"}, inplace=True)

        df_fact["crash_date"] = pd.to_datetime(df_fact[["crash_year", "crash_month_num", "crash_day", "crash_hour"]].astype(str).agg(" ".join, axis=1), errors="coerce", format="%Y %m %d %H")
        to_insert = df_fact[["crash_date", "num_units", "id_injury", "id_weather", "id_traffic", "id_crash", "id_cause", "id_road", "id_time"]]
        to_insert.columns = ["crash_date", "num_units", "injury_severity_id", "weather_id", "traffic_control_id", "crash_type_id", "cause_id", "road_id", "time_id"]
        to_insert = to_insert.dropna(subset=["crash_date"])
        to_insert.to_sql("fact_accidents", con=engine, if_exists="append", index=False)

    engine = get_mysql_connection()
    df = load_merged_csv(merged_path)
    create_dimensional_model_tables(engine)
    load_fact_table(df, engine)
