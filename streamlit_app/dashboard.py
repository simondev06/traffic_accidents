import streamlit as st
import redis
import pandas as pd
import json

redis_client = redis.Redis(host='redis', port=6379, db=0)

st.set_page_config(page_title="Traffic Accidents Dashboard", layout="wide")
st.title("dash en tiemop real")

data = redis_client.lrange("accidents", -500, -1)

if not data:
    st.warning("No hay datos recibidos del topic aún.")
else:
    rows = [json.loads(d) for d in data]
    df = pd.DataFrame(rows)

    st.metric("Mensajes recibidos", len(df))

    with st.expander("Vista de tabla"):
        st.dataframe(df.tail(100))

    col1, col2 = st.columns(2)

    with col1:
        if 'weather_condition' in df.columns:
            st.subheader("🌦 Condiciones climáticas")
            st.bar_chart(df['weather_condition'].value_counts())

        if 'lighting_condition' in df.columns:
            st.subheader("💡 Condiciones de iluminación")
            st.bar_chart(df['lighting_condition'].value_counts())

        if 'alignment' in df.columns:
            st.subheader("🛣 Alineación de la vía")
            st.bar_chart(df['alignment'].value_counts())

    with col2:
        if 'first_crash_type' in df.columns:
            st.subheader("🚧 Tipo de choque")
            st.bar_chart(df['first_crash_type'].value_counts())

        if 'prim_contributory_cause' in df.columns:
            st.subheader("⚠️ Causa principal")
            st.bar_chart(df['prim_contributory_cause'].value_counts())

        if 'most_severe_injury' in df.columns:
            st.subheader("🩺 Tipo de lesión más grave")
            st.bar_chart(df['most_severe_injury'].value_counts())

    st.subheader("Distribución de accidentes por hora")
    if 'crash_hour' in df.columns:
        st.line_chart(df['crash_hour'].value_counts().sort_index())

    st.subheader("Accidentes por día de la semana")
    if 'crash_day_of_week' in df.columns:
        st.bar_chart(df['crash_day_of_week'].value_counts().sort_index())