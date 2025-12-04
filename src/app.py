# src/app.py
from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

import folium
from streamlit_folium import st_folium

DB_PATH = Path("data/gtfs.duckdb")

st.set_page_config(page_title="GTFS Santiago – Analytics", layout="wide")

@st.cache_resource
def get_con():
    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"No existe {DB_PATH}. Corre primero:\n"
            f"  python src/ingest_gtfs.py\n"
            f"  python src/build_duckdb.py"
        )
    return duckdb.connect(str(DB_PATH), read_only=True)

con = get_con()

def qdf(sql: str, params=None) -> pd.DataFrame:
    return con.execute(sql, params or []).df()

st.title("🚌 GTFS Santiago – Analytics (MVP)")

with st.sidebar:
    st.header("Controles")
    routes_df = qdf("""
        SELECT route_id, route_short_name, route_long_name
        FROM gtfs.gtfs.routes
        ORDER BY
            CASE WHEN route_short_name IS NULL OR route_short_name = '' THEN 1 ELSE 0 END,
            route_short_name, route_long_name, route_id
    """)
    routes_df["label"] = routes_df.apply(
        lambda r: f"{r['route_short_name']} — {r['route_long_name']}" if (r["route_short_name"] or "") else f"{r['route_long_name']} ({r['route_id']})",
        axis=1
    )
    route_label = st.selectbox("Ruta", routes_df["label"].tolist())
    route_id = routes_df.loc[routes_df["label"] == route_label, "route_id"].iloc[0]

    direction_filter = st.selectbox("Dirección (si aplica)", ["Ambas", "0", "1"])
    show_shapes = st.checkbox("Dibujar shape (si existe)", value=True)
    show_stops = st.checkbox("Mostrar paradas", value=True)

# --- Datos base: trips de la ruta ---
trips_sql = """
    SELECT trip_id, direction_id, shape_id, service_id
    FROM gtfs.gtfs.trips
    WHERE route_id = ?
"""
trips_params = [route_id]
trips = qdf(trips_sql, trips_params)

if direction_filter != "Ambas":
    trips = trips[trips["direction_id"].astype(str) == direction_filter]

if trips.empty:
    st.warning("No encontré viajes (trips) para esa ruta/filtro.")
    st.stop()

# Un trip representativo para el mapa (primer trip)
trip_id = trips["trip_id"].iloc[0]
shape_id = trips["shape_id"].iloc[0] if "shape_id" in trips.columns else None

# --- Métricas rápidas ---
col1, col2, col3, col4 = st.columns(4)

n_trips = len(trips)
n_stops = qdf("""
    SELECT COUNT(DISTINCT st.stop_id) AS n
    FROM gtfs.gtfs.stop_times st
    WHERE st.trip_id IN (SELECT trip_id FROM gtfs.gtfs.trips WHERE route_id = ?)
""", [route_id]).iloc[0]["n"]

# Rango de horas aproximado usando stop_times del trip representativo
time_span = qdf("""
    SELECT
      MIN(arrival_time) AS first_time,
      MAX(departure_time) AS last_time
    FROM gtfs.gtfs.stop_times
    WHERE trip_id = ?
""", [trip_id])

first_time = time_span["first_time"].iloc[0]
last_time = time_span["last_time"].iloc[0]

col1.metric("Trips (ruta)", f"{n_trips:,}")
col2.metric("Paradas únicas (ruta)", f"{int(n_stops):,}")
col3.metric("Ejemplo: primera hora", str(first_time))
col4.metric("Ejemplo: última hora", str(last_time))

st.divider()

# --- Datos para mapa ---
# Paradas del trip representativo (ordenadas por stop_sequence)
stops = qdf("""
    SELECT
      s.stop_id,
      s.stop_name,
      TRY_CAST(s.stop_lat AS DOUBLE) AS stop_lat,
      TRY_CAST(s.stop_lon AS DOUBLE) AS stop_lon,
      stp.stop_sequence
    FROM gtfs.gtfs.stop_times stp
    JOIN gtfs.gtfs.stops s ON s.stop_id = stp.stop_id
    WHERE stp.trip_id = ?
    ORDER BY stp.stop_sequence
""", [trip_id]).dropna(subset=["stop_lat", "stop_lon"])

# Shape (si existe en feed)
shape_points = None
if show_shapes and shape_id is not None and str(shape_id) != "" and "gtfs.shapes" in set(qdf("SHOW TABLES")["name"].tolist()):
    shape_points = qdf("""
        SELECT
          TRY_CAST(shape_pt_lat AS DOUBLE) AS lat,
          TRY_CAST(shape_pt_lon AS DOUBLE) AS lon,
          TRY_CAST(shape_pt_sequence AS INTEGER) AS seq
        FROM gtfs.gtfs.shapes
        WHERE shape_id = ?
        ORDER BY seq
    """, [shape_id]).dropna(subset=["lat", "lon"])

# Centro del mapa
if not stops.empty:
    center_lat = float(stops["stop_lat"].mean())
    center_lon = float(stops["stop_lon"].mean())
elif shape_points is not None and not shape_points.empty:
    center_lat = float(shape_points["lat"].mean())
    center_lon = float(shape_points["lon"].mean())
else:
    center_lat, center_lon = -33.45, -70.65  # fallback Santiago

m = folium.Map(location=[center_lat, center_lon], zoom_start=12, tiles="CartoDB positron")

# Dibuja shape
if show_shapes and shape_points is not None and not shape_points.empty:
    folium.PolyLine(
        locations=shape_points[["lat", "lon"]].values.tolist(),
        weight=4,
        opacity=0.9
    ).add_to(m)

# Dibuja paradas
if show_stops and not stops.empty:
    for _, r in stops.iterrows():
        folium.CircleMarker(
            location=[float(r["stop_lat"]), float(r["stop_lon"])],
            radius=4,
            popup=f"{int(r['stop_sequence'])}. {r['stop_name']} ({r['stop_id']})",
            fill=True
        ).add_to(m)

left, right = st.columns([2, 1], gap="large")

with left:
    st.subheader("🗺️ Mapa")
    st_folium(m, height=650, use_container_width=True)

with right:
    st.subheader("📋 Trip representativo")
    st.caption("Este MVP toma 1 trip de ejemplo para mostrar paradas/shape. Luego lo extendemos a métricas por hora/día.")
    st.write({
        "route_id": route_id,
        "trip_id": trip_id,
        "direction_id": str(trips["direction_id"].iloc[0]),
        "shape_id": str(shape_id),
        "service_id (ejemplo)": str(trips["service_id"].iloc[0]),
    })

    st.subheader("Paradas (en orden)")
    if stops.empty:
        st.info("No hay paradas para el trip seleccionado.")
    else:
        st.dataframe(
            stops[["stop_sequence", "stop_name", "stop_id"]].reset_index(drop=True),
            use_container_width=True,
            height=350
        )

st.divider()

with st.expander("🔎 Diagnóstico rápido del feed"):
    tbls = qdf("SHOW TABLES")
    st.write("Tablas cargadas:", ", ".join(tbls["name"].tolist()))
    meta = qdf("SELECT * FROM gtfs.gtfs._meta")
    st.dataframe(meta, use_container_width=True)
