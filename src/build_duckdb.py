# src/build_duckdb.py
from __future__ import annotations

import json
import os
import zipfile
from pathlib import Path

import duckdb

DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "raw"
EXTRACT_DIR = DATA_DIR / "extracted"
DB_PATH = DATA_DIR / "gtfs.duckdb"

GTFS_FILES = [
    "agency.txt",
    "stops.txt",
    "routes.txt",
    "trips.txt",
    "stop_times.txt",
    "calendar.txt",
    "calendar_dates.txt",
    "shapes.txt",
    "frequencies.txt",
    "transfers.txt",
    "pathways.txt",
    "feed_info.txt",
]

def find_latest_zip() -> Path:
    latest_meta = RAW_DIR / "latest.json"
    if latest_meta.exists():
        meta = json.loads(latest_meta.read_text(encoding="utf-8"))
        p = RAW_DIR / meta["filename"]
        if p.exists():
            return p

    zips = sorted(RAW_DIR.glob("*.zip"), key=lambda p: p.stat().st_mtime)
    if not zips:
        raise RuntimeError("No hay ZIPs en data/raw. Corre primero: python src/ingest_gtfs.py")
    return zips[-1]

def extract_zip(zip_path: Path) -> Path:
    out_dir = EXTRACT_DIR / zip_path.stem
    out_dir.mkdir(parents=True, exist_ok=True)

    # Extrae solo si no están los archivos
    with zipfile.ZipFile(zip_path, "r") as z:
        names = set(z.namelist())
        needs = any((out_dir / f).exists() is False for f in GTFS_FILES if f in names)
        if needs:
            z.extractall(out_dir)
    return out_dir

def load_to_duckdb(extracted_dir: Path):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    con.execute("CREATE SCHEMA IF NOT EXISTS gtfs;")

    # Ajustes útiles
    con.execute("PRAGMA threads=4;")
    con.execute("PRAGMA enable_progress_bar=true;")

    for fname in GTFS_FILES:
        fpath = extracted_dir / fname
        if not fpath.exists():
            print(f"No existe {fname} en este feed, lo salto.")
            continue

        table = "gtfs." + fname.replace(".txt", "")
        print(f"🧱 Cargando {fname} -> {table}")

        # all_varchar=true: evita que stop_id/route_id por accidente queden numéricos
        con.execute(f"""
            CREATE OR REPLACE TABLE gtfs.{table} AS
            SELECT *
            FROM read_csv_auto(
                '{str(fpath).replace("'", "''")}',
                header=true,
                all_varchar=true
            );
        """)

    # metadata simple
    con.execute("""
    CREATE OR REPLACE TABLE gtfs.gtfs._meta (
        loaded_at TIMESTAMP,
        extracted_dir VARCHAR
        );
    """)

    con.execute(
        "INSERT INTO gtfs.gtfs._meta VALUES (now(), ?);",
        [str(extracted_dir)]
    )

    con.close()
    print(f"✅ DuckDB listo en: {DB_PATH}")

if __name__ == "__main__":
    zip_path = find_latest_zip()
    print(f"📦 Usando ZIP: {zip_path}")
    extracted = extract_zip(zip_path)
    print(f"📂 Extraído en: {extracted}")
    load_to_duckdb(extracted)
