# src/ingest_gtfs.py
from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

DTPM_GTFS_PAGE = os.getenv("DTPM_GTFS_PAGE", "https://www.dtpm.cl/index.php/gtfs-vigente")
# Si cambia el HTML, se setea
# export GTFS_ZIP_URL="https://www.dtpm.cl/descargas/gtfs/GTFS_YYYYMMDD.zip"
GTFS_ZIP_URL_OVERRIDE = os.getenv("GTFS_ZIP_URL", "").strip()

DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "gtfs-santiago-analytics/0.1 (+github)"
})

def _extract_date_from_filename(name: str) -> str | None:
    m = re.search(r"GTFS_(\d{8})", name)
    return m.group(1) if m else None

def find_gtfs_zip_url() -> str:
    if GTFS_ZIP_URL_OVERRIDE:
        return GTFS_ZIP_URL_OVERRIDE

    resp = SESSION.get(DTPM_GTFS_PAGE, timeout=60)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")

    candidates: list[str] = []
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if not href:
            continue
        if ".zip" in href.lower():
            candidates.append(urljoin(DTPM_GTFS_PAGE, href))

    # regex por si el HTML cambia
    if not candidates:
        candidates = re.findall(r"https?://[^\s\"']+\.zip", resp.text, flags=re.IGNORECASE)

    if not candidates:
        raise RuntimeError(f"No se encuentran links .zip en {DTPM_GTFS_PAGE}")

    # Normaliza / dedup
    candidates = sorted(set(candidates))

    # Elige el más reciente por fecha en filename si existe, si no el último
    def key(u: str):
        fname = u.split("/")[-1]
        d = _extract_date_from_filename(fname) or "00000000"
        return (d, fname)

    chosen = sorted(candidates, key=key)[-1]
    return chosen

def download_zip(url: str) -> Path:
    filename = url.split("/")[-1].split("?")[0] or "gtfs.zip"
    out_path = RAW_DIR / filename

    # Descarga streaming (no revienta RAM)
    with SESSION.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        tmp = out_path.with_suffix(out_path.suffix + ".part")
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
        tmp.replace(out_path)

    # sanity check ZIP (magic bytes PK)
    with open(out_path, "rb") as f:
        magic = f.read(2)
    if magic != b"PK":
        raise RuntimeError(f"El archivo descargado no parece ZIP (magic={magic!r}): {out_path}")

    meta = {
        "source_page": DTPM_GTFS_PAGE,
        "zip_url": url,
        "filename": out_path.name,
        "downloaded_at_utc": datetime.now(timezone.utc).isoformat(),
        "size_bytes": out_path.stat().st_size,
    }
    (RAW_DIR / "latest.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    return out_path

if __name__ == "__main__":
    print(f"🔎 Buscando GTFS ZIP en: {DTPM_GTFS_PAGE}")
    url = find_gtfs_zip_url()
    print(f"⬇️  Descargando: {url}")
    t0 = time.time()
    path = download_zip(url)
    print(f"✅ Listo: {path} ({path.stat().st_size/1024/1024:.1f} MB) en {time.time()-t0:.1f}s")
