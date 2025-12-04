# GTFS Santiago – Analytics

Pipeline + dashboard to compute public transport service metrics from GTFS:
- stop coverage (walk buffers)
- service span (first/last trips)
- frequency/headways by time-of-day (planned)

## Quickstart
```bash
pip install -r requirements.txt
python src/ingest_gtfs.py
python src/build_duckdb.py
streamlit run src/app.py