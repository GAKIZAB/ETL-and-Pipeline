# 🌦️ Weather ETL Pipeline

A **production-ready** Extract → Transform → Load pipeline that collects current weather data for French cities from the [Open-Meteo API](https://open-meteo.com/) and stores it in both CSV files and a SQLite database.

---

## 📂 Project Structure

```
weather_etl_project/
├── config/
│   └── cities.yaml          # City coordinates, API params, paths, schedule
├── data/                    # (created at runtime) CSV exports & SQLite DB
├── logs/                    # (created at runtime) Rotating log files
├── weather_etl/             # Python package
│   ├── __init__.py
│   ├── extract.py           # API calls with retry & back-off
│   ├── transform.py         # JSON → pandas DataFrame
│   ├── load.py              # CSV + SQLite persistence
│   └── utils.py             # Config loader, logging, helpers
├── tests/                   # pytest unit tests
│   ├── test_extract.py
│   ├── test_transform.py
│   ├── test_load.py
│   └── test_utils.py
├── main.py                  # CLI entry point
├── Dockerfile
├── requirements.txt
├── .gitignore
└── README.md
```


---
