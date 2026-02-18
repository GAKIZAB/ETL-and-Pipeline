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

## 🚀 Quick Start

### 1. Clone & enter the project

```bash
cd weather_etl_project
```

### 2. Create a virtual environment

```bash
python -m venv venv
# Windows
venv\Scripts\activate
# macOS / Linux
source venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Run the pipeline (one-shot)

```bash
python main.py
```

### 5. Run on a schedule

```bash
python main.py --schedule
```

The schedule interval is configured in `config/cities.yaml` (default: **60 minutes**).

### 6. Point to a custom config

```bash
python main.py --config /path/to/my_config.yaml
# or via environment variable
set WEATHER_ETL_CONFIG=/path/to/my_config.yaml   # Windows
export WEATHER_ETL_CONFIG=/path/to/my_config.yaml # Linux / macOS
python main.py
```

---

## 🔧 Configuration

All settings live in **`config/cities.yaml`**:

| Section    | Key                | Description                           |
|------------|--------------------|---------------------------------------|
| `api`      | `base_url`         | Open-Meteo forecast endpoint          |
| `api`      | `timeout_seconds`  | HTTP request timeout                  |
| `api`      | `max_retries`      | Retry attempts per city               |
| `api`      | `backoff_factor`   | Exponential back-off multiplier       |
| `cities`   | List of dicts      | `name`, `latitude`, `longitude`       |
| `paths`    | `data_dir`         | Where CSV files are saved             |
| `paths`    | `log_dir`          | Where rotating logs are written       |
| `paths`    | `database`         | SQLite database path                  |
| `schedule` | `interval_minutes` | Minutes between scheduled runs        |

---

## 🧪 Testing

```bash
pytest tests/ -v
```

All tests use **mocks** for HTTP calls and **temporary directories** for file I/O, so nothing external is required.

---

## 🐳 Docker

### Build

```bash
docker build -t weather-etl .
```

### Run (one-shot)

```bash
docker run --rm weather-etl
```

### Run (scheduled)

```bash
docker run -d --name weather-etl weather-etl python main.py --schedule
```

### Persist data on the host

```bash
docker run --rm -v $(pwd)/data:/app/data -v $(pwd)/logs:/app/logs weather-etl
```

---

## 📊 Querying the Database

```bash
sqlite3 data/weather.db "SELECT * FROM weather_current ORDER BY retrieval_timestamp DESC LIMIT 10;"
```

Or from Python:

```python
import sqlite3, pandas as pd

with sqlite3.connect("data/weather.db") as conn:
    df = pd.read_sql("SELECT * FROM weather_current", conn)
print(df)
```

---

## ⚙️ Architecture & Design Decisions

| Aspect              | Choice                                                         |
|----------------------|----------------------------------------------------------------|
| **Retry strategy**   | Exponential back-off (configurable factor) via pure Python    |
| **Partial failures** | One failed city does not abort the pipeline                    |
| **Logging**          | Rotating file handler (5 MB × 5 backups) + console            |
| **Storage**          | Dual-write: timestamped CSV for auditability, SQLite for queries |
| **Scheduling**       | `schedule` library (in-process); easily replaced with cron     |
| **Config**           | YAML file + env-var override for portability                   |

---

## 🗓️ Cron Alternative

Instead of `--schedule`, add a system cron entry:

```cron
0 * * * * cd /path/to/weather_etl_project && /path/to/venv/bin/python main.py >> /dev/null 2>&1
```

---

## 📝 License

This project is provided for educational and internal use.
