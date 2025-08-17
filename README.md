**CAUTION: Work In Progress**

# 🛫 WingWatch: Real-Time Flight Data Pipeline
![Medallion Architecture](https://img.shields.io/badge/architecture-medallion%20(bronze%2C%20silver%2Cized](https://img.shields.io/badge/deployment-docker](https://img.shields.io/badge/orchestration-airflowimg.shields.io/badge/transgres](https://img.shields.io/badge/warehouse-postgres://img.shields.io/badge/raw 🧠 Aviation. Ingested. Modeled. Analyzed.
**WingWatch** is a **real-time aviation analytics platform** that ingests **OpenSky flight telemetry, airports, airlines, cities, and weather data** and transforms it into business-ready insights.

It’s a **data engineering playground** designed around the **medallion architecture** (Bronze → Silver → Gold), where raw chaos from APIs & CSVs becomes structured, validated, and transformed aviation intelligence.

Think of it as **turning the sky into data-driven dashboards** ✨.

***

## 🚀 Project Highlights

- 📡 **Real-time ingestion**: Collects live flight positions and airline metadata.
- 🔁 **End-to-end orchestration**: Airflow DAGs manage ingestion, validation, transformation, and analytics.
- 🏗️ **Medallion Architecture**: Bronze (raw), Silver (refined), Gold (analytics).
- 🧪 **Data Quality built-in**: Configurable checks (NULL, UNIQUE, RANGE, DUPLICATES).
- 📊 **Analytical depth**: Airspace monitoring (approaching flights, emergencies, phases, altitude bands).
- 🐳 **Production Deployment**: Fully dockerized stack with Airflow, Postgres, MinIO, Redis, pgAdmin.

***

## 🛠️ Tech Stack

| Component         | Technology                          | Role                                |
| ----------------- | ----------------------------------- | ----------------------------------- |
| **Orchestration** | Apache Airflow 2.10.0               | DAG scheduling & monitoring          |
| **Transformations** | dbt-core + dbt-postgres           | SQL-based transformation & modeling |
| **Raw Storage**   | MinIO                               | Object storage for APIs & datasets  |
| **Warehouse**     | PostgreSQL 13                       | Bronze, Silver, Gold data schemas   |
| **Admin UI**      | pgAdmin                             | PostgreSQL GUI client               |
| **Deployment**    | Docker Compose                      | Multi-container orchestration       |
| **Data APIs**     | OpenSky, OpenWeather, Airlines DB   | External aviation datasets          |

***

## 📊 Data Architecture

```mermaid
graph LR
    A[External APIs: OpenSky, Airlines, Airports, Cities, Weather] -->|Raw JSON/CSV| B[MinIO - Bronze Zone]
    B --> C{Airflow DAGs}
    C -->|Ingestion (Python + PostgresHook)| D[PostgreSQL - Bronze Schema]
    D -->|dbt Silver models| E[PostgreSQL - Silver Schema]
    E -->|dbt Gold models| F[PostgreSQL - Gold Schema]
    F --> G[BI/Analytics Dashboards]
    subgraph Docker Environment
    B
    C
    D
    E
    F
    end
```

***

## 🧱 Medallion Layers

### 1️⃣ **Bronze (Raw Ingestion)**
- Sources: OpenSky API, Airports DB, Airlines, Cities, Weather.
- Stored in **MinIO (JSON/CSV)**.
- Ingested into Postgres `bronze.*` tables via **Airflow DAG → MinIO → Postgres**.

### 2️⃣ **Silver (Cleaned + Standardized)**
- dbt models transform Bronze → Silver.
- Cleans nulls, enforces types, adds metadata.
- Tables: `silver_airports`, `silver_airlines`, `silver_flights`, `silver_cities`.

### 3️⃣ **Gold (Analytics + Aggregations)**
- dbt models aggregate Silver into analytics-ready tables:
  - ✈️ **Approaching Aircrafts** near airports
  - 🚨 **Emergency Events** (squawk 7500/7600/7700)
  - 📈 **Flight Phases** (Takeoff, Climb, Cruise, Descent, Landing)
  - 🛳️ **Flights by Airline, Supersonic, Rare Aircrafts**

***

## 📂 Directory Structure

```
.
├── dags/                  # Airflow DAGs
│   ├── raw_minio_data_ingestion.py
│   ├── bronze_postgres_data_ingestion.py
│   ├── silver_postgres_deployment.py
│   ├── gold_realtime_flights.py
│   ├── master_data_pipeline_orchestration.py
│   └── utilities/          # Utilities: ingestion, DQ, logging
├── dbt/
│   ├── models/
│   │   ├── silver/*.sql
│   │   └── gold/*.sql
│   ├── macros/
│   ├── profiles.yml
│   └── dbt_project.yml
├── sql_scripts/            # Table DDLs (bronze, silver, gold, admin)
├── scripts/                # Init scripts (create dbs, schemas, minio)
├── docker-compose.yml
├── Dockerfile.airflow
├── requirements.txt
└── README.md
```

***

## ⚙️ Getting Started

### ✅ Prerequisites
- Docker & Docker Compose
- `.env` file with credentials:

```ini
POSTGRES_USER=asad
POSTGRES_PASSWORD=asad
POSTGRES_DB=airflow
MINIO_ROOT_USER=minio_admin
MINIO_ROOT_PASSWORD=minio_password
REDIS_PASSWORD=redis_pass
OPENWEATHER_API_KEY=your_api_key
```

### 📥 Installation
```bash
git clone https://github.com/your-org/avnalytics.git
cd avnalytics
docker-compose up -d --build
```

### 🌐 Service Endpoints

| Service   | URL                              | Credentials   |
|-----------|----------------------------------|---------------|
| Airflow   | [http://localhost:8080](http://localhost:8080) | `airflow / airflow` |
| MinIO     | [http://localhost:9001](http://localhost:9001) | from `.env` |
| pgAdmin   | [http://localhost:5050](http://localhost:5050) | from `.env` |
| Postgres  | `localhost:5432`                 | from `.env`   |

***

## 🔍 Sample Insights

- Which flights are **approaching a destination airport now**?
- Are there planes broadcasting an **emergency squawk**?
- What percentage of flights are **climbing, cruising, or landing**?
- Which airlines operate the **most supersonic or rare aircraft types**?

***

## 🛤 Roadmap

- [ ] Add **real-time streaming ingestion** (Kafka → MinIO → Postgres).
- [ ] Build **Superset/Metabase dashboards** on Gold layer.
- [ ] Expand to **weather-integrated flight risk analysis**.
- [ ] Implement **alerts & notifications** (Slack/Email) for DQ failures or emergencies.

***

## 📜 License
MIT License

***

## 💡 Why This Matters
WingWatch demonstrates how **real aviation data** can be **transformed into live insights** using production-grade data engineering principles:
- Orchestration (Airflow)
- Storage (MinIO -> Postgres)
- Transformations (dbt)
- Observability (DQ checks + admin logs)

This isn’t simulation. **It’s the sky, modeled and analyzed.** 🌍✈️
