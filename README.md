# Automated Coffee Sales ETL Pipeline

An end-to-end, automated ETL pipeline that extracts raw coffee sales data, validates and cleans it with **PySpark**, and loads it into **MongoDB Atlas**, orchestrated by **Apache Airflow**. Data quality is enforced with **Great Expectations**.

![Airflow DAG](P2M3_raka_airlangga_DAG_graph.jpg.png)

---

## Table of Contents
1. [Project Overview](#project-overview)
2. [Architecture](#architecture)
3. [Tech Stack](#tech-stack)
4. [Dataset](#dataset)
5. [Pipeline Steps](#pipeline-steps)
6. [Data Validation](#data-validation)
7. [Orchestration](#orchestration)
8. [Results](#results)
9. [Repository Structure](#repository-structure)
10. [How to Run](#how-to-run)
11. [Key Takeaways](#key-takeaways)
12. [Author](#author)

---

## Project Overview

**Problem:** Raw sales data is often messy (inconsistent column names, missing values, wrong data types) and processing it by hand is slow and error-prone.

**Solution:** A scheduled pipeline that automatically ingests the raw CSV, cleans and standardizes it, validates quality, and stores analysis-ready documents in a NoSQL database.

**Outcome:** Replaced manual data preparation with a fully automated, scheduled pipeline that cleans coffee sales data, passes all 7 data quality checks, and stores analysis-ready records in MongoDB Atlas.

## Architecture

```
coffe_sales_raw.csv
        │
        ▼
 ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
 │  Extract    │ ──▶ │  Transform  │ ──▶ │    Load     │
 │ (PySpark)   │     │ (PySpark)   │     │ (PyMongo)   │
 └─────────────┘     └─────────────┘     └─────────────┘
        │                                       │
        └──── Orchestrated by Apache Airflow ───┘
                                                ▼
                                        MongoDB Atlas
```

## Tech Stack

| Layer | Tool |
|---|---|
| Language | Python |
| Processing | PySpark |
| Orchestration | Apache Airflow |
| Containerization | Docker / Docker Compose (runs Airflow and the Spark jobs) |
| Data Validation | Great Expectations |
| Database | MongoDB Atlas (PyMongo) |
| Exploration | Jupyter Notebook, Pandas |

## Dataset

- **Name:** Coffee Sales
- **Source:** https://www.kaggle.com/datasets/rifatalam3/coffee-revenue-dataset
- **Size:** 3548 rows × 11 columns
- **Key columns:** date, money, cash_type, coffee_name

## Pipeline Steps

| Step | File | Description |
|---|---|---|
| Extract | `extract.py` | Reads the raw CSV into a Spark DataFrame |
| Transform | `transform.py` | Renames columns to a consistent snake_case style (e.g. `cash_type` → `payment_type`, `money` → `total_price`), trims fractional seconds from `time_details` (format `HH:mm:ss`), casts `date` to a proper date type (`yyyy-MM-dd`), and removes duplicate rows |
| Load | `load.py` | Converts `date` to a string (Spark timestamps are not directly storable in MongoDB), converts each row to a document, and inserts them into the `coffee_sales` collection in the `coffee_sales_db` database via PyMongo |

## Data Validation

Before building the pipeline, the raw data was explored and validated in `P2M3_raka_airlangga_GX.ipynb` using Great Expectations. The findings from this step defined the cleaning rules implemented in `transform.py`. The notebook is a development step and is not a task inside the DAG. All **7 expectations pass**:

- hour_of_day: Validate that the minimum value is 6 and the maximum value is 22.
- payment_type: Validate that the column contains only one unique value, card.
- coffee_name: Validate that the column contains no null values.
- total_price: Validate that all values are greater than 0.
- date: Validate that all values contain valid date values.
- sales_id: Validate that all values are unique.
- id: Validate that the id column is included in the fact_sales DataFrame.

## Orchestration

The Airflow DAG (`P2M3_raka_airlangga_DAG.py.py`) chains three tasks: **Extract → Transform → Load**.

- **Schedule:** every Saturday, 09:10–09:30 AM, every 10 minutes
- **Start date:** 1 November 2024

## Results

**Successful DAG run**

![DAG graph](P2M3_raka_airlangga_DAG_graph.jpg.png)

**Data stored in MongoDB Atlas**

![MongoDB](P2M3_raka_airlangga_screenshot_mongo.png)

## Repository Structure

```
ETL-Portofolio/
├── coffe_sales_raw.csv                      # Raw dataset
├── extract.py                               # Extract step (PySpark)
├── transform.py                             # Transform step (PySpark)
├── load.py                                  # Load step (MongoDB)
├── P2M3_raka_airlangga_DAG.py.py            # Airflow DAG
├── P2M3_raka_airlangga_GX.ipynb             # EDA + Great Expectations
├── P2M3_raka_airlangga_DAG_graph.jpg.png    # DAG screenshot
├── P2M3_raka_airlangga_screenshot_mongo.png # MongoDB screenshot
└── README.md
```

## How to Run

```bash
# 1. Clone the repository
git clone https://github.com/airlangga-08/ETL-Portofolio.git
cd ETL-Portofolio

# 2. Set your MongoDB connection string (never commit credentials)
export MONGO_URI="your-mongodb-atlas-uri"

# 3. Start Airflow with Docker Compose
docker compose up -d

# 4. Open the Airflow UI at http://localhost:8080,
#    enable the DAG and trigger it (or wait for the schedule)
```

The pipeline runs inside Docker containers, so no local Spark or Airflow installation is needed. Data is exchanged between tasks through the mounted folder `/opt/airflow/data` (`sales_extracted` → `sales_transformed`).

> Requirements: Docker and Docker Compose installed.
## Key Takeaways

- Built a modular ETL pipeline with separate extract, transform and load scripts.
- Automated the workflow end to end with Airflow scheduling.
- Containerized the environment with Docker so the pipeline is reproducible on any machine.
- Enforced data quality with 7 Great Expectations checks.
- Handled real-world integration issues, such as converting Spark date types so they can be stored in MongoDB.
- **Next improvements:** make the load step idempotent (avoid duplicate inserts on repeated runs) and build an analysis or dashboard on top of the stored data.

## Author

**Raka Airlangga**
[LinkedIn](www.linkedin.com/in/raka-airlangga) · [Email](raka.airlanggaa@gmail.com)
