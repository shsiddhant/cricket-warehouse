# Cricket Warehouse

![Python](https://img.shields.io/badge/Python-3.10+-blue?style=for-the-badge)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-database-blue?style=for-the-badge)
![dbt](https://img.shields.io/badge/dbt-data_pipeline-orange?style=for-the-badge)
![License](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)

Build a cricket data warehouse for analytics.

## Installation

### Prerequisites

* Python 3.10+
* PostgreSQL 14+
* Git

### 1. Clone the repository

```shell
git clone https://github.com/shsiddhant/cricket-warehouse.git
cd womens-wc
```

### 2. Create and activate a virtual environment

#### A. Using `uv`

```shell
uv venv .venv --seed
source .venv/bin/activate
uv sync
```

#### B. Using `pip`

```shell
python -m venv .venv
source .venv/bin/activate
pip install .
```

---

## Data Source

### Raw Data

* **Source:** [Cricsheet](https://cricsheet.org/)
* **Format:** JSON (one file per match)
* **Granularity:** Ball-by-ball

---


## Database Schema

- Ball-by-ball match data stored in normalized relational tables
- Key tables: venues, matches, players, teams, innings, deliveries
- Derived tables provide aggregated stats for analytics and modeling

---

## Tools and Libraries

* Python
* PostgreSQL
* Airflow
* dbt
* pandas
* numpy
* psycopg2

---

## License

[MIT](LICENSE)
