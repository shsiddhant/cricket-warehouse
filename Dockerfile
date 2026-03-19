FROM apache/airflow:2.9.3-python3.12

USER root

# git dependency for running dbt deps
RUN apt-get update && apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/*

USER airflow
WORKDIR /opt/airflow

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV DBT_PROFILES_DIR="/opt/airflow/dbt"

# Copy dependencies list
COPY --chown=airflow:0 pyproject.toml uv.lock requirements.txt ./

# Install dependencies
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r requirements.txt

# 1. Copy the source code
COPY --chown=airflow:0 src/ ./src/

# 2. Copy the dbt project folder
COPY --chown=airflow:0 dbt/ ./dbt/

# Install package
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" .
