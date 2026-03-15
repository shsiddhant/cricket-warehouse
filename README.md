# Cricket Warehouse

![Python](https://img.shields.io/badge/Python-3.11+-blue?style=for-the-badge)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-database-blue?style=for-the-badge)
![dbt](https://img.shields.io/badge/dbt-data_pipeline-orange?style=for-the-badge)
![License](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)

A data warehouse for ball-by-ball cricket match data, designed for analytics and modeling.

The project ingests raw match JSON from [Cricsheet](https://cricsheet.org/), normalizes the data into relational tables in PostgreSQL, and builds analytical models using dbt.

## Overview

Cricsheet data is nested and complex. Each match file contains hierarchical JSON describing:

- match info
	- match dates and format
	- teams and players
	- outcomes and events
- innings
	- deliveries

This project builds a reproducible data pipeline that transforms this raw data into a structured warehouse suitable for querying and analytics.

**Pipeline stages:**

1. Fetch raw match data from Cricsheet.
2. Ingest JSON files into normalized source tables
3. Track ingestion state using file hashes for incremental updates
4. Transform and model data using dbt
5. Expose analytical tables for queries and downstream models

## Data Source

### Cricsheet

**Source:** https://cricsheet.org/

Cricsheet provides structured cricket match data across multiple formats and leagues.

Raw dataset characteristics:


**Format:** JSON
**Granularity:** Ball-by-ball
**File structure:**	One file per match

## Architecture

```mermaid
flowchart TD

A[Cricsheet Match Data: JSON ZIP] -->|Fetch| B[Raw JSON Files]

B -->|Incremental Ingestion| C[Source Tables: PostgreSQL]

C -->|dbt| D[Staging Layer: Normalized JSONB]

D -->|dbt| E[Intermediate Layer: Relational Tables]

E -->|dbt| F[Marts: Analytics Tables]
```

Key design decisions:

- JSON ingestion is partially flattened during loading.
- Ingestion is incremental, tracked using file hashes.
- Transformations are implemented as dbt models
- venue metadata is managed via seed tables.


## Database Schema

The warehouse stores cricket match data in normalized relational tables.

 1. **Staging Models**
	 - `stg_cricsheet__match_info`
	 - `stg_cricsheet__deliveries`
2. **Intermediate Models**
	- `int_venues` — Each row represents a match venue.
    - `int_matches` - Each row represents a match, with columns such as
	    - `match_id`
	    - `start_date`
	    - `toss_winner`
	    - `outcome_type`
	    - `winner`
    - `int_deliveries`: Each row represents a unique match delivery.
    - `int_innings`: Each row represents a match innings, with columns such as
        - `innings_number`
        - `team` 
        - `runs_scored`
    - `int_teams`: Each row represents a unique (team, format) pair.
    - `int_match_teams`: Junction table for represent many to many relationship between matches and teams.
    - `int_players`: Each row represents a unique (player, team, format) tuple.
    - `int_match_players`: Junction table for represent many to many relationship between matches and players.

## CLI

The project includes a CLI for managing the ingestion pipeline.

```shell
cricwh --help
Usage: cricwh [OPTIONS] COMMAND [ARGS]...                                                                                                                                    
╭─ Options ────────────────────────────────────────────────────────────╮
│ --install-completion       Install completion for the current shell. │
│ --show-completion          Show completion for the current shell, to │
│                            copy it or customize the installation.    │
│ --help                     Show this message and exit.               │
╰──────────────────────────────────────────────────────────────────────╯
╭─ Commands────────────────────────────────────────────────────────────╮
│ fetch      Fetch data from Cricsheet.                                │
│ configure  Configure cricket-warehouse.                              │
│ init       Initialize source tables and seeds.                       │
│ ingest     Ingest JSON files into source tables.                     │
│ update     Update venue city seed.                                   │
╰──────────────────────────────────────────────────────────────────────╯
```

### Configuration

A config file is provided to manage PostgreSQL database credentials. On first run, `cricwh` initializes an example config. The config file may be found at:

|Operating System |Location|
|---|---|
|**Linux/Unix** |`~/.config/cricketwarehouse/config.yaml`|
|**macOS**|`~/Library/Preferences/cricketwarehouse/config.yaml`|
|**Windows**|`C:\Users\<username>\AppData\Local\cricketwarehouse\cricketwarehouse/config.yaml`|

You can edit the configuration using the `configure` command:

```shell
cricwh configure [--init-config-file]
```

You can reset the config file using the `--init-config-file` flag in the `configure` command.

### Logs

Detailed logs are written during each command. The log file may be found at:

|Operating System |Location|
|---|---|
|**Linux/Unix** |`~/.local/share/cricketwarehouse/cricwh.log`|
|**macOS**|`~/Library/Application Support/cricketwarehouse/cricwh.log`|
|**Windows**|`C:\Users\<username>\AppData\Local\cricketwarehouse\cricketwarehouse/cricwh.log`|

## Workflow

Typical workflow:

1. Initialize source tables on first run.

	```shell
	cricwh init
	```

2. Fetch and extract raw match data.

    ```shell
    cricwh fetch [URL] [ZIP FILE PATH]
    ```

3. Seed lookup tables.

	```shell
	dbt seed
	```

4. Ingest match data into source tables.

	```shell
	cricwh ingest
	```

5. Update venue city and city-country lookup seeds.

	```
	cricwh update --seeds
	```

6. (Optional) Manually update missing city and country values in seed CSV files.
7. Run dbt models
	```shell
	dbt build
	```

The ingestion process tracks processed files using file hashes, ensuring new files are added without duplicating existing data.

## Installation

### Prerequisites

* Python 3.10+
* PostgreSQL 14+
* Git

### Clone the repository

```shell
git clone https://github.com/shsiddhant/cricket-warehouse.git
cd cricket-warehouse
```

### Create and activate a virtual environment

#### Using `uv`

```shell
uv venv .venv --seed
source .venv/bin/activate
uv sync
```

#### Using `pip`

```shell
python -m venv .venv
source .venv/bin/activate
pip install .
```

## Tools and Libraries

| Tool	      | Purpose                           |
|-------------|-----------------------------------|
| Python	  | Data ingestion and CLI tooling    |
| PostgreSQL  | Data warehouse                    |
| psycopg2	  | PostgreSQL database interface     |
| dbt	      | Data modeling and transformations |




## License

[MIT](LICENSE)
