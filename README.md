# Age of Empires 2 Dashboard

## 1. Overview

The Age of Empires 2 (aoe2) dashboard provides a comprehensive summary view of player and match statistics from the video game 'Age of Empires 2 DE'.  Data is automatically pulled and refreshed weekly in a PowerBI report, enabling in-depth analysis and data slicing/dicing.  The dashboard aims to answer questions such as:

* Who are the top performing players currently?
* What civilization should I counter-pick to maximize my chances of beating my opponent?
* How has my favorite civilization performed over time?

## 2. Features

* Automated weekly data extraction, loading, and transformation.
* Pipeline incorporates numerous tests to ensure high data quality.
* User-friendly dashboards for data and insights visualization.

## 3. Project Structure

_(Insert Excalidraw diagram of pipeline here)_

## 4. Dashboard Example

_(Display 2 screenshots of PowerBI dashboard insights here)_


## 5. Tools

* **a) Python (Data Extract & Load)**
    * Custom-built modules (API data extraction)
    * Pydantic (schema validation)
    * Pytest (unit testing)
    * Logging & API retries (error handling)
* **b) Apache Airflow (Data Orchestration)**
    * Astronomer Cosmos library (via Docker container)
* **c) Snowflake (Data Warehouse)**
* **d) dbt core (Data Transformation)**
    * User-defined Macros
    * Seeds
    * User-defined Generic and Custom tests
    * `dbt_external_tables` package
    * SQL & Python models
    * Incremental modeling (Performance Optimization)
* **e) Git/Github Actions (Version Control)**
    * CI/CD pipeline (linting, testing, replication)
    * Slim CI (Optimization)
    * Dev & Prod environments (Software Development)
* **f) PowerBI (Data Visualization)**
* **g) Other**
    * AWS S3 buckets (Data Storage)
    * Medallion architecture (Logical data modeling)
    * Star Schema (Dimensional data modeling)
    * `.env` & `config.yaml` files (Configuration as Code)
    * `README.md` files & Dbt docs (Documentation)
    * `requirements.txt` (Package Management)

## 6. Project Methodology & Technical Details

### Data Extraction and Load

The data pipeline uses the ELT framework, extracting and loading data "as-is" from APIs into an AWS S3 bucket.  Data is sourced from two APIs:

1. **Aoestats.io API (`https://aoestats.io`)**
   This API provides historical player and match data.  Two endpoints are used: one for dataset metadata (JSON) and another for direct download of match and player data (Parquet).  Custom Python functions generate API endpoint strings, query the API, validate schemas using Pydantic, and load data into S3.

2. **Relic-link API (now WorldsEdge)**
   This unofficial community API provides the latest leaderboard data (JSON).  Due to a 100-row request limit, data is retrieved in chunks.  Each chunk is validated and loaded as a separate JSON file into S3 to avoid exceeding Snowflake's Variant column limit.

Each API endpoint has dedicated Python scripts following a consistent template:

a. Import functions from helper modules (`utils`, `filter`, `loader`).
b. Ingest parameters from the configuration file.
c. Establish an AWS S3 connection.
d. Submit GET requests to retrieve data.
e. Validate data against the expected schema (Pydantic).
f. Load data into the S3 bucket.

Unit tests using `pytest` ensure function correctness. Airflow DAGs orchestrate script execution. An `all_project_dag.py` script runs all individual DAGs, including a `dbt_dag.py` for transformation steps.

### Data Transformation

Data transformation occurs in dbt on Snowflake, using the Medallion architecture (bronze -> silver -> gold). The gold layer uses a star schema optimized for visualization tools like PowerBI.  Each schema has a corresponding `_schema.yml` file for documentation.  Further details are available in `Medallion_README.md`.

### Workflow Environment

Development and production environments are separated using distinct S3 buckets (`dev`, `prod`) and Snowflake databases (`aoe_dev`, `aoe_prod`).  Dbt profiles switch between targets.  Production data is synced from development.

### Github Workflows

CI workflows (`ci.yaml`) on pull requests run linting (Black), `pytest`, and Slim CI for dbt. CD workflows (`cd.yaml`) on merge to main sync S3 data and run Slim CI on production tables.

### Reducing Compute Costs

Date-driven directory structures in S3 enable delta loading in Snowflake, processing only new data.  Dbt's incremental models and Slim CI further minimize compute costs.

## 7. Future Direction

* Enhanced dbt testing (unit tests, improved thresholds).
* Data quality dashboards for dbt/Airflow runs.
* Improved Airflow failure notifications.
* Incorporating additional AOE data (civilization strengths/weaknesses, logos).
* Infrastructure as Code (IaC) for Snowflake/AWS.
* Migrating processing to AWS (managed Airflow, EC2).
* Utilizing RelicLink API for live data.

## 8. Miscellaneous

### Project Structure

* Dockerfile
* .env
* README.md
* requirements.txt
* .github
  + workflows
    - ci.yaml
    - cd.yaml
* dags
  + all_project_dag.py
  + dbt_dag.py
  + elt_metadata_dag.py
  + elt_relic_api_dag.py
  + elt_stat_matches_dag.py
  + elt_stat_players_dag.py
  + set_load_master_dag.py
* src
  + __init__.py
  + config.yaml
  + elt_metadata.py
  + elt_relic_api.py
  + elt_stat_matches.py
  + elt_stat_players.py
  + project_tests.py
  + set_load_master.py
  + utils.py
  + extract
    - __init__.py
    - filter.py
    - models.py
  + load
    - __init__.py
    - loader.py
  + transform
    + dbt_aoe
      - README.md
      - dbt_project.yml
      - package-lock.yml
      - packages.yml
      - profiles.yml.template
      - analyses
      - dbt_packages
        - dbt_external_tables
      - macros
        - deduplicate_by_key.sql
        - filter_load.sql
        - generate_schema_name.sql
      - models
        - Medallion_README.md
        - bronze
          - bronze_schema.yml
          - dim_date_br.py
          - ext_table_schema.yml
          - leaderboards_br.sql
          - matches_br.sql
          - players_br.sql
          - statgroup_br.sql
          - v_matches_raw.sql
          - v_players_raw.sql
          - v_relic_raw.sql
        - gold
          - dim_civ.sql
          - dim_date.sql
          - dim_match.sql
          - dim_player.sql
          - fact_player_matches.sql
          - gold_schema.yml
        - silver
          - matches_sr.sql
          - player_leaderboard_stats_sr.sql
          - player_match_sr.sql
          - silver_schema.yml
      - seeds
        - country_list.csv
        - seeds.yml
      - snapshots
      - tests
        - assert_countrys_mapped.sql
        - generic
          - test_recent_ldts.sql
          - test_within_threshold.sql

### Project Structure2

- `Dockerfile`
- `.env`
- `README.md`
- `requirements.txt`
- `.github`
  - `workflows`
    - `ci.yaml`
    - `cd.yaml`
- `dags`
  - `all_project_dag.py`
  - `dbt_dag.py`
  - `elt_metadata_dag.py`
  - `elt_relic_api_dag.py`
  - `elt_stat_matches_dag.py`
  - `elt_stat_players_dag.py`
  - `set_load_master_dag.py`
- `src`
  - `__init__.py`
  - `config.yaml`
  - `elt_metadata.py`
  - `elt_relic_api.py`
  - `elt_stat_matches.py`
  - `elt_stat_players.py`
  - `project_tests.py`
  - `set_load_master.py`
  - `utils.py`
  - `extract`
    - `__init__.py`
    - `filter.py`
    - `models.py`
  - `load`
    - `__init__.py`
    - `loader.py`
  - `transform`
    - `dbt_aoe`
      - `README.md`
      - `dbt_project.yml`
      - `package-lock.yml`
      - `packages.yml`
      - `profiles.yml.template`
      - `analyses`
      - `dbt_packages`
        - `dbt_external_tables`
      - `macros`
        - `deduplicate_by_key.sql`
        - `filter_load.sql`
        - `generate_schema_name.sql`
      - `models`
        - `Medallion_README.md`
        - `bronze`
          - `bronze_schema.yml`
          - `dim_date_br.py`
          - `ext_table_schema.yml`
          - `leaderboards_br.sql`
          - `matches_br.sql`
          - `players_br.sql`
          - `statgroup_br.sql`
          - `v_matches_raw.sql`
          - `v_players_raw.sql`
          - `v_relic_raw.sql`
        - `gold`
          - `dim_civ.sql`
          - `dim_date.sql`
          - `dim_match.sql`
          - `dim_player.sql`
          - `fact_player_matches.sql`
          - `gold_schema.yml`
        - `silver`
          - `matches_sr.sql`
          - `player_leaderboard_stats_sr.sql`
          - `player_match_sr.sql`
          - `silver_schema.yml`
      - `seeds`
        - `country_list.csv`
        - `seeds.yml`
      - `snapshots`
      - `tests`
        - `assert_countrys_mapped.sql`
        - `generic`
          - `test_recent_ldts.sql`
          - `test_within_threshold.sql`