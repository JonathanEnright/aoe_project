# Medallion Logical Layer Design

For this project, I have structured the tables to use the Medallion framework, sorting the data pipeline into the logical layers: Bronze -> Silver -> Gold. This approach keeps the data logically separated into separate schemas as it moves from 'dirty' raw data into 'clean' conformed dimensional models. Note, I have overridden the dbt macro `generate_schema_name.sql` such that the schema now takes upon the models medallion layer as its schema name within the Snowflake data warehouse.

---

## Bronze Layer

### Overview

The Bronze layer represents the cleaned landing data reconfigured into database usable form. It is expected to hold temporary data that is prepared to be loaded into Silver Tables.

#### Components

- **External Tables**

  External tables are defined in the `ext_table_schema.yml` and represent the data within the S3 bucket path. Loading and refresh of the external table is handled by the dbt package `dbt_external_tables`. No transformation takes place here; fields are shown 'as is' from the underlying data. We add the `_ext` suffix to easily identify these within Snowflake.

- **Raw Views**

  The Raw Views are Snowflake views built on top of the external tables. They contain all fields from the external table, plus metadata fields such as source name and load timestamp. Data is filtered on its `file_date` as according to the `control_master_table`, such that we only bring in relevant rows of data needed for consumption. This is called by the user-defined macro `filter_load.sql`, which can be found in the macros folder. We add the `v_` prefix to denote it is a view, and the `_raw` suffix to identify this is a raw reflection of the source data.

- **Bronze Tables**

  The Bronze Tables read the data from the Raw Views and apply light transformations. This includes flattening JSON, applying correct data types, and renaming fields. Data is also de-duplicated based on the defined primary key and timestamp, using the user-defined macro `deduplicate_by_key.sql`. A Python model `dim_date_br.py` is also used to create the `dim_date` table, as it is incredibly easy and efficient to do so using the pandas library. We add the `_br` suffix to denote the bronze tables.

### Data Retention

The Bronze layer's purpose is to reflect a cleaned version of the staging area and hence is expected to contain temporary held tables/views.

- **Object Type**: Transient Tables or Views.
- **Loading Type**: Truncate & Load or Create & Replace.
- **Keeps Historical Data**: No. Data is not expected to be retained in the bronze layer. The reloading of data will overwrite the previously held data in the table. History will be retained in the Silver/Gold Layer.

---

## Silver Layer

### Overview

The Silver layer represents the area of which we run further transformations and apply business logic to the data. Transformation can involve creating new surrogate keys, joining new datasets together, applying conditional logic, etc.

#### Silver Tables

In this project, the data is already in a mostly usable form, so we have a light silver layer with minor transformations. We create a surrogate key on the `player_match_sr` table using a MD5 hash, to uniquely identify rows based on a single field. This makes it easier for Snowflake to perform UPSERT commands, rather than trying to use a composite key. We also ingest a simply 'seed' file (`country_list.csv`) into the table `player_leaderboard_stats_sr`, to provide the mapping between country codes and names. We add the `_sr` suffix to denote the silver tables.

### Data Retention

The Silver layer's purpose is to hold a record and history of previously loaded bronze data. Hence data is expected to be held in permanent tables.

- **Object Type**: Permanent and/or incrementally loaded Tables.
- **Loading Type**: Insert & Update (mainly).
- **Keeps Historical Data**: Yes. Key Silver tables should not be dropped or truncated unless a full reload is required or specified otherwise.

---

## Gold Layer

### Overview

The Gold layer represents the area of which data is modeled into a dimensional model as preparation for reporting. As such, this entails:

- **Fact Tables**: The primary table that stores the measures and foreign keys to the dimension table.
- **Dimension Tables**: Tables that contain the attributes of a certain area, with a primary key that joins to the Fact table.

### Data Retention

Gold layer tables should retain data and keep an historical view. As such, inserts should be done as 'delta' loads, only adding in new data.

- **Object Type**: Permanent and/or incrementally loaded Tables.
- **Loading Type**: Insert & Update (mainly).
- **Keeps Historical Data**: Yes. Gold tables should not be dropped or truncated unless a full reload is required or specified otherwise.