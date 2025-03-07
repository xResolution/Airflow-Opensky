# Airflow Demonstration Project

## Overview
This project demonstrates my ability to create scalable, dynamic, and robust data pipelines using Apache Airflow, Python, DuckDB, and external APIs. It fetches aviation-related data from the OpenSky Network API, dynamically handling multiple API endpoints and demonstrating conditional branching, data ingestion, and quality assurance workflows.

## Project Structure
- **Airflow DAGs**: Dynamically defined pipelines for each API endpoint (`states` and `flights`).
- **Task Groups**:
  - `data_ingestion_tg`: Handles data retrieval, conditional branching based on data volume, and data loading.
  - `data_quality_tg`: Validates the integrity and quality of ingested data.

## Workflow Steps
1. **Run Parameters Generation**: Prepares API URLs and file paths based on scheduling intervals.
2. **Data Retrieval**:
   - Fetches data from OpenSky APIs using basic authentication.
   - Dynamically formats and processes API responses.
3. **Conditional Data Loading**:
   - If data volume exceeds the defined threshold, data is saved to a JSON file and bulk-loaded into DuckDB.
   - If data volume is below the threshold, data is inserted directly into DuckDB.
4. **Data Quality Checks**:
   - Verifies the number of rows ingested matches the API response.
   - Checks for duplicates using SQL queries executed against DuckDB.

## Technologies Used
- **Apache Airflow**: Orchestrates workflow execution, handles task dependencies and branching logic.
- **DuckDB**: A fast, embedded analytical database used for demonstration purposes.
- **Python**: Core scripting language for tasks, including requests to external APIs and data transformations.

## How to Run
- Ensure Airflow is running and configured correctly.
- Place the provided Python DAG script in Airflow's DAGs directory.
- Configure connection (`DUCK_DB`) in Airflow connections.
- Trigger the DAGs manually or enable scheduling.

## Project Goal
The primary purpose of this repository is to demonstrate:
- Ability to create dynamic, reusable DAGs.
- Competency with branching logic based on runtime parameters.
- Skills in API integration, data transformation, and loading techniques.
- Proficiency in implementing data quality control measures.

This is a showcase project intended solely to illustrate my data engineering and workflow orchestration skills.

