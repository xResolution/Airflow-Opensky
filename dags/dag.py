import json
from datetime import datetime
from time import time
import duckdb

from airflow.decorators import task, task_group
from airflow.models.dag import dag
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from requests import get
from requests.auth import HTTPBasicAuth

api_list = [
    {
        "name": "states",
        "schedule" : "30 9 * * *",
        "url": "https://opensky-network.org/api/states/all?extended=1",
        "columns": [
            "icao24",
            "callsign",
            "origin_country",
            "time_position",
            "last_contact",
            "longitude",
            "latitude",
            "baro_altitude",
            "on_ground",
            "velocity",
            "true_track",
            "vertical_rate",
            "sensors",
            "geo_altitude",
            "squawk",
            "spi",
            "position_source",
            "category"
        ],
        "target_table": "bdd_airflow.main.openskynetwork_raw",
        "timestamp_required": False
    },
    {
        "name": "flights",
        "schedule" : "30 14 * * *",
        "url": "https://opensky-network.org/api/flights/all?begin={begin}&end={end}",
        "columns": [
            "icao24",
            "firstSeen",
            "estDepartureAirport",
            "lastSeen",
            "estArrivalAirport",
            "callsign",
            "estDepartureAirportHorizDistance",
            "estDepartureAirportVertDistance",
            "estArrivalAirportHorizDistance",
            "estArrivalAirportVertDistance",
            "departureAirportCandidatesCount",
            "arrivalAirportCandidatesCount"
        ],
        "target_table": "bdd_airflow.main.flights_raw",
        "timestamp_required": True
    }
]

USERNAME = "your_username"
PASSWORD = "your_password"


def states_to_dict(states_list, columns, timestamp):
    return [{**dict(zip(columns, state)), "timestamp": timestamp} for state in states_list]


def flights_to_dict(flights, timestamp):
    return [{**flight, "timestamp": timestamp} for flight in flights]


def format_datetime(input_datetime):
    return input_datetime.strftime("%Y%m%d")


@task(multiple_outputs=True)
def run_parameters(api=None, dag_run=None):
    data_interval_start = format_datetime(dag_run.data_interval_start)
    data_interval_end = format_datetime(dag_run.data_interval_end)

    out = api
    out["data_filename"] = f"data/data_{out['name']}_{data_interval_start}_{data_interval_end}.json"

    if out["timestamp_required"]:
        end = int(time())
        begin = end - 3600
        out["url"] = out["url"].format(begin=begin, end=end)

    return out


@task(multiple_outputs=True)
def get_flight_data(username, password, rows_limit, run_params):
    print("Getting data...")
    auth = HTTPBasicAuth(username, password)
    url = run_params["url"]
    columns = run_params["columns"]
    req = get(url, auth=auth)
    req.raise_for_status()
    resp = req.json()
    if "states" in resp:
        timestamp = resp["time"]
        results_json = states_to_dict(resp["states"], columns, timestamp)
    else:
        timestamp = int(time())
        results_json = flights_to_dict(resp, timestamp)

    nb_rows = len(results_json)

    if nb_rows <= int(rows_limit) :
        with duckdb.connect("data/bdd_airflow") as conn:
            for result in results_json:
                value_placeholder = ",".join(["?"]*len(result))
                conn.execute(f"INSERT INTO {run_params['target_table']} VALUES ({value_placeholder})", result.values())
        return {"timestamp": timestamp, "rows": nb_rows}

    data_filename = run_params["data_filename"]

    with open(data_filename, "w") as f:
        json.dump(results_json, f)

    return {"filename": data_filename, "timestamp": timestamp, "rows": nb_rows}


def load_from_file():
    return SQLExecuteQueryOperator(
        task_id="load_from_file",
        conn_id="DUCK_DB",
        sql="scripts/load_from_file.sql",
        return_last=True,
        show_return_value_in_logs=True
    )


@task.branch()
def branch_low_row_number(row_limit, ti=None):
    nb_rows = ti.xcom_pull(task_ids="data_ingestion_tg.get_flight_data", key="rows")
    if nb_rows > int(row_limit):
        return "data_ingestion_tg.load_from_file"
    else:
        return "data_ingestion_tg.skip_load"


@task_group()
def data_ingestion_tg(username, password, run_params):
    end_data_ingestion = EmptyOperator(task_id="end_data_ingestion", trigger_rule="none_failed_min_one_success")
    skip_load = EmptyOperator(task_id="skip_load")
    get_flight_data(username, password, "{{ params.rows_limit }}",run_params) >> branch_low_row_number("{{ params.rows_limit }}") >>  [load_from_file(), skip_load] >> end_data_ingestion


@task()
def check_row_numbers(row_limit, ti=None):
    nb_expected_lines = ti.xcom_pull(task_ids="data_ingestion_tg.get_flight_data", key="rows")

    if nb_expected_lines <= int(row_limit):
        print(f"Number of lines loaded ({nb_expected_lines}) below the row limit ({row_limit}), data are already in the database")
        return

    nb_lines_found = ti.xcom_pull(task_ids="data_ingestion_tg.load_from_file", key="return_value")[0][0]

    if nb_lines_found != nb_lines_found:
        print(
            f"Error : Number of lines loaded ({nb_lines_found}) != number of lines from the API ({nb_expected_lines})")

    print(f"Number of lines = {nb_lines_found}")


def check_duplicates():
    return SQLExecuteQueryOperator(
        task_id="check_duplicates",
        conn_id="DUCK_DB",
        sql="scripts/check_duplicates.sql",
        return_last=True,
        show_return_value_in_logs=True
    )


@task_group
def data_quality_tg():
    check_row_numbers("{{ params.rows_limit }}")
    check_duplicates()

for api in api_list:

    @dag(
        dag_id=api["name"],
        start_date=datetime(2025, 1, 1),
        params={ "rows_limit": 600 },
        catchup=False,
        concurrency=1
    )
    def flights_pipeline():
        run_parameters_task = run_parameters(api)
        start = EmptyOperator(task_id="start")
        end = EmptyOperator(task_id="end")
        (
                start
                >> run_parameters_task
                >> data_ingestion_tg(username=USERNAME, password=PASSWORD, run_params=run_parameters_task)
                >> data_quality_tg()
                >> end
        )

    flights_pipeline()
