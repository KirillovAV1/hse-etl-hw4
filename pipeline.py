# airflow/dags/pipeline.py

import pandas as pd
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def extract(ti):
    data = pd.read_csv("/opt/airflow/dags/IOT-temp.csv")
    ti.xcom_push(key="raw_temperature", value=data.to_json(orient="records"))


def prepare_data(json_data):
    data = pd.read_json(json_data, orient="records")

    data = data[data["out/in"].astype(str).str.lower().str.strip() == "in"]

    data["noted_date"] = pd.to_datetime(
        data["noted_date"],
        format="%d-%m-%Y %H:%M"
    ).dt.date

    # per5 = data["temp"].quantile(0.05)
    # per95 = data["temp"].quantile(0.95)
    # data = data[(data["temp"] >= per5) & (data["temp"] <= per95)]

    return data


def transform_full(ti):
    json_data = ti.xcom_pull(task_ids="extract", key="raw_temperature")
    data = prepare_data(json_data)
    result = build_result(data)

    ti.xcom_push(
        key="temperature_full",
        value=result.to_json(orient="records", date_format="iso")
    )


def transform_incremental(ti):
    json_data = ti.xcom_pull(task_ids="extract", key="raw_temperature")
    data = prepare_data(json_data)

    pg_hook = PostgresHook(postgres_conn_id="hse")
    max_date = pg_hook.get_first("SELECT MAX(noted_date) FROM temperature")[0]

    if max_date is not None:
        border_date = max_date - timedelta(days=3)
        data = data[data["noted_date"] >= border_date]

    result = build_result(data)

    ti.xcom_push(
        key="temperature_incremental",
        value=result.to_json(orient="records", date_format="iso")
    )


def build_result(data):
    hottest_days = data.loc[
        data.groupby("noted_date")["temp"].idxmax()
    ].nlargest(5, "temp")

    coldest_days = data.loc[
        data.groupby("noted_date")["temp"].idxmin()
    ].nsmallest(5, "temp")

    result = pd.concat([coldest_days, hottest_days], axis=0)

    result = result.rename(columns={
        "room_id/id": "room_id",
        "temp": "temperature",
        "out/in": "out_in",
    })

    return result


def load_data(ti, source_task, key):
    json_data = ti.xcom_pull(task_ids=source_task, key=key)
    data = pd.read_json(json_data, orient="records")

    pg_hook = PostgresHook(postgres_conn_id="hse")

    stmt = """
    INSERT INTO temperature (id, room_id, noted_date, temperature, out_in)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (id) DO UPDATE SET
        room_id = EXCLUDED.room_id,
        noted_date = EXCLUDED.noted_date,
        temperature = EXCLUDED.temperature,
        out_in = EXCLUDED.out_in
    """

    values = [
        (
            row["id"],
            row["room_id"],
            str(row["noted_date"]),
            int(row["temperature"]),
            row["out_in"],
        )
        for _, row in data.iterrows()
    ]

    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(stmt, values)
        conn.commit()


with DAG(dag_id="transform_practice_full") as dag_full:
    extract_data = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    transform_data = PythonOperator(
        task_id="transform_full",
        python_callable=transform_full,
    )

    load_data_task = PythonOperator(
        task_id="load_full",
        python_callable=load_data,
        op_kwargs={
            "source_task": "transform_full",
            "key": "temperature_full",
        },
    )

    extract_data >> transform_data >> load_data_task

with DAG(dag_id="transform_practice_incremental") as dag_incremental:
    extract_data = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    transform_data = PythonOperator(
        task_id="transform_incremental",
        python_callable=transform_incremental,
    )

    load_data_task = PythonOperator(
        task_id="load_incremental",
        python_callable=load_data,
        op_kwargs={
            "source_task": "transform_incremental",
            "key": "temperature_incremental",
        },
    )

    extract_data >> transform_data >> load_data_task
