from datetime import timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.task.trigger_rule import TriggerRule
from pendulum import datetime


# -----------------------------
# TASK FUNCTIONS
# -----------------------------

def validate_orders():
    print("Validating orders...")

    # Demo data
    data = {
        "row_count": 150,
        "total_amount": 50000
    }

    print(f"Rows found: {data['row_count']}")
    print(f"Total amount: {data['total_amount']}")

    # Return value automatically becomes XCom
    return data


def decide_path(**context):
    # Pull data from previous task's XCom
    ti = context["ti"]

    data = ti.xcom_pull(
        task_ids="validate_orders"
    )

    row_count = data["row_count"]

    print(f"Row count received through XCom: {row_count}")

    if row_count > 100:
        return "full_transform"

    return "light_transform"


def full_transform():
    print("Running FULL transformation...")
    return "full transformation completed"


def light_transform():
    print("Running LIGHT transformation...")
    return "light transformation completed"


# -----------------------------
# DAG
# -----------------------------

with DAG(
    dag_id="airflow_operators_demo",
    start_date=datetime(2026, 9, 1),
    schedule=None,
    catchup=False,
    tags=["training", "operators"],
) as dag:

    # 1. SENSOR
    wait_for_file = FileSensor(
        task_id="wait_for_orders_file",
        filepath="/usr/local/airflow/include/data/",
        poke_interval=10,
        timeout=300,
        mode="reschedule",
    )

    # 2. ACTION - PythonOperator
    validate = PythonOperator(
        task_id="validate_orders",
        python_callable=validate_orders,
        retries=2,
        retry_delay=timedelta(seconds=10),
    )

    # 3. BRANCH
    route = BranchPythonOperator(
        task_id="route_by_volume",
        python_callable=decide_path,
    )

    # 4A. LARGE DATA PATH
    full = PythonOperator(
        task_id="full_transform",
        python_callable=full_transform,
    )

    # 4B. SMALL DATA PATH
    light = PythonOperator(
        task_id="light_transform",
        python_callable=light_transform,
    )

    # 5. JOIN
    complete = BashOperator(
        task_id="complete",
        bash_command="""
            echo "Pipeline completed"
            echo "Execution date: {{ ds }}"
        """,
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    # DEPENDENCIES
    wait_for_file >> validate >> route

    route >> [full, light]

    [full, light] >> complete
