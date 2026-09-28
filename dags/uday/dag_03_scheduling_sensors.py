"""
═══════════════════════════════════════════════════════════════
  DAG 03 — Scheduling, Sensors & DAG Configuration
═══════════════════════════════════════════════════════════════
  Topics:
    • Execution date concept (data_interval_start)
    • Schedule options (cron, presets, timedelta)
    • DAG parameters (catchup, start_date, max_active_runs)
    • Task parameters (retries, exponential backoff)
    • Sensors (FileSensor — poke vs reschedule)

  How to Run:
    1. Unpause and trigger manually
    2. The FileSensor waits for: include/data/orders_ready.txt
       (already present — sensor should pass quickly)
═══════════════════════════════════════════════════════════════
"""

from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'uday',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}


@dag(
    dag_id='dag_03_scheduling_sensors',
    description='Scheduling, Sensors & DAG Configuration',
    start_date=datetime(2024, 1, 1),
    schedule=None,                   # Manual only for demo
    catchup=False,                   # Don't backfill past runs
    max_active_runs=1,               # Only 1 run at a time
    default_args=default_args,
    dagrun_timeout=timedelta(hours=1),
    tags=['lecture', 'scheduling', 'sensors', 'uday'],
)
def dag_03_scheduling_sensors():

    start = EmptyOperator(task_id='start')
    end   = EmptyOperator(task_id='end', trigger_rule='none_failed_min_one_success')

    # ═════════════════════════════════════════════════════════
    #  SECTION 1 — Execution Date Concept
    # ═════════════════════════════════════════════════════════

    @task
    def show_execution_date(ds=None, data_interval_start=None,
                            data_interval_end=None, **context) -> None:
        """
        execution_date = START of data interval, NOT when DAG runs!

        With @daily, start_date=Jan 1:
          Jan 2 00:00 → DAG runs → execution_date = Jan 1
          Jan 3 00:00 → DAG runs → execution_date = Jan 2

        Why? On Jan 2, you process Jan 1's COMPLETE data.
        """
        print("--- Execution Date Concept ---")
        print(f"  ds (execution date)  : {ds}")
        print(f"  data_interval_start  : {data_interval_start}")
        print(f"  data_interval_end    : {data_interval_end}")
        print(f"  run_id               : {context.get('run_id')}")
        print()
        print("  Key: ds = start of the period being processed")

    # ═════════════════════════════════════════════════════════
    #  SECTION 2 — Schedule Options
    # ═════════════════════════════════════════════════════════

    @task
    def show_schedule_options() -> None:
        """All the ways to schedule a DAG."""
        print("--- Schedule Options ---")
        print()
        print("Presets:")
        print("  @once     → run one time only")
        print("  @hourly   → every hour        → 0 * * * *")
        print("  @daily    → every midnight     → 0 0 * * *")
        print("  @weekly   → every Sunday       → 0 0 * * 0")
        print("  @monthly  → first of month     → 0 0 1 * *")
        print("  @yearly   → January 1st        → 0 0 1 1 *")
        print("  None      → manual only")
        print()
        print("Cron: '0 2 * * *' → daily at 2 AM")
        print("Timedelta: timedelta(hours=6) → every 6 hours")

    # ═════════════════════════════════════════════════════════
    #  SECTION 3 — DAG Parameters
    # ═════════════════════════════════════════════════════════

    @task
    def show_dag_params() -> None:
        """Critical DAG-level parameters."""
        print("--- DAG Parameters ---")
        print()
        print("  start_date      → MUST be a fixed date, never datetime.now()!")
        print("  catchup=True    → backfill all missed runs")
        print("  catchup=False   → only run from today (recommended)")
        print("  max_active_runs → limit concurrent DAG runs")
        print("  dagrun_timeout  → fail DAG if it takes too long")
        print("  default_args    → default task settings")

    # ═════════════════════════════════════════════════════════
    #  SECTION 4 — Task Parameters (Retries Demo)
    # ═════════════════════════════════════════════════════════

    def flaky_task():
        """70% chance of success — shows retry behaviour."""
        import random
        if random.random() > 0.3:
            print("Task succeeded!")
        else:
            raise Exception("Simulated failure — Airflow will retry!")

    retry_demo = PythonOperator(
        task_id='retry_demo',
        python_callable=flaky_task,
        retries=3,
        retry_delay=timedelta(seconds=10),
        retry_exponential_backoff=True,    # 10s → 20s → 40s
        max_retry_delay=timedelta(minutes=2),
    )

    # ═════════════════════════════════════════════════════════
    #  SECTION 5 — Sensors
    # ═════════════════════════════════════════════════════════
    # Sensors WAIT for a condition before proceeding.
    # Two modes:
    #   poke      → holds worker slot (OK for short waits)
    #   reschedule → releases worker (USE for long waits)

    wait_for_file = FileSensor(
        task_id='wait_for_orders_file',
        filepath='/usr/local/airflow/include/data/orders_ready.txt',
        poke_interval=10,        # check every 10 seconds
        timeout=120,             # give up after 2 minutes
        mode='poke',             # short wait, so poke is fine
        soft_fail=True,          # timeout → skipped (not failed)
    )

    @task
    def process_after_sensor() -> None:
        """Only runs after sensor detects the file."""
        print("File detected! Processing orders...")

    # ═════════════════════════════════════════════════════════
    #  DEPENDENCIES
    # ═════════════════════════════════════════════════════════

    exec_date = show_execution_date()
    sched     = show_schedule_options()
    params    = show_dag_params()
    after     = process_after_sensor()

    start >> exec_date >> end
    start >> sched >> end
    start >> params >> end
    start >> retry_demo >> end
    start >> wait_for_file >> after >> end


dag_instance = dag_03_scheduling_sensors()
