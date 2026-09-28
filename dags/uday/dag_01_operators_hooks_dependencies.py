"""
═══════════════════════════════════════════════════════════════
  DAG 01 — Traditional Operators, Hooks & Dependencies
═══════════════════════════════════════════════════════════════
  Everything here uses the TRADITIONAL syntax:
    • PythonOperator
    • BashOperator + Jinja Templates
    • BranchPythonOperator
    • Hook concept (simulated)
    • Dependency patterns (>>)

  How to Run:
    1. Unpause this DAG in the Airflow UI
    2. Trigger manually (▶ button)
    3. Go to Graph View to see the structure
    4. Click each task → Log tab to see output
═══════════════════════════════════════════════════════════════
"""

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import random


# ─────────────────────────────────────────────────────────────
#  DEFAULT ARGS — applied to every task in this DAG
# ─────────────────────────────────────────────────────────────
default_args = {
    'owner': 'uday',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


with DAG(
    dag_id='dag_01_operators_hooks_dependencies',
    description='Traditional Operators, Hooks & Dependencies',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=['lecture', 'operators', 'hooks', 'traditional', 'uday'],
) as dag:

    start = EmptyOperator(task_id='start')
    end   = EmptyOperator(task_id='end', trigger_rule='none_failed_min_one_success')

    # ═════════════════════════════════════════════════════════
    #  SECTION 1 — PythonOperator (Simple ETL)
    # ═════════════════════════════════════════════════════════
    # The most flexible operator. Runs any Python function.
    # Return values are auto-pushed to XCom.

    def extract_data():
        """Extract order data. Return value goes to XCom."""
        orders = [
            {'id': 1, 'product': 'Laptop',   'amount': 1200},
            {'id': 2, 'product': 'Mouse',    'amount': 25},
            {'id': 3, 'product': 'Keyboard', 'amount': 75},
        ]
        print(f"Extracted {len(orders)} orders")
        return orders

    def transform_data(**context):
        """Pull from XCom, apply business logic."""
        ti = context['ti']
        orders = ti.xcom_pull(task_ids='extract')

        for order in orders:
            order['tax']   = round(order['amount'] * 0.10, 2)
            order['total'] = round(order['amount'] + order['tax'], 2)

        print(f"Transformed {len(orders)} orders, added tax")
        return orders

    def load_data(**context):
        """Pull transformed data, simulate loading."""
        ti = context['ti']
        orders = ti.xcom_pull(task_ids='transform')

        for o in orders:
            print(f"  Loading: {o['product']} → ${o['total']}")
        print(f"Loaded {len(orders)} orders")

    extract   = PythonOperator(task_id='extract',   python_callable=extract_data)
    transform = PythonOperator(task_id='transform', python_callable=transform_data)
    load      = PythonOperator(task_id='load',      python_callable=load_data)

    # ═════════════════════════════════════════════════════════
    #  SECTION 2 — BashOperator + Jinja Templates
    # ═════════════════════════════════════════════════════════
    # Runs shell commands. {{ ds }} etc. are Jinja variables
    # that Airflow replaces at runtime.

    bash_info = BashOperator(
        task_id='bash_show_info',
        bash_command='''
            echo "DAG ID       : {{ dag.dag_id }}"
            echo "Task ID      : {{ task.task_id }}"
            echo "Execution dt : {{ ds }}"
            echo "No dashes    : {{ ds_nodash }}"
            echo "Run ID       : {{ run_id }}"
        ''',
    )

    bash_work = BashOperator(
        task_id='bash_create_file',
        bash_command='''
            mkdir -p /tmp/airflow_demo/{{ ds }}
            echo "report for {{ ds }}" > /tmp/airflow_demo/{{ ds }}/report.txt
            echo "Created report at /tmp/airflow_demo/{{ ds }}/report.txt"
        ''',
    )

    # ═════════════════════════════════════════════════════════
    #  SECTION 3 — BranchPythonOperator
    # ═════════════════════════════════════════════════════════
    # Returns the task_id of the branch to execute next.
    # The other branch gets "skipped".

    def pick_branch(**context):
        """Return the task_id of the path to follow."""
        size = random.randint(10, 200)
        print(f"Data size: {size} MB")

        if size > 100:
            return 'process_large'      # ← task_id
        else:
            return 'process_small'      # ← task_id

    branch_decide = BranchPythonOperator(
        task_id='branch_decide',
        python_callable=pick_branch,
    )

    process_large = PythonOperator(
        task_id='process_large',
        python_callable=lambda: print("Using Spark for large dataset"),
    )

    process_small = PythonOperator(
        task_id='process_small',
        python_callable=lambda: print("Using Pandas for small dataset"),
    )

    # trigger_rule='none_failed' is KEY for branching!
    # Without it, this task won't run because the skipped
    # branch counts as "not success".
    branch_done = PythonOperator(
        task_id='branch_done',
        python_callable=lambda: print("Branching complete!"),
        trigger_rule='none_failed',
    )

    # ═════════════════════════════════════════════════════════
    #  SECTION 4 — Hook Concept (Simulated)
    # ═════════════════════════════════════════════════════════
    # Hooks are the connection layer. Operators use them internally.
    #
    # Operator (what to do)  ──→  Hook (how to connect)
    # PostgresOperator       ──→  PostgresHook
    # SnowflakeOperator      ──→  SnowflakeHook

    def demo_hook_concept():
        """
        Shows WHY hooks exist — no real DB needed.

        BAD:  psycopg2.connect(password='hardcoded!')
        GOOD: PostgresHook(postgres_conn_id='postgres_default')
        """
        print("--- Hook Concept ---")
        print()
        print("BAD (never do this):")
        print("  conn = psycopg2.connect(password='MyS3cret!')")
        print()
        print("GOOD (Airflow way):")
        print("  hook = PostgresHook(postgres_conn_id='postgres_default')")
        print("  records = hook.get_records('SELECT * FROM users')")
        print()
        print("Hook methods:")
        print("  hook.get_records()   → list of tuples")
        print("  hook.get_first()     → single row")
        print("  hook.get_pandas_df() → DataFrame")
        print("  hook.run()           → INSERT/UPDATE/DELETE")
        print("  hook.insert_rows()   → bulk insert")

    hook_demo = PythonOperator(
        task_id='hook_demo',
        python_callable=demo_hook_concept,
    )

    # ═════════════════════════════════════════════════════════
    #  DEPENDENCIES — wiring tasks together
    # ═════════════════════════════════════════════════════════
    # >> means "runs before"
    # [a, b] means "both a and b" (parallel)

    # ETL chain: extract → transform → load
    start >> extract >> transform >> load >> end

    # Bash tasks (parallel pair)
    start >> bash_info >> bash_work >> end

    # Branching: decide → [large OR small] → done
    start >> branch_decide >> [process_large, process_small] >> branch_done >> end

    # Hook demo (standalone)
    start >> hook_demo >> end
