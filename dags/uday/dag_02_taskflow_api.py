"""
═══════════════════════════════════════════════════════════════
  DAG 02 — TaskFlow API (The Modern Way)
═══════════════════════════════════════════════════════════════
  Everything here uses the TASKFLOW syntax:
    • @dag and @task decorators
    • Automatic XCom (no manual push/pull)
    • Dependencies via function calls
    • @task.branch for branching
    • @task(multiple_outputs=True)
    • Accessing Airflow context

  Compare this with DAG 01 to see the difference!

  How to Run:
    1. Unpause this DAG in the Airflow UI
    2. Trigger manually
    3. Compare the code with DAG 01 — same logic, cleaner syntax
═══════════════════════════════════════════════════════════════
"""

from airflow.decorators import dag, task
from datetime import datetime, timedelta
import random


@dag(
    dag_id='dag_02_taskflow_api',
    description='TaskFlow API — The Modern Way to Write DAGs',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={'owner': 'uday', 'retries': 1},
    tags=['lecture', 'taskflow', 'uday'],
)
def dag_02_taskflow_api():

    # ═════════════════════════════════════════════════════════
    #  SECTION 1 — Basic TaskFlow ETL
    # ═════════════════════════════════════════════════════════
    # Same ETL as DAG 01, but using @task decorator.
    # Notice: NO xcom_pull, NO PythonOperator, NO provide_context!

    @task
    def extract() -> list:
        """Return value is auto-pushed to XCom."""
        orders = [
            {'id': 1, 'product': 'Laptop',   'amount': 1200},
            {'id': 2, 'product': 'Mouse',    'amount': 25},
            {'id': 3, 'product': 'Keyboard', 'amount': 75},
        ]
        print(f"Extracted {len(orders)} orders")
        return orders

    @task
    def transform(orders: list) -> list:
        """Data arrives directly as argument — no xcom_pull needed!"""
        for order in orders:
            order['tax']   = round(order['amount'] * 0.10, 2)
            order['total'] = round(order['amount'] + order['tax'], 2)

        print(f"Transformed {len(orders)} orders, added tax")
        return orders

    @task
    def load(orders: list) -> None:
        """Load the transformed data."""
        for o in orders:
            print(f"  Loading: {o['product']} → ${o['total']}")
        print(f"Loaded {len(orders)} orders")

    # Dependencies via function calls — much cleaner!
    raw_data       = extract()
    processed_data = transform(raw_data)
    load(processed_data)

    # ═════════════════════════════════════════════════════════
    #  SECTION 2 — Multiple Outputs
    # ═════════════════════════════════════════════════════════
    # With multiple_outputs=True, each dict key becomes
    # a separate XCom. Downstream tasks pick individual keys.

    @task(multiple_outputs=True)
    def compute_metrics() -> dict:
        """Each key becomes a separate XCom entry."""
        return {
            'total_sales': 15000.0,
            'order_count': 230,
            'avg_order':   65.22,
        }

    @task
    def show_revenue(total_sales: float, order_count: int) -> None:
        """Picks only the keys it needs."""
        print(f"Revenue: ${total_sales:,.2f} across {order_count} orders")

    @task
    def check_average(avg_order: float) -> None:
        """Another downstream picking a different key."""
        if avg_order >= 50:
            print(f"Avg order ${avg_order:.2f} is above target")
        else:
            print(f"Avg order ${avg_order:.2f} is below target!")

    metrics = compute_metrics()
    show_revenue(metrics['total_sales'], metrics['order_count'])
    check_average(metrics['avg_order'])

    # ═════════════════════════════════════════════════════════
    #  SECTION 3 — Branching with @task.branch
    # ═════════════════════════════════════════════════════════
    # Same branching as DAG 01, but using @task.branch decorator.

    @task
    def get_data_size() -> int:
        size = random.randint(10, 200)
        print(f"Data size: {size} MB")
        return size

    @task.branch
    def pick_path(size: int) -> str:
        """Return the task_id of the branch to follow."""
        if size > 100:
            return 'spark_processing'
        else:
            return 'pandas_processing'

    @task
    def spark_processing() -> None:
        print("Using Spark for large dataset")

    @task
    def pandas_processing() -> None:
        print("Using Pandas for small dataset")

    @task(trigger_rule='none_failed')
    def branching_done() -> None:
        print("Branching complete!")

    size   = get_data_size()
    branch = pick_path(size)
    spark  = spark_processing()
    pandas = pandas_processing()
    done   = branching_done()

    branch >> [spark, pandas] >> done

    # ═════════════════════════════════════════════════════════
    #  SECTION 4 — Accessing Airflow Context
    # ═════════════════════════════════════════════════════════
    # Airflow injects context variables by parameter name.
    # Just add ds, ti, etc. as function arguments.

    @task
    def show_context(ds=None, ti=None, **context) -> None:
        """Airflow auto-injects ds, ti, and other context vars."""
        print(f"Execution date : {ds}")
        print(f"Task ID        : {ti.task_id}")
        print(f"DAG ID         : {context['dag'].dag_id}")
        print(f"Run ID         : {context['run_id']}")

    show_context()


# Create the DAG instance
dag_instance = dag_02_taskflow_api()
