"""
═══════════════════════════════════════════════════════════════
  DAG 04 — XCom, Trigger Rules, Dynamic Mapping & Task Groups
═══════════════════════════════════════════════════════════════
  Topics:
    • XCom basics (auto push/pull)
    • XCom with named keys (explicit push/pull)
    • All 6 trigger rules (live demo)
    • Dynamic Task Mapping (.expand, .partial)
    • Task Groups (@task_group)

  How to Run:
    1. Unpause and trigger manually
    2. In Graph View: expand task groups to see inside
    3. Check XCom tab on tasks to see passed values
    4. In trigger_rules group: see which tasks ran/skipped
═══════════════════════════════════════════════════════════════
"""

from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import json
import random


@dag(
    dag_id='dag_04_xcom_triggers_dynamic_groups',
    description='XCom, Trigger Rules, Dynamic Mapping & Task Groups',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={'owner': 'uday', 'retries': 1},
    tags=['lecture', 'xcom', 'trigger-rules', 'dynamic', 'task-groups', 'uday'],
)
def dag_04_xcom_triggers_dynamic_groups():

    start = EmptyOperator(task_id='start')
    end   = EmptyOperator(task_id='end', trigger_rule='all_done')

    # ═════════════════════════════════════════════════════════
    #  SECTION 1 — XCom Basics
    # ═════════════════════════════════════════════════════════

    @task_group(group_id='xcom_basics')
    def xcom_basics():

        # Auto XCom: return value is pushed automatically
        @task
        def produce() -> dict:
            result = {'sales': 50000, 'region': 'US-West'}
            print(f"Producing: {result}")
            return result

        @task
        def consume(data: dict) -> None:
            print(f"Consumed: sales={data['sales']}, region={data['region']}")

        # Explicit XCom: push/pull with named keys
        @task
        def push_named(ti=None) -> None:
            ti.xcom_push(key='revenue', value=75000)
            ti.xcom_push(key='product', value='Widget')
            print("Pushed: revenue=75000, product=Widget")

        @task
        def pull_named(ti=None) -> None:
            rev = ti.xcom_pull(task_ids='xcom_basics.push_named', key='revenue')
            prod = ti.xcom_pull(task_ids='xcom_basics.push_named', key='product')
            print(f"Pulled: revenue={rev}, product={prod}")

        # Multi-upstream: pull from parallel tasks
        @task
        def get_orders() -> dict:
            return {'source': 'orders', 'count': 500}

        @task
        def get_customers() -> dict:
            return {'source': 'customers', 'count': 200}

        @task
        def combine(orders: dict, customers: dict) -> None:
            total = orders['count'] + customers['count']
            print(f"Combined: {orders['source']}={orders['count']} + "
                  f"{customers['source']}={customers['count']} = {total}")

        # Wire up
        data = produce()
        consume(data)

        p = push_named()
        pl = pull_named()
        p >> pl

        o = get_orders()
        c = get_customers()
        combine(o, c)

    # ═════════════════════════════════════════════════════════
    #  SECTION 2 — Trigger Rules (All 6)
    # ═════════════════════════════════════════════════════════
    # One task succeeds, one fails → see which rules fire.
    #
    # all_success  → ALL parents must succeed    (default)
    # all_failed   → ALL parents must fail
    # one_success  → at least ONE succeeded
    # one_failed   → at least ONE failed
    # none_failed  → no parent FAILED (skip is OK)
    # all_done     → all finished, any state

    @task_group(group_id='trigger_rules')
    def trigger_rules():

        @task
        def success_task() -> None:
            print("This task succeeds")

        @task
        def fail_task() -> None:
            raise Exception("This task fails on purpose!")

        @task(trigger_rule='all_success')
        def rule_all_success() -> None:
            print("all_success: ran because ALL parents succeeded")

        @task(trigger_rule='all_failed')
        def rule_all_failed() -> None:
            print("all_failed: ran because ALL parents failed")

        @task(trigger_rule='one_success')
        def rule_one_success() -> None:
            print("one_success: ran because at least ONE parent succeeded")

        @task(trigger_rule='one_failed')
        def rule_one_failed() -> None:
            print("one_failed: ran because at least ONE parent failed")

        @task(trigger_rule='none_failed')
        def rule_none_failed() -> None:
            print("none_failed: ran because no parent FAILED")

        @task(trigger_rule='all_done')
        def rule_all_done() -> None:
            print("all_done: ran because all parents FINISHED (any state)")

        s = success_task()
        f = fail_task()

        [s, f] >> rule_all_success()
        [s, f] >> rule_all_failed()
        [s, f] >> rule_one_success()
        [s, f] >> rule_one_failed()
        [s, f] >> rule_none_failed()
        [s, f] >> rule_all_done()

    # ═════════════════════════════════════════════════════════
    #  SECTION 3 — Dynamic Task Mapping
    # ═════════════════════════════════════════════════════════
    # .expand() creates parallel tasks at RUNTIME.
    # You don't need to know the count when writing the DAG.

    @task_group(group_id='dynamic_mapping')
    def dynamic_mapping():

        @task
        def get_regions() -> list:
            regions = ['North', 'South', 'East', 'West']
            print(f"Found {len(regions)} regions")
            return regions

        @task
        def process_region(region: str) -> dict:
            """Runs once per region — in parallel!"""
            records = random.randint(100, 1000)
            print(f"Region {region}: {records} records")
            return {'region': region, 'records': records}

        @task
        def summarise(results: list) -> None:
            """Gets ALL results after all mapped tasks finish."""
            total = sum(r['records'] for r in results)
            for r in results:
                print(f"  {r['region']}: {r['records']} records")
            print(f"  Total: {total} records")

        regions = get_regions()
        results = process_region.expand(region=regions)  # 4 parallel tasks!
        summarise(results)

        # .partial() + .expand() example
        @task
        def get_files() -> list:
            return ['q1.csv', 'q2.csv', 'q3.csv']

        @task
        def convert_file(filename: str, output_format: str) -> None:
            print(f"Converting {filename} → {output_format}")

        files = get_files()
        convert_file.partial(output_format='parquet').expand(filename=files)

    # ═════════════════════════════════════════════════════════
    #  SECTION 4 — Task Groups
    # ═════════════════════════════════════════════════════════
    # Task Groups are a UI feature for visual organisation.
    # They appear as collapsible boxes in Graph View.

    @task_group(group_id='etl_pipeline')
    def etl_pipeline():

        @task_group(group_id='extract_phase')
        def extract_phase() -> str:
            @task
            def validate() -> bool:
                print("Validating source...")
                return True

            @task
            def extract(is_valid: bool) -> str:
                print("Extracting data...")
                return json.dumps({'users': 3, 'orders': 10})

            return extract(validate())

        @task_group(group_id='transform_phase')
        def transform_phase(raw: str) -> str:
            @task
            def clean(data: str) -> str:
                print("Cleaning data...")
                return data

            @task
            def enrich(data: str) -> str:
                print("Enriching data...")
                return data

            return enrich(clean(raw))

        @task
        def load(data: str) -> None:
            print(f"Loading to warehouse: {data}")

        raw         = extract_phase()
        transformed = transform_phase(raw)
        load(transformed)

    # ═════════════════════════════════════════════════════════
    #  DEPENDENCIES
    # ═════════════════════════════════════════════════════════

    xcom    = xcom_basics()
    triggers = trigger_rules()
    dynamic = dynamic_mapping()
    etl     = etl_pipeline()

    start >> xcom >> end
    start >> triggers >> end
    start >> dynamic >> end
    start >> etl >> end


dag_instance = dag_04_xcom_triggers_dynamic_groups()
