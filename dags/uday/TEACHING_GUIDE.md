# 📘 Apache Airflow — Operators, Hooks, Scheduling & TaskFlow API

## Teaching Guide & Demo Walkthrough

---

## 🗂️ DAG Overview

| DAG | File | What It Covers |
|-----|------|----------------|
| **DAG 01** | `dag_01_operators_hooks_dependencies.py` | Traditional syntax: PythonOperator, BashOperator, BranchPythonOperator, Hooks, Dependencies |
| **DAG 02** | `dag_02_taskflow_api.py` | TaskFlow API: @task, @dag, auto XCom, multiple outputs, @task.branch, context |
| **DAG 03** | `dag_03_scheduling_sensors.py` | Execution date, schedule options, DAG/task parameters, retries, sensors |
| **DAG 04** | `dag_04_xcom_triggers_dynamic_groups.py` | XCom, all 6 trigger rules, dynamic task mapping, task groups |

> **NOTE:** All DAGs use `schedule=None` (manual trigger only). Students trigger and observe at their own pace.

---

## 🧭 Recommended Teaching Sequence

```
DAG 01  →  DAG 02  →  DAG 03  →  DAG 04
Traditional   TaskFlow    Scheduling   Advanced
 Operators     API         & Sensors    Patterns
```

---

## 📖 Step-by-Step Walkthrough

---

### 🔷 DAG 01 — Traditional Operators, Hooks & Dependencies

**File:** `dag_01_operators_hooks_dependencies.py`
**Action:** Unpause → Trigger → Graph View

#### 1.1 PythonOperator (Simple ETL)

**Tasks:** `extract` → `transform` → `load`

- Click `extract` → **Log**: see how `return orders` auto-pushes to XCom
- Click `extract` → **XCom tab**: see the stored value
- Click `transform` → **Log**: see `ti.xcom_pull(task_ids='extract')` fetching data
- Click `load` → **Log**: see final loaded output

**Key point:** Any return value from a Python function is stored in XCom automatically.

```python
def extract_data():
    orders = [{'id': 1, 'product': 'Laptop', 'amount': 1200}]
    return orders   # ← stored in XCom (key='return_value')

def transform_data(**context):
    ti = context['ti']
    orders = ti.xcom_pull(task_ids='extract')   # ← pull from XCom
```

#### 1.2 BashOperator + Jinja Templates

**Tasks:** `bash_show_info` → `bash_create_file`

- Click `bash_show_info` → **Log**: see `{{ ds }}`, `{{ dag.dag_id }}` replaced with real values
- These are **Jinja template variables** — Airflow substitutes them at runtime

| Variable | Example | What it is |
|----------|---------|------------|
| `{{ ds }}` | `2024-01-15` | Execution date |
| `{{ ds_nodash }}` | `20240115` | Without dashes |
| `{{ dag.dag_id }}` | `my_dag` | DAG name |
| `{{ task.task_id }}` | `extract` | Task name |
| `{{ run_id }}` | `manual__2024-...` | Run identifier |

#### 1.3 BranchPythonOperator

**Tasks:** `branch_decide` → `[process_large, process_small]` → `branch_done`

- After running, **one branch is green** (ran), **other is pink** (skipped)
- Click `branch_decide` → **Log**: see the decision
- `branch_done` uses `trigger_rule='none_failed'` — essential after branching!

```python
def pick_branch():
    if size > 100:
        return 'process_large'   # ← return the task_id to run
    else:
        return 'process_small'
```

#### 1.4 Hook Concept

**Task:** `hook_demo`

- Click → **Log**: see BAD (hardcoded) vs GOOD (hook) comparison
- Explain: Operator = **what** to do, Hook = **how** to connect

```
PostgresOperator  ──uses──→  PostgresHook
SnowflakeOperator ──uses──→  SnowflakeHook
```

#### 1.5 Dependencies

Show in code how tasks are wired:

```python
start >> extract >> transform >> load >> end        # chain
start >> branch_decide >> [large, small] >> done    # branching
```

---

### 🔷 DAG 02 — TaskFlow API (The Modern Way)

**File:** `dag_02_taskflow_api.py`
**Action:** Unpause → Trigger → Compare with DAG 01!

**Key message:** Same logic as DAG 01, but cleaner syntax.

#### 2.1 Basic ETL — Traditional vs TaskFlow

| Traditional (DAG 01) | TaskFlow (DAG 02) |
|----------------------|-------------------|
| `PythonOperator(task_id=..., python_callable=...)` | `@task` decorator |
| `ti.xcom_pull(task_ids='extract')` | Data arrives as function argument |
| `extract >> transform >> load` | `transform(extract())` |

```python
# TaskFlow — clean!
@task
def extract() -> list:
    return [{'id': 1, 'product': 'Laptop'}]  # auto XCom

@task
def transform(orders: list) -> list:      # data arrives directly!
    return orders

raw = extract()
processed = transform(raw)  # dependency via function call
```

#### 2.2 Multiple Outputs

**Task:** `compute_metrics`

```python
@task(multiple_outputs=True)
def compute_metrics() -> dict:
    return {'total_sales': 15000, 'order_count': 230}
    # Each key becomes a separate XCom!

metrics = compute_metrics()
show_revenue(metrics['total_sales'], metrics['order_count'])
```

- Click `compute_metrics` → **XCom tab**: see each key as a separate entry

#### 2.3 Branching with @task.branch

Same as DAG 01's BranchPythonOperator but using decorator:

```python
@task.branch
def pick_path(size: int) -> str:
    return 'spark_processing' if size > 100 else 'pandas_processing'
```

#### 2.4 Context Access

```python
@task
def show_context(ds=None, ti=None, **context):
    print(f"Execution date: {ds}")   # Airflow injects these by name
```

---

### 🔷 DAG 03 — Scheduling, Sensors & DAG Configuration

**File:** `dag_03_scheduling_sensors.py`
**Action:** Unpause → Trigger

#### 3.1 Execution Date

**Task:** `show_execution_date`

> **The most confusing concept for beginners!**

```
@daily schedule, start_date = Jan 1:
  Jan 2 00:00 → DAG runs → execution_date = Jan 1
  Jan 3 00:00 → DAG runs → execution_date = Jan 2

The DAG runs AFTER the interval. ds = start of the period.
```

#### 3.2 Schedule Options

**Task:** `show_schedule_options`

| Type | Example | Meaning |
|------|---------|---------|
| Preset | `@daily` | Every midnight |
| Cron | `'0 2 * * *'` | Daily at 2 AM |
| Timedelta | `timedelta(hours=6)` | Every 6 hours |
| None | `schedule=None` | Manual only |

#### 3.3 DAG Parameters

**Task:** `show_dag_params`

| Parameter | Why it matters |
|-----------|---------------|
| `start_date` | Must be fixed — never `datetime.now()`! |
| `catchup=False` | Don't backfill old runs (recommended) |
| `max_active_runs=1` | Prevent overlapping runs |

#### 3.4 Retry Demo

**Task:** `retry_demo`

- This task randomly fails (70% success rate)
- Configured with `retries=3, retry_exponential_backoff=True`
- Watch the retry attempts in the log!

```
Attempt 1 fails → wait 10s
Attempt 2 fails → wait 20s  (doubled)
Attempt 3 fails → wait 40s  (doubled)
```

#### 3.5 Sensors

**Task:** `wait_for_orders_file` → `process_after_sensor`

- FileSensor checks for `include/data/orders_ready.txt`
- If found → succeeds → next task runs
- Two modes:

| Mode | Behaviour | Use when |
|------|-----------|----------|
| `poke` | Holds worker slot | Short waits (< 5 min) |
| `reschedule` | Releases worker | Long waits — **always prefer** |

---

### 🔷 DAG 04 — XCom, Trigger Rules, Dynamic Mapping & Task Groups

**File:** `dag_04_xcom_triggers_dynamic_groups.py`
**Action:** Unpause → Trigger → Expand task groups in Graph View

#### 4.1 XCom (expand `xcom_basics` group)

**Auto XCom:** `produce` → `consume` (data flows as argument)

**Named keys:** `push_named` → `pull_named`
```python
ti.xcom_push(key='revenue', value=75000)
ti.xcom_pull(task_ids='push_named', key='revenue')
```

**Multi-upstream:** `get_orders` + `get_customers` → `combine`

> **Best practice:** Only store small data in XCom. For large data, save to S3 and store the path.

#### 4.2 Trigger Rules (expand `trigger_rules` group)

One task succeeds, one fails. Watch which downstream rules fire:

| Rule | Fires? | Why |
|------|--------|-----|
| `all_success` | ❌ | One parent failed |
| `all_failed` | ❌ | One parent succeeded |
| `one_success` | ✅ | At least one succeeded |
| `one_failed` | ✅ | At least one failed |
| `none_failed` | ❌ | One parent failed |
| `all_done` | ✅ | All finished (any state) |

> **Key takeaway:** Use `trigger_rule='none_failed'` after branching!

#### 4.3 Dynamic Task Mapping (expand `dynamic_mapping` group)

```python
regions = get_regions()                           # returns ['North','South','East','West']
results = process_region.expand(region=regions)   # creates 4 parallel tasks!
summarise(results)                                # gets all results as a list
```

- `.expand()` = create one task per item (dynamic count)
- `.partial()` = set shared/static args

#### 4.4 Task Groups (expand `etl_pipeline` group)

- Collapsible boxes in Graph View — UI-only, doesn't change execution
- Makes large DAGs (50+ tasks) manageable
- Click to expand: `extract_phase` → `transform_phase` → `load`

---

## ❓ Interview Questions

| # | Question | Key Answer |
|---|----------|------------|
| Q1 | Operator vs Hook? | Operator = what to do, Hook = how to connect |
| Q2 | Sensor poke vs reschedule? | Poke holds worker; reschedule releases it |
| Q3 | XCom biggest mistake? | Storing large data — use file paths instead |
| Q4 | TaskFlow advantage? | Auto XCom, cleaner syntax, deps via function calls |
| Q5 | Dynamic Task Mapping? | `.expand()` creates parallel tasks at runtime |
| Q6 | Resource Pool? | Limits concurrent tasks (Admin → Pools) |
| Q7 | `depends_on_past=True`? | Task only runs if previous day's run succeeded |

---

## 📋 Pre-lecture Checklist

- [ ] Airflow is running (`astro dev start`)
- [ ] All 4 DAGs visible in the UI
- [ ] File exists: `include/data/orders_ready.txt` (for sensor demo)
- [ ] Unpause all 4 DAGs
