# Airflow Operators & Concepts Demo

This repository contains a demonstration DAG (`airflow_operators_demo`) designed for live lectures to practically illustrate core Apache Airflow concepts.

## 1. Concepts Covered in This Demo

The `airflow_operators_demo` DAG covers the following concepts, which can be demonstrated live in the Airflow UI:

| Concept | Implementation in Code |
| :--- | :--- |
| **DAG** | `with DAG(...)` |
| **Task** | Every operator instance |
| **Sensor** | `FileSensor` |
| **Action** | `PythonOperator`, `BashOperator` |
| **Python Callable** | `python_callable=validate_orders` |
| **Dependency** | `>>` |
| **XCom** | `return data` + `ti.xcom_pull()` |
| **Branching** | `BranchPythonOperator` |
| **Skipped State** | Unselected branch |
| **Trigger Rule** | `trigger_rule=TriggerRule.NONE_FAILED` |
| **Jinja Templating** | `{{ ds }}` |
| **Retry & Retry Delay** | `retries=2`, `retry_delay=...` |
| **Reschedule Mode** | `mode="reschedule"` |

These elements align perfectly with the theoretical concepts explained in the presentation slides.

## 2. DAG Execution Flow

```text
                 wait_for_orders_file
                         │
                         ▼
                  validate_orders
                         │
                         │ XCom
                         ▼
                   route_by_volume
                    /          \
                   /            \
                  ▼              ▼
          full_transform    light_transform
                  \              /
                   \            /
                    ▼          ▼
                       complete
```

## 3. Live Demo Steps for the Lecture

Follow this optimal sequence to demonstrate these concepts during the lecture:

### Step 1: Explain the Theoretical Concepts
Review the theoretical concepts on your presentation slides before jumping into the UI.

### Step 2: Show the Demo DAG Code
Open the `dags/operators_demo.py` file and walk through the code, mapping the components to the concepts discussed.

### Step 3: Trigger the DAG in the Astro UI
Go to the Airflow UI (`localhost:8080`) and manually trigger the `airflow_operators_demo` DAG.

### Step 4: Show the Sensor Waiting
The DAG will pause at the `wait_for_orders_file` task.
The state will toggle between **UP_FOR_RESCHEDULE** and **RUNNING** because the file `/tmp/orders_ready.txt` does not exist yet.
> **Script for Students:** *"Look, the Sensor is waiting for the condition to become true before moving forward."*

### Step 5: Create the Trigger File
Open your terminal and run the following command to enter the Astro bash environment:
```bash
astro dev bash
```
Then, create the file to satisfy the sensor condition:
```bash
touch /tmp/orders_ready.txt
```
*(Note: This is a local demo. Tell the students that in a production environment, you would use an `S3KeySensor` instead of a local `FileSensor`.)*

### Step 6: Observe the Graph Progression (Success/Skip)
Once the file is created, the Sensor will succeed on its next check, and the downstream tasks will start executing.

### Step 7: Demonstrate XCom
Show the logs or the XCom tab for the `validate_orders` task. It returns:
```python
return {
    "row_count": 150,
    "total_amount": 50000
}
```
Explain how this data is pulled using `ti.xcom_pull()`.
> **Script for Students:** *"Task 1 and Task 2 do not share the same memory. Therefore, we use XCom to pass small pieces of data between tasks."*
*(Relate this back to the slide describing XCom for small task-to-task communication).*

### Step 8: Explain Branching
Point out that we hardcoded the `row_count = 150`.
Because `150 > 100`, the branch function returns `"full_transform"`.
Show the UI Graph view:
- `full_transform` is marked as **SUCCESS**.
- `light_transform` is marked as **SKIPPED**.

> **Script for Students:** *"Notice how the Branch task didn't do any transformation itself. It only decided which downstream task should run next."*
*(This aligns exactly with the branching concept in the presentation).*

### Step 9: Explain the `none_failed` Trigger Rule
Look at the `complete` task. We set `trigger_rule=TriggerRule.NONE_FAILED`.
Because `light_transform` was SKIPPED, the default trigger rule (`all_success`) would have caused the `complete` task to be skipped as well. By using `none_failed`, the `complete` task runs successfully after a branching operation.
*(Relate this back to Slide 14 explaining trigger rules).*

### Step 10: Show Jinja Templating in Logs
Open the logs for the `complete` task. You will see:
```text
Pipeline completed
Execution date: 2026-09-28
```
This shows how `{{ ds }}` in the `bash_command` dynamically injected the execution date.
*(This connects to the previous Jinja slide).*

### Step 11: Connect to Production Architecture
> **Script for Students:** *"Right now, we saw the orchestration concepts working in a local environment. In a real production pipeline:
> - The `FileSensor` is replaced by an `S3KeySensor`.
> - Local Python transformations are replaced by actual data processing jobs.
> - The final Bash task could be a `SnowflakeOperator` or a Transfer Operator."*

*(This connects perfectly with the production-style architecture slide (Slide 18) at the end of the presentation).*

---
**Summary of the best sequence:**
PPT concepts ➔ Demo DAG Code ➔ Astro UI ➔ Trigger ➔ Show Sensor waiting ➔ Create File ➔ Show Graph success/skip ➔ Show XCom ➔ Show Logs ➔ Explain Branching ➔ Explain `none_failed` ➔ Production S3 + Snowflake architecture.

By following this, the lecture won't just be theoretical — students will literally see the concepts happening live in the Airflow UI!
