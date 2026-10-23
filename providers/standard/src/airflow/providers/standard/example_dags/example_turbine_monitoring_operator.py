# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Tutorial example Dag: turbine monitoring storyline (PythonOperator).

Classic, ``PythonOperator`` based counterpart of
``example_turbine_monitoring_decorator.py``. It runs the same
"list, check each, summarize" turbine monitoring pipeline so that learners
can compare dynamic task mapping in the TaskFlow API and in the
operator-based style on identical business logic.

The Dag has no external dependencies (no connections, no datasets, no hooks),
so it parses and runs out of the box and is suitable as a tutorial or as a
documentation snippet.
"""

from __future__ import annotations

import pendulum

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

DAG_DOC_MD = """
### Turbine monitoring (PythonOperator)

Tutorial Dag showing a "list, check each, summarize" monitoring pipeline
implemented with classic ``PythonOperator`` tasks and dynamic task mapping.

**Storyline**

1. `list_turbine_readings` returns the latest reading of every turbine.
2. `check_turbine` is mapped over the readings with
   `PythonOperator.partial(...).expand(op_kwargs=...)`, so each turbine gets
   its own task instance.
3. `report_fleet_health` pulls all check results from XCom and logs the
   turbines that need attention.

**When to use this example**

- Comparing dynamic task mapping in the TaskFlow API and the classic operator
  style on the same storyline (see ``example_turbine_monitoring_decorator.py``).
- As a reference shape for new "tutorial" example Dags following the
  [example Dag review checklist](
  https://github.com/apache/airflow/blob/main/contributing-docs/28_example_dag_review_checklist.rst).
"""

MAX_GEARBOX_TEMPERATURE_C = 80.0
MAX_VIBRATION_MM_S = 7.1


def list_turbine_readings(**context):
    """Return the latest telemetry reading of every turbine.

    Each dict holds the ``op_kwargs`` of one mapped ``check_turbine`` task
    instance, so the returned list can be passed to ``expand`` directly.
    """
    return [
        {"turbine_id": "WT-01", "gearbox_temperature_c": 65.2, "vibration_mm_s": 3.4},
        {"turbine_id": "WT-02", "gearbox_temperature_c": 84.9, "vibration_mm_s": 4.1},
        {"turbine_id": "WT-03", "gearbox_temperature_c": 71.0, "vibration_mm_s": 9.8},
    ]


def check_turbine(turbine_id, gearbox_temperature_c, vibration_mm_s):
    """Compare one turbine's reading against the operating limits.

    The limits are module constants for brevity; a production pipeline would
    read them from a Variable or a per-turbine-model configuration.
    """
    issues = []
    if gearbox_temperature_c > MAX_GEARBOX_TEMPERATURE_C:
        issues.append(f"gearbox temperature {gearbox_temperature_c} C")
    if vibration_mm_s > MAX_VIBRATION_MM_S:
        issues.append(f"vibration {vibration_mm_s} mm/s")
    return {"turbine_id": turbine_id, "issues": issues}


def report_fleet_health(**context):
    """Log a fleet health report built from all mapped check results.

    Pulling from a mapped task returns the results of every mapped task
    instance. The tutorial implementation logs via ``print``; a production
    Dag would notify the maintenance team or open a work order instead.
    """
    results = list(context["ti"].xcom_pull(task_ids="check_turbine"))
    unhealthy = [result for result in results if result["issues"]]
    print(f"{len(results) - len(unhealthy)} of {len(results)} turbines are healthy")
    for result in unhealthy:
        print(f"{result['turbine_id']} needs attention: {', '.join(result['issues'])}")


# [START example_turbine_monitoring_operator]
with DAG(
    dag_id="example_turbine_monitoring_operator",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["example"],
    doc_md=DAG_DOC_MD,
) as dag:
    list_readings = PythonOperator(
        task_id="list_turbine_readings",
        python_callable=list_turbine_readings,
    )

    check = PythonOperator.partial(
        task_id="check_turbine",
        python_callable=check_turbine,
    ).expand(op_kwargs=list_readings.output)

    report = PythonOperator(
        task_id="report_fleet_health",
        python_callable=report_fleet_health,
    )

    check >> report
# [END example_turbine_monitoring_operator]
