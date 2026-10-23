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
Tutorial example Dag: turbine monitoring storyline (TaskFlow).

This Dag is part of the "Examples Refurbish" storyline. A wind farm operator
collects telemetry from each turbine and wants a health report for the whole
fleet. The Dag demonstrates dynamic task mapping with the TaskFlow API on a
self-contained workflow that:

1. lists the latest telemetry reading of every turbine,
2. checks each turbine in its own mapped task instance,
3. summarizes the fleet health in a single report.

The Dag has no external dependencies (no connections, no datasets, no hooks),
so it parses and runs out of the box. Pair it with
``example_turbine_monitoring_operator.py`` to compare TaskFlow and the
classic ``PythonOperator`` style on the exact same storyline.
"""

from __future__ import annotations

import pendulum

from airflow.sdk import dag, task

DAG_DOC_MD = """
### Turbine monitoring (TaskFlow)

Tutorial Dag showing a "list, check each, summarize" monitoring pipeline
implemented with the TaskFlow API and dynamic task mapping.

**Storyline**

1. `list_turbine_readings` returns the latest reading of every turbine.
2. `check_turbine` is mapped over the readings, so each turbine gets its own
   task instance and can be inspected (or retried) on its own in the Grid view.
3. `report_fleet_health` collects all check results and logs the turbines
   that need attention.

**When to use this example**

- Learning dynamic task mapping (`expand_kwargs`) with the TaskFlow API.
- As a reference shape for new "tutorial" example Dags following the
  [example Dag review checklist](
  https://github.com/apache/airflow/blob/main/contributing-docs/28_example_dag_review_checklist.rst).
"""

MAX_GEARBOX_TEMPERATURE_C = 80.0
MAX_VIBRATION_MM_S = 7.1


# [START example_turbine_monitoring_decorator]
@dag(
    dag_id="example_turbine_monitoring_decorator",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["example"],
    doc_md=DAG_DOC_MD,
)
def turbine_monitoring_decorator():
    """Tutorial Dag: check every turbine of the fleet and report its health."""

    @task
    def list_turbine_readings() -> list[dict]:
        """Return the latest telemetry reading of every turbine.

        In a real pipeline this would query the SCADA system or an upstream
        dataset. For the tutorial it returns constants so the Dag is fully
        self-contained. Each dict holds the keyword arguments of one
        ``check_turbine`` task instance.
        """
        return [
            {"turbine_id": "WT-01", "gearbox_temperature_c": 65.2, "vibration_mm_s": 3.4},
            {"turbine_id": "WT-02", "gearbox_temperature_c": 84.9, "vibration_mm_s": 4.1},
            {"turbine_id": "WT-03", "gearbox_temperature_c": 71.0, "vibration_mm_s": 9.8},
        ]

    @task
    def check_turbine(turbine_id: str, gearbox_temperature_c: float, vibration_mm_s: float) -> dict:
        """Compare one turbine's reading against the operating limits.

        The limits are module constants for brevity; a real pipeline would
        read them from a Variable or a per-turbine-model configuration.
        """
        issues = []
        if gearbox_temperature_c > MAX_GEARBOX_TEMPERATURE_C:
            issues.append(f"gearbox temperature {gearbox_temperature_c} C")
        if vibration_mm_s > MAX_VIBRATION_MM_S:
            issues.append(f"vibration {vibration_mm_s} mm/s")
        return {"turbine_id": turbine_id, "issues": issues}

    @task
    def report_fleet_health(results: list[dict]) -> None:
        """Log a fleet health report.

        The tutorial implementation logs via ``print`` so the report is
        visible in the task logs. A production Dag would notify the
        maintenance team or open a work order instead.
        """
        unhealthy = [result for result in results if result["issues"]]
        print(f"{len(results) - len(unhealthy)} of {len(results)} turbines are healthy")
        for result in unhealthy:
            print(f"{result['turbine_id']} needs attention: {', '.join(result['issues'])}")

    readings = list_turbine_readings()
    results = check_turbine.expand_kwargs(readings)
    report_fleet_health(results)


turbine_monitoring_decorator()
# [END example_turbine_monitoring_decorator]
