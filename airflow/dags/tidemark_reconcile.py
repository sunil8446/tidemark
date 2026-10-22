"""Hourly: prove (and repair) lakehouse completeness, then rebuild the dbt marts.

reconcile  ->  wait for Flink to commit any repaired trades  ->  dbt build
"""

from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

TRINO_DSN = "http://tidemark@trino:8080?catalog=iceberg&schema=market"
DBT_ENV = {
    "TRINO_HOST": "trino",
    "TRINO_PORT": "8080",
    "DBT_TARGET_PATH": "/tmp/dbt/target",
    "DBT_LOG_PATH": "/tmp/dbt/logs",
}


@dag(
    dag_id="tidemark_reconcile_and_dbt",
    schedule="15 * * * *",
    start_date=datetime(2026, 10, 22),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["tidemark", "completeness"],
    doc_md=__doc__,
)
def tidemark_reconcile_and_dbt():
    reconcile = BashOperator(
        task_id="reconcile",
        bash_command=(
            "/opt/tidemark/bin/reconcile --window 2h --settle 10m "
            f"--trino '{TRINO_DSN}' --brokers redpanda-0:9092 --registry http://redpanda-0:8081"
        ),
    )
    # The lakehouse job checkpoints (and so commits to Iceberg) every 60 s.
    wait_for_commit = BashOperator(task_id="wait_for_flink_commit", bash_command="sleep 90")
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command="cd /opt/tidemark/dbt && /home/airflow/dbt-venv/bin/dbt build --profiles-dir .",
        env=DBT_ENV,
        append_env=True,
    )
    reconcile >> wait_for_commit >> dbt_build


tidemark_reconcile_and_dbt()
