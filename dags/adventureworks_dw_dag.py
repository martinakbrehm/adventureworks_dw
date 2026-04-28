"""
DAG: adventureworks_dw
Descrição: Orquestra o pipeline dbt do projeto AdventureWorks DW usando
           Astronomer Cosmos, seguindo as práticas do Modern Data Stack.

Camadas executadas:
  1. staging  — views que limpam e renomeiam os dados brutos das fontes
  2. marts    — tabelas dimensionais e fatos para consumo analítico

Requisitos:
  pip install astronomer-cosmos apache-airflow-providers-common-sql
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from cosmos import DbtDag, DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping  # troque pelo seu adapter
from cosmos.constants import LoadMode

# ---------------------------------------------------------------------------
# Configuração do projeto dbt
# ---------------------------------------------------------------------------
PROJECT_ROOT = "/usr/local/airflow/dbt/adventureworks_dw"  # ajuste conforme o ambiente

project_config = ProjectConfig(
    dbt_project_path=PROJECT_ROOT,
)

# Troque SnowflakeUserPasswordProfileMapping pelo mapping do seu banco de dados.
# Opções disponíveis no cosmos: Postgres, BigQuery, Redshift, Databricks, DuckDB, etc.
profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="adventureworks_snowflake",  # Connection cadastrada no Airflow
        profile_args={
            "database": "adventureworks_dw",
            "schema": "dbt_transformed",
        },
    ),
)

# ---------------------------------------------------------------------------
# Argumentos padrão
# ---------------------------------------------------------------------------
default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
with DAG(
    dag_id="adventureworks_dw",
    default_args=default_args,
    description="Pipeline dbt do projeto AdventureWorks DW",
    schedule_interval="0 6 * * *",  # executa diariamente às 06h UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dbt", "adventureworks", "modern_data_stack"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # ------------------------------------------------------------------
    # Camada Staging — materializa como views
    # ------------------------------------------------------------------
    staging = DbtTaskGroup(
        group_id="staging",
        project_config=project_config,
        profile_config=profile_config,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=["tag:staging"],
        ),
    )

    # ------------------------------------------------------------------
    # Camada Marts — materializa como tables (dims + fatos)
    # ------------------------------------------------------------------
    marts = DbtTaskGroup(
        group_id="marts",
        project_config=project_config,
        profile_config=profile_config,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=["tag:marts"],
        ),
    )

    # ------------------------------------------------------------------
    # Dependências
    # ------------------------------------------------------------------
    start >> staging >> marts >> end
