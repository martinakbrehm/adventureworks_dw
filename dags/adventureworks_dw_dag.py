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
from airflow.operators.python import PythonOperator

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
    # Camada ML — Previsão de Demanda
    # ------------------------------------------------------------------
    def run_demand_forecast(**context):
        """
        Executa o pipeline de previsão de demanda após a camada Marts.
        Lê dados do Snowflake via fact_sales e persiste artefatos no
        diretório ml/artifacts/.

        Para rodar em modo desenvolvimento sem Snowflake:
            altere source='synthetic' abaixo.
        """
        import sys
        import os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ml"))
        from demand_forecast import DemandForecastPipeline

        # Lê credenciais do Snowflake a partir de variáveis de ambiente.
        # Configure no Airflow UI: Admin → Variables ou Connections.
        snowflake_params = {
            "user":       os.getenv("SNOWFLAKE_USER"),
            "password":   os.getenv("SNOWFLAKE_PASSWORD"),
            "account":    os.getenv("SNOWFLAKE_ACCOUNT"),
            "warehouse":  os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            "database":   os.getenv("SNOWFLAKE_DATABASE", "adventureworks_dw"),
            "schema":     os.getenv("SNOWFLAKE_SCHEMA", "dbt_transformed"),
        }

        # Usa 'synthetic' se as credenciais não estiverem definidas (CI/CD)
        source = "snowflake" if all(snowflake_params.values()) else "synthetic"

        pipeline = DemandForecastPipeline(
            source=source,
            snowflake_conn_params=snowflake_params if source == "snowflake" else {},
            forecast_horizon=6,
            save=True,
        )
        result = pipeline.run()

        # Disponibiliza métricas no XCom para monitoramento
        context["ti"].xcom_push(
            key="champion_model",
            value=result.champion_model_name,
        )
        context["ti"].xcom_push(
            key="metrics",
            value=result.metrics_df.to_dict(orient="records"),
        )

    demand_forecast = PythonOperator(
        task_id="demand_forecast_ml",
        python_callable=run_demand_forecast,
        provide_context=True,
        doc_md="""
        ## Previsão de Demanda — ML
        Treina e avalia múltiplos modelos de regressão (LinearRegression,
        Ridge, RandomForest, GradientBoosting) sobre as vendas mensais
        agregadas do *fact_sales*, usando walk-forward cross-validation.

        **Saídas salvas em** `ml/artifacts/`:
        - `champion_model.pkl` — modelo serializado
        - `model_metrics.csv` — comparativo de métricas (MAE, RMSE, MAPE, R²)
        - `forecast.csv` — previsão dos próximos 6 meses
        - `feature_importance.csv` — importância das features (modelos tree-based)
        """,
    )

    # ------------------------------------------------------------------
    # Dependências
    # ------------------------------------------------------------------
    start >> staging >> marts >> demand_forecast >> end
