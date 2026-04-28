"""
data_loader.py
Conecta ao Snowflake via variáveis de ambiente e executa as queries do dashboard.
Se as credenciais não estiverem disponíveis, retorna dados de amostra para
visualização offline — útil para demos e desenvolvimento local.
"""

from __future__ import annotations

import os
import warnings
from typing import Optional

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# ── Conexão Snowflake ──────────────────────────────────────────────────────────

def _get_snowflake_connection():
    """Retorna uma conexão snowflake-connector-python ou None se indisponível."""
    try:
        import snowflake.connector  # type: ignore

        conn = snowflake.connector.connect(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
            role=os.getenv("SNOWFLAKE_ROLE", "TRANSFORMER"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            database=os.getenv("SNOWFLAKE_DATABASE", "ADVENTUREWORKS_DW"),
            schema=os.getenv("SNOWFLAKE_SCHEMA", "DBT_TRANSFORMED"),
        )
        return conn
    except Exception as exc:  # noqa: BLE001
        warnings.warn(
            f"Snowflake indisponível — usando dados de amostra. Detalhe: {exc}",
            stacklevel=2,
        )
        return None


def run_query(sql: str) -> Optional[pd.DataFrame]:
    conn = _get_snowflake_connection()
    if conn is None:
        return None
    try:
        df = pd.read_sql(sql, conn)
        df.columns = [c.lower() for c in df.columns]
        return df
    finally:
        conn.close()


# ── Dados de Amostra (fallback offline) ───────────────────────────────────────

def sample_kpi() -> pd.DataFrame:
    return pd.DataFrame([{
        "total_orders": 31_465,
        "total_customers": 19_119,
        "total_revenue": 109_846_381.40,
        "avg_order_value": 3_490.57,
        "total_product_revenue": 91_734_248.11,
        "total_items_sold": 275_862,
    }])


def sample_monthly_revenue() -> pd.DataFrame:
    import numpy as np
    months = pd.date_range("2011-05-01", "2014-06-01", freq="MS")
    rng = np.random.default_rng(42)
    base = np.linspace(1_500_000, 4_200_000, len(months))
    noise = rng.normal(0, 180_000, len(months))
    return pd.DataFrame({
        "order_month": months,
        "monthly_revenue": (base + noise).clip(500_000).round(2),
        "monthly_orders": (rng.integers(400, 1800, len(months))).tolist(),
    })


def sample_top_products() -> pd.DataFrame:
    products = [
        ("Mountain-200 Black, 38", "Mountain Bikes", "Bikes", 4_279_440),
        ("Mountain-200 Black, 42", "Mountain Bikes", "Bikes", 4_032_210),
        ("Mountain-200 Silver, 38","Mountain Bikes", "Bikes", 3_890_300),
        ("Road-150 Red, 62",       "Road Bikes",     "Bikes", 3_320_100),
        ("Road-150 Red, 56",       "Road Bikes",     "Bikes", 3_180_540),
        ("Touring-1000 Blue, 60",  "Touring Bikes",  "Bikes", 2_943_000),
        ("Mountain-100 Silver, 38","Mountain Bikes", "Bikes", 2_560_700),
        ("Road-350-W Yellow, 40",  "Road Bikes",     "Bikes", 2_120_400),
        ("HL Mountain Frame Black","Mountain Frames","Components",1_980_200),
        ("HL Road Frame Black, 58","Road Frames",    "Components",1_750_900),
    ]
    return pd.DataFrame(products, columns=["product_name","subcategory","category","total_revenue"])


def sample_revenue_by_country() -> pd.DataFrame:
    return pd.DataFrame([
        ("United States",  56_420_810, 14_220),
        ("Australia",      18_990_340,  4_882),
        ("Canada",         12_340_120,  3_110),
        ("United Kingdom",  9_870_440,  2_540),
        ("France",          6_140_290,  1_680),
        ("Germany",         5_930_110,  1_560),
    ], columns=["country","total_revenue","total_orders"])


def sample_sales_channel() -> pd.DataFrame:
    return pd.DataFrame([
        ("Online",       27_659, 64_310_420.80),
        ("Loja Física",   3_806, 45_535_960.60),
    ], columns=["channel","total_orders","total_revenue"])


def sample_sales_reasons() -> pd.DataFrame:
    return pd.DataFrame([{
        "price_count": 9_840,
        "manufacturer_count": 4_210,
        "quality_count": 3_180,
        "promotion_count": 7_920,
        "review_count": 1_640,
        "other_count": 2_310,
        "television_count": 1_020,
    }])


def sample_order_status() -> pd.DataFrame:
    return pd.DataFrame([
        ("Enviado",          27_659, 98_450_210),
        ("Em processo",       2_130,  6_320_900),
        ("Aprovado",          1_240,  3_810_440),
        ("Cancelado",           312,    892_100),
        ("Pedido em espera",    124,    372_731),
    ], columns=["status","total_orders","total_revenue"])


def sample_sellers_by_region() -> pd.DataFrame:
    return pd.DataFrame([
        ("United States","M","Sales Representative",4_820),
        ("United States","F","Sales Representative",3_910),
        ("Australia",    "M","Sales Representative",1_240),
        ("Australia",    "F","Sales Representative",  980),
        ("Canada",       "M","Sales Representative",  870),
        ("Canada",       "F","Sales Representative",  730),
        ("United Kingdom","M","Sales Representative",  690),
        ("United Kingdom","F","Sales Representative",  560),
        ("France",       "M","Sales Manager",          430),
        ("Germany",      "F","Sales Manager",          380),
    ], columns=["country","gender","jobtitle","total_orders"])


def sample_revenue_by_category() -> pd.DataFrame:
    return pd.DataFrame([
        ("Bikes",      85_143_324,  64_292_100, 20_851_224),
        ("Components",  9_738_440,   7_420_310,  2_318_130),
        ("Clothing",    2_412_780,   1_640_220,    772_560),
        ("Accessories", 1_290_510,     840_440,    450_070),
    ], columns=["category","total_revenue","total_cost","gross_profit"])


# ── Interface pública ──────────────────────────────────────────────────────────

def load(query_sql: str, fallback_fn) -> pd.DataFrame:
    """Tenta Snowflake; cai no fallback de amostra se necessário."""
    result = run_query(query_sql)
    if result is not None and not result.empty:
        return result
    return fallback_fn()
