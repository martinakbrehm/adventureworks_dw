"""
AdventureWorks DW — Previsão de Demanda com Machine Learning
=============================================================
Pipeline completo de ML para previsão de vendas mensais, cobrindo:
  - Extração de dados do Snowflake (ou CSV de fallback)
  - Engenharia de features temporais e de lag
  - Treinamento de múltiplos modelos (baseline → ensemble)
  - Validação walk-forward (time-series cross-validation)
  - Métricas de desempenho: MAE, RMSE, MAPE, R²
  - Exportação do modelo campeão e previsões futuras

Instalação:
    pip install -r ml/requirements.txt

Execução standalone:
    python ml/demand_forecast.py

Execução via Airflow (chamado pelo DAG):
    Instanciar DemandForecastPipeline e chamar .run()
"""

from __future__ import annotations

import logging
import os
import warnings
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Tuple

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
from sklearn.linear_model import LinearRegression, Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import TimeSeriesSplit
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

warnings.filterwarnings("ignore")

# ---------------------------------------------------------------------------
# Configuração de logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------
RANDOM_STATE = 42
N_SPLITS = 5          # splits do walk-forward CV
FORECAST_HORIZON = 6  # meses futuros a prever
ARTIFACTS_DIR = Path(__file__).parent / "artifacts"
DATA_DIR = Path(__file__).parent / "data"

# ---------------------------------------------------------------------------
# Dataclasses de resultado
# ---------------------------------------------------------------------------

@dataclass
class ModelMetrics:
    """Armazena métricas de avaliação de um modelo."""
    model_name: str
    mae: float
    rmse: float
    mape: float
    r2: float
    cv_mae_mean: float
    cv_mae_std: float

    def as_dict(self) -> dict:
        return {
            "model": self.model_name,
            "MAE": round(self.mae, 2),
            "RMSE": round(self.rmse, 2),
            "MAPE (%)": round(self.mape, 2),
            "R²": round(self.r2, 4),
            "CV MAE médio": round(self.cv_mae_mean, 2),
            "CV MAE desvio": round(self.cv_mae_std, 2),
        }


@dataclass
class ForecastResult:
    """Resultado final do pipeline."""
    champion_model_name: str
    metrics_df: pd.DataFrame
    forecast_df: pd.DataFrame
    feature_importance: pd.DataFrame | None = None
    artifacts_dir: Path = ARTIFACTS_DIR


# ---------------------------------------------------------------------------
# Funções utilitárias de métricas
# ---------------------------------------------------------------------------

def mape(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """Mean Absolute Percentage Error — ignora zeros para evitar divisão por zero."""
    mask = y_true != 0
    if mask.sum() == 0:
        return np.nan
    return float(np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask]))) * 100


def compute_metrics(y_true: np.ndarray, y_pred: np.ndarray, model_name: str,
                    cv_scores: np.ndarray) -> ModelMetrics:
    return ModelMetrics(
        model_name=model_name,
        mae=mean_absolute_error(y_true, y_pred),
        rmse=float(np.sqrt(mean_squared_error(y_true, y_pred))),
        mape=mape(y_true, y_pred),
        r2=r2_score(y_true, y_pred),
        cv_mae_mean=float(cv_scores.mean()),
        cv_mae_std=float(cv_scores.std()),
    )


# ---------------------------------------------------------------------------
# Engenharia de Features
# ---------------------------------------------------------------------------

def build_time_features(df: pd.DataFrame, date_col: str = "order_month") -> pd.DataFrame:
    """Adiciona features temporais derivadas da coluna de data."""
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col])
    df["month"] = df[date_col].dt.month
    df["quarter"] = df[date_col].dt.quarter
    df["year"] = df[date_col].dt.year
    df["month_sin"] = np.sin(2 * np.pi * df["month"] / 12)
    df["month_cos"] = np.cos(2 * np.pi * df["month"] / 12)
    df["quarter_sin"] = np.sin(2 * np.pi * df["quarter"] / 4)
    df["quarter_cos"] = np.cos(2 * np.pi * df["quarter"] / 4)
    return df


def build_lag_features(df: pd.DataFrame, target_col: str = "total_revenue",
                        lags: List[int] | None = None) -> pd.DataFrame:
    """Cria features de lag e rolling statistics para o target."""
    if lags is None:
        lags = [1, 2, 3, 6, 12]
    df = df.copy().sort_values("order_month")
    for lag in lags:
        df[f"lag_{lag}"] = df[target_col].shift(lag)
    # Rolling statistics
    df["rolling_3_mean"] = df[target_col].shift(1).rolling(3).mean()
    df["rolling_6_mean"] = df[target_col].shift(1).rolling(6).mean()
    df["rolling_3_std"] = df[target_col].shift(1).rolling(3).std()
    df["rolling_6_std"] = df[target_col].shift(1).rolling(6).std()
    # Tendência linear simples (mês ordinal)
    df["trend"] = np.arange(len(df))
    return df


def prepare_features(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Series]:
    """
    Orquestra a engenharia de features e retorna (X, y).
    Remove linhas com NaN geradas pelos lags.
    """
    df = build_time_features(df)
    df = build_lag_features(df)
    df = df.dropna().reset_index(drop=True)

    feature_cols = [
        "month", "quarter", "year",
        "month_sin", "month_cos", "quarter_sin", "quarter_cos",
        "trend",
        "lag_1", "lag_2", "lag_3", "lag_6", "lag_12",
        "rolling_3_mean", "rolling_6_mean",
        "rolling_3_std", "rolling_6_std",
    ]
    # Mantém apenas colunas existentes (lag_12 pode não existir para séries curtas)
    feature_cols = [c for c in feature_cols if c in df.columns]

    X = df[feature_cols]
    y = df["total_revenue"]
    return X, y


# ---------------------------------------------------------------------------
# Extração / Geração de Dados
# ---------------------------------------------------------------------------

def load_from_snowflake(conn_params: dict) -> pd.DataFrame:
    """
    Carrega dados de vendas mensais do Snowflake.
    conn_params deve conter as chaves aceites pelo snowflake-connector-python.
    """
    import snowflake.connector  # import lazy para não quebrar sem a lib

    query = """
        SELECT
            DATE_TRUNC('month', orderdate)::DATE   AS order_month,
            COUNT(DISTINCT salesorderid)            AS total_orders,
            SUM(totaldue)                           AS total_revenue,
            SUM(orderqty)                           AS total_qty,
            COUNT(DISTINCT customersk)              AS unique_customers,
            AVG(totaldue)                           AS avg_order_value
        FROM dbt_transformed.fact_sales
        JOIN dbt_transformed.dim_dates USING (datesk)
        GROUP BY 1
        ORDER BY 1
    """
    log.info("Conectando ao Snowflake...")
    conn = snowflake.connector.connect(**conn_params)
    df = pd.read_sql(query, conn)
    conn.close()
    df.columns = [c.lower() for c in df.columns]
    log.info("Dados carregados: %d meses.", len(df))
    return df


def load_from_csv(csv_path: str | Path | None = None) -> pd.DataFrame:
    """Carrega dados de um CSV local (fallback / desenvolvimento)."""
    if csv_path is None:
        csv_path = DATA_DIR / "monthly_sales.csv"
    df = pd.read_csv(csv_path, parse_dates=["order_month"])
    df.columns = [c.lower() for c in df.columns]
    log.info("Dados carregados de CSV: %d meses (%s).", len(df), csv_path)
    return df


def generate_synthetic_data(seed: int = RANDOM_STATE) -> pd.DataFrame:
    """
    Gera dados sintéticos que imitam o comportamento sazonal do AdventureWorks
    para uso em testes e desenvolvimento sem acesso ao Snowflake.
    """
    rng = np.random.default_rng(seed)
    dates = pd.date_range("2011-06-01", "2014-06-01", freq="MS")
    n = len(dates)
    trend = np.linspace(200_000, 500_000, n)
    seasonality = 80_000 * np.sin(2 * np.pi * np.arange(n) / 12 - np.pi / 2)
    noise = rng.normal(0, 20_000, n)
    revenue = np.maximum(trend + seasonality + noise, 50_000)

    return pd.DataFrame({
        "order_month": dates,
        "total_orders": (rng.integers(200, 800, n)).astype(int),
        "total_revenue": revenue,
        "total_qty": (revenue / 450).astype(int),
        "unique_customers": (rng.integers(150, 600, n)).astype(int),
        "avg_order_value": revenue / rng.integers(200, 800, n),
    })


# ---------------------------------------------------------------------------
# Modelos candidatos
# ---------------------------------------------------------------------------

def get_candidate_models() -> Dict[str, Pipeline]:
    """Retorna dicionário {nome: Pipeline sklearn} com os modelos candidatos."""
    return {
        "LinearRegression (baseline)": Pipeline([
            ("scaler", StandardScaler()),
            ("model", LinearRegression()),
        ]),
        "Ridge": Pipeline([
            ("scaler", StandardScaler()),
            ("model", Ridge(alpha=10.0)),
        ]),
        "RandomForest": Pipeline([
            ("model", RandomForestRegressor(
                n_estimators=200,
                max_depth=6,
                min_samples_leaf=3,
                random_state=RANDOM_STATE,
            )),
        ]),
        "GradientBoosting": Pipeline([
            ("model", GradientBoostingRegressor(
                n_estimators=200,
                learning_rate=0.05,
                max_depth=4,
                subsample=0.8,
                random_state=RANDOM_STATE,
            )),
        ]),
    }


# ---------------------------------------------------------------------------
# Treinamento e Avaliação
# ---------------------------------------------------------------------------

def walk_forward_cv(pipeline: Pipeline, X: pd.DataFrame, y: pd.Series,
                     n_splits: int = N_SPLITS) -> np.ndarray:
    """
    Walk-forward cross-validation respeitando a ordenação temporal.
    Retorna array de MAE por fold.
    """
    tscv = TimeSeriesSplit(n_splits=n_splits)
    mae_scores = []
    for train_idx, val_idx in tscv.split(X):
        X_tr, X_val = X.iloc[train_idx], X.iloc[val_idx]
        y_tr, y_val = y.iloc[train_idx], y.iloc[val_idx]
        pipeline.fit(X_tr, y_tr)
        preds = pipeline.predict(X_val)
        mae_scores.append(mean_absolute_error(y_val, preds))
    return np.array(mae_scores)


def train_and_evaluate(X: pd.DataFrame, y: pd.Series,
                        test_size: float = 0.2) -> Tuple[Dict, str, Pipeline]:
    """
    Treina todos os modelos candidatos e retorna:
      - dict de ModelMetrics
      - nome do modelo campeão (menor MAE no holdout)
      - pipeline do modelo campeão já treinado
    """
    split = int(len(X) * (1 - test_size))
    X_train, X_test = X.iloc[:split], X.iloc[split:]
    y_train, y_test = y.iloc[:split], y.iloc[split:]

    log.info("Treino: %d amostras | Teste (holdout): %d amostras", len(X_train), len(X_test))

    models = get_candidate_models()
    results: Dict[str, ModelMetrics] = {}

    for name, pipeline in models.items():
        log.info("Treinando: %s", name)
        cv_scores = walk_forward_cv(pipeline, X_train, y_train)
        pipeline.fit(X_train, y_train)
        y_pred = pipeline.predict(X_test)
        metrics = compute_metrics(y_test.values, y_pred, name, cv_scores)
        results[name] = metrics
        log.info(
            "  MAE=%.2f  RMSE=%.2f  MAPE=%.2f%%  R²=%.4f",
            metrics.mae, metrics.rmse, metrics.mape, metrics.r2,
        )

    # Seleciona campeão pelo menor MAE no holdout
    champion_name = min(results, key=lambda k: results[k].mae)
    log.info(">>> Modelo campeão: %s (MAE=%.2f)", champion_name, results[champion_name].mae)

    # Re-treina campeão no dataset completo
    champion_pipeline = models[champion_name]
    champion_pipeline.fit(X, y)

    return results, champion_name, champion_pipeline


# ---------------------------------------------------------------------------
# Feature Importance
# ---------------------------------------------------------------------------

def extract_feature_importance(pipeline: Pipeline,
                                feature_names: List[str]) -> pd.DataFrame | None:
    """Extrai feature importance se o modelo suportar (tree-based)."""
    step = pipeline.steps[-1][1]  # último step do Pipeline
    if hasattr(step, "feature_importances_"):
        fi = pd.DataFrame({
            "feature": feature_names,
            "importance": step.feature_importances_,
        }).sort_values("importance", ascending=False).reset_index(drop=True)
        return fi
    return None


# ---------------------------------------------------------------------------
# Geração de Previsões Futuras
# ---------------------------------------------------------------------------

def generate_future_forecast(pipeline: Pipeline, last_known: pd.DataFrame,
                               horizon: int = FORECAST_HORIZON) -> pd.DataFrame:
    """
    Gera previsões para os próximos `horizon` meses usando o modelo treinado.
    Utiliza estratégia recursiva: cada previsão alimenta os lags do passo seguinte.
    """
    history = last_known["total_revenue"].tolist()
    last_date = pd.to_datetime(last_known["order_month"].max())

    future_rows = []
    for step in range(1, horizon + 1):
        next_date = last_date + pd.DateOffset(months=step)
        # Calcula lags a partir do histórico acumulado
        lag_dict = {}
        for lag in [1, 2, 3, 6, 12]:
            idx = -(lag)
            lag_dict[f"lag_{lag}"] = history[idx] if len(history) >= lag else np.nan

        rolling_window = history[-3:] if len(history) >= 3 else history
        rolling_window_6 = history[-6:] if len(history) >= 6 else history

        row = {
            "order_month": next_date,
            "month": next_date.month,
            "quarter": (next_date.month - 1) // 3 + 1,
            "year": next_date.year,
            "month_sin": np.sin(2 * np.pi * next_date.month / 12),
            "month_cos": np.cos(2 * np.pi * next_date.month / 12),
            "quarter_sin": np.sin(2 * np.pi * ((next_date.month - 1) // 3 + 1) / 4),
            "quarter_cos": np.cos(2 * np.pi * ((next_date.month - 1) // 3 + 1) / 4),
            "trend": len(last_known) + step - 1,
            **lag_dict,
            "rolling_3_mean": np.mean(rolling_window),
            "rolling_6_mean": np.mean(rolling_window_6),
            "rolling_3_std": float(np.std(rolling_window, ddof=1)) if len(rolling_window) > 1 else 0.0,
            "rolling_6_std": float(np.std(rolling_window_6, ddof=1)) if len(rolling_window_6) > 1 else 0.0,
        }
        future_rows.append(row)

        # Obtém as features usadas na última etapa de fit
        feature_cols = [c for c in pipeline.feature_names_in_
                        if c in row] if hasattr(pipeline, "feature_names_in_") else [
            c for c in row if c != "order_month"]
        X_step = pd.DataFrame([row])[feature_cols]
        predicted = float(pipeline.predict(X_step)[0])
        history.append(max(predicted, 0))

    forecast_df = pd.DataFrame(future_rows)[["order_month"]]
    forecast_df["predicted_revenue"] = [max(v, 0) for v in history[-horizon:]]
    return forecast_df


# ---------------------------------------------------------------------------
# Artefatos
# ---------------------------------------------------------------------------

def save_artifacts(pipeline: Pipeline, metrics_df: pd.DataFrame,
                    forecast_df: pd.DataFrame,
                    feature_importance: pd.DataFrame | None,
                    artifacts_dir: Path = ARTIFACTS_DIR) -> None:
    """Persiste modelo, métricas e previsões em disco."""
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    joblib.dump(pipeline, artifacts_dir / "champion_model.pkl")
    metrics_df.to_csv(artifacts_dir / "model_metrics.csv", index=False)
    forecast_df.to_csv(artifacts_dir / "forecast.csv", index=False)
    if feature_importance is not None:
        feature_importance.to_csv(artifacts_dir / "feature_importance.csv", index=False)
    log.info("Artefatos salvos em: %s", artifacts_dir)


def load_champion_model(artifacts_dir: Path = ARTIFACTS_DIR) -> Pipeline:
    """Carrega o modelo campeão persistido."""
    path = artifacts_dir / "champion_model.pkl"
    if not path.exists():
        raise FileNotFoundError(f"Modelo não encontrado em {path}. Execute o pipeline primeiro.")
    return joblib.load(path)


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------

class DemandForecastPipeline:
    """
    Orquestra todas as etapas do pipeline de previsão de demanda.

    Exemplo de uso:
        pipeline = DemandForecastPipeline(source="synthetic")
        result = pipeline.run()
        print(result.metrics_df)
        print(result.forecast_df)
    """

    def __init__(
        self,
        source: str = "synthetic",          # "snowflake" | "csv" | "synthetic"
        snowflake_conn_params: dict | None = None,
        csv_path: str | Path | None = None,
        forecast_horizon: int = FORECAST_HORIZON,
        n_cv_splits: int = N_SPLITS,
        save: bool = True,
    ):
        self.source = source
        self.snowflake_conn_params = snowflake_conn_params or {}
        self.csv_path = csv_path
        self.forecast_horizon = forecast_horizon
        self.n_cv_splits = n_cv_splits
        self.save = save

    # ------------------------------------------------------------------
    def _load_data(self) -> pd.DataFrame:
        if self.source == "snowflake":
            return load_from_snowflake(self.snowflake_conn_params)
        elif self.source == "csv":
            return load_from_csv(self.csv_path)
        else:
            log.info("Usando dados sintéticos (modo desenvolvimento).")
            return generate_synthetic_data()

    # ------------------------------------------------------------------
    def run(self) -> ForecastResult:
        log.info("=" * 60)
        log.info("  AdventureWorks — Pipeline de Previsão de Demanda")
        log.info("=" * 60)

        # 1. Carga de dados
        raw_df = self._load_data()

        # 2. Engenharia de features
        log.info("Construindo features...")
        X, y = prepare_features(raw_df)
        feature_names = list(X.columns)

        # 3. Treinamento e avaliação
        log.info("Treinando e avaliando modelos...")
        results_dict, champion_name, champion_pipeline = train_and_evaluate(X, y)

        # Tenta armazenar feature_names_in_ manualmente (compatibilidade sklearn)
        try:
            champion_pipeline.feature_names_in_ = np.array(feature_names)
        except Exception:
            pass

        # 4. Tabela comparativa de métricas
        metrics_df = pd.DataFrame([m.as_dict() for m in results_dict.values()])
        metrics_df = metrics_df.sort_values("MAE").reset_index(drop=True)

        log.info("\nComparativo de Modelos:\n%s", metrics_df.to_string(index=False))

        # 5. Feature importance
        feature_importance = extract_feature_importance(champion_pipeline, feature_names)
        if feature_importance is not None:
            log.info("\nTop-10 Features:\n%s",
                     feature_importance.head(10).to_string(index=False))

        # 6. Previsão futura recursiva
        log.info("Gerando previsão para %d meses futuros...", self.forecast_horizon)
        # Reconstrói série completa com features para passar o contexto de lags
        df_with_features = build_time_features(raw_df)
        df_with_features = build_lag_features(df_with_features).dropna().reset_index(drop=True)
        forecast_df = generate_future_forecast(
            champion_pipeline, df_with_features, self.forecast_horizon
        )
        log.info("\nPrevisão:\n%s", forecast_df.to_string(index=False))

        # 7. Persistência de artefatos
        if self.save:
            save_artifacts(champion_pipeline, metrics_df, forecast_df, feature_importance)

        return ForecastResult(
            champion_model_name=champion_name,
            metrics_df=metrics_df,
            forecast_df=forecast_df,
            feature_importance=feature_importance,
        )


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    pipeline = DemandForecastPipeline(source="synthetic", save=True)
    result = pipeline.run()

    print("\n" + "=" * 60)
    print(f"  Modelo Campeão: {result.champion_model_name}")
    print("=" * 60)
    print("\nMétricas de Desempenho:")
    print(result.metrics_df.to_string(index=False))
    print("\nPrevisão de Receita (próximos meses):")
    print(result.forecast_df.to_string(index=False))
