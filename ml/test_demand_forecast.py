"""
AdventureWorks DW — Testes do Pipeline de Previsão de Demanda
=============================================================
Cobertura:
  - Geração de dados sintéticos
  - Engenharia de features temporais e de lag
  - Métricas (MAE, RMSE, MAPE, R²)
  - Treinamento e avaliação dos modelos candidatos
  - Walk-forward cross-validation
  - Geração de previsões futuras
  - Serialização e carregamento de artefatos
  - Pipeline end-to-end

Execução:
    pytest ml/test_demand_forecast.py -v
    pytest ml/test_demand_forecast.py -v --tb=short
"""

import shutil
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from sklearn.linear_model import LinearRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

# ── Importações do módulo em teste ──────────────────────────────────────────
import sys
sys.path.insert(0, str(Path(__file__).parent))

from demand_forecast import (
    ARTIFACTS_DIR,
    DemandForecastPipeline,
    ModelMetrics,
    build_lag_features,
    build_time_features,
    compute_metrics,
    extract_feature_importance,
    generate_future_forecast,
    generate_synthetic_data,
    get_candidate_models,
    load_champion_model,
    mape,
    prepare_features,
    save_artifacts,
    walk_forward_cv,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def synthetic_df() -> pd.DataFrame:
    """Dataset sintético reusado em vários testes."""
    return generate_synthetic_data(seed=0)


@pytest.fixture(scope="module")
def features_xy(synthetic_df):
    """X e y prontos para uso em testes de modelos."""
    return prepare_features(synthetic_df)


@pytest.fixture()
def tmp_artifacts(tmp_path) -> Path:
    """Diretório temporário para artefatos gerados nos testes."""
    return tmp_path / "artifacts"


# ---------------------------------------------------------------------------
# 1. Geração de Dados Sintéticos
# ---------------------------------------------------------------------------

class TestSyntheticData:

    def test_shape(self, synthetic_df):
        """Deve ter pelo menos 36 linhas (3 anos) e as colunas obrigatórias."""
        assert len(synthetic_df) >= 36
        required = {"order_month", "total_orders", "total_revenue",
                    "total_qty", "unique_customers", "avg_order_value"}
        assert required.issubset(synthetic_df.columns)

    def test_no_nulls(self, synthetic_df):
        assert synthetic_df.isnull().sum().sum() == 0

    def test_revenue_positive(self, synthetic_df):
        assert (synthetic_df["total_revenue"] > 0).all()

    def test_order_month_dtype(self, synthetic_df):
        assert pd.api.types.is_datetime64_any_dtype(synthetic_df["order_month"])

    def test_reproducibility(self):
        df1 = generate_synthetic_data(seed=42)
        df2 = generate_synthetic_data(seed=42)
        pd.testing.assert_frame_equal(df1, df2)

    def test_different_seeds_differ(self):
        df1 = generate_synthetic_data(seed=1)
        df2 = generate_synthetic_data(seed=2)
        assert not df1["total_revenue"].equals(df2["total_revenue"])


# ---------------------------------------------------------------------------
# 2. Engenharia de Features
# ---------------------------------------------------------------------------

class TestFeatureEngineering:

    def test_time_features_columns(self, synthetic_df):
        df = build_time_features(synthetic_df)
        expected = {"month", "quarter", "year",
                    "month_sin", "month_cos", "quarter_sin", "quarter_cos"}
        assert expected.issubset(df.columns)

    def test_month_range(self, synthetic_df):
        df = build_time_features(synthetic_df)
        assert df["month"].between(1, 12).all()

    def test_quarter_range(self, synthetic_df):
        df = build_time_features(synthetic_df)
        assert df["quarter"].between(1, 4).all()

    def test_trig_range(self, synthetic_df):
        df = build_time_features(synthetic_df)
        for col in ["month_sin", "month_cos", "quarter_sin", "quarter_cos"]:
            assert df[col].between(-1.01, 1.01).all(), f"{col} fora de [-1, 1]"

    def test_lag_features_columns(self, synthetic_df):
        df = build_lag_features(synthetic_df)
        for lag in [1, 2, 3, 6]:
            assert f"lag_{lag}" in df.columns

    def test_lag_shift_correctness(self, synthetic_df):
        """lag_1 da linha i deve ser igual a total_revenue da linha i-1."""
        df = build_lag_features(synthetic_df.copy()).reset_index(drop=True)
        idx = 5  # linha suficientemente longe do início
        expected = df.loc[idx - 1, "total_revenue"]
        assert df.loc[idx, "lag_1"] == pytest.approx(expected)

    def test_rolling_columns_present(self, synthetic_df):
        df = build_lag_features(synthetic_df)
        assert "rolling_3_mean" in df.columns
        assert "rolling_6_mean" in df.columns

    def test_prepare_features_no_nan(self, features_xy):
        X, y = features_xy
        assert X.isnull().sum().sum() == 0
        assert y.isnull().sum() == 0

    def test_prepare_features_aligned(self, features_xy):
        X, y = features_xy
        assert len(X) == len(y)

    def test_prepare_features_min_rows(self, features_xy):
        X, _ = features_xy
        # Deve reter pelo menos 20 amostras após remoção dos NaN de lag
        assert len(X) >= 20


# ---------------------------------------------------------------------------
# 3. Métricas
# ---------------------------------------------------------------------------

class TestMetrics:

    def test_mape_perfect(self):
        y = np.array([100.0, 200.0, 300.0])
        assert mape(y, y) == pytest.approx(0.0)

    def test_mape_known_value(self):
        y_true = np.array([100.0, 200.0])
        y_pred = np.array([110.0, 180.0])
        # 10%  +  10%  / 2 = 10%
        assert mape(y_true, y_pred) == pytest.approx(10.0)

    def test_mape_ignores_zero(self):
        y_true = np.array([0.0, 100.0])
        y_pred = np.array([50.0, 110.0])
        # Apenas a segunda entrada é usada → 10%
        assert mape(y_true, y_pred) == pytest.approx(10.0)

    def test_mape_all_zeros_returns_nan(self):
        y = np.zeros(5)
        assert np.isnan(mape(y, y + 1))

    def test_compute_metrics_fields(self, features_xy):
        X, y = features_xy
        pipeline = Pipeline([("scaler", StandardScaler()),
                              ("model", LinearRegression())])
        pipeline.fit(X, y)
        preds = pipeline.predict(X)
        cv_scores = np.array([1000.0, 1100.0, 950.0])
        m = compute_metrics(y.values, preds, "LR", cv_scores)
        assert isinstance(m, ModelMetrics)
        assert m.mae >= 0
        assert m.rmse >= 0
        assert m.cv_mae_mean == pytest.approx(cv_scores.mean())
        assert m.cv_mae_std == pytest.approx(cv_scores.std())

    def test_model_metrics_as_dict_keys(self, features_xy):
        X, y = features_xy
        pipeline = Pipeline([("model", LinearRegression())])
        pipeline.fit(X, y)
        preds = pipeline.predict(X)
        m = compute_metrics(y.values, preds, "test", np.array([1.0]))
        d = m.as_dict()
        assert set(d.keys()) == {"model", "MAE", "RMSE", "MAPE (%)", "R²",
                                  "CV MAE médio", "CV MAE desvio"}


# ---------------------------------------------------------------------------
# 4. Modelos Candidatos
# ---------------------------------------------------------------------------

class TestCandidateModels:

    def test_candidate_models_keys(self):
        models = get_candidate_models()
        assert "LinearRegression (baseline)" in models
        assert "RandomForest" in models
        assert "GradientBoosting" in models

    def test_candidate_models_are_pipelines(self):
        for name, model in get_candidate_models().items():
            assert isinstance(model, Pipeline), f"{name} não é um Pipeline"

    def test_all_models_fit_predict(self, features_xy):
        X, y = features_xy
        for name, pipeline in get_candidate_models().items():
            pipeline.fit(X, y)
            preds = pipeline.predict(X)
            assert len(preds) == len(y), f"{name}: tamanho errado nas predições"
            assert not np.isnan(preds).any(), f"{name}: predições contêm NaN"


# ---------------------------------------------------------------------------
# 5. Walk-forward Cross-Validation
# ---------------------------------------------------------------------------

class TestWalkForwardCV:

    def test_returns_correct_number_of_scores(self, features_xy):
        X, y = features_xy
        pipeline = Pipeline([("model", LinearRegression())])
        scores = walk_forward_cv(pipeline, X, y, n_splits=3)
        assert len(scores) == 3

    def test_all_scores_positive(self, features_xy):
        X, y = features_xy
        pipeline = Pipeline([("model", LinearRegression())])
        scores = walk_forward_cv(pipeline, X, y)
        assert (scores >= 0).all()

    def test_cv_with_gradient_boosting(self, features_xy):
        from sklearn.ensemble import GradientBoostingRegressor
        X, y = features_xy
        pipeline = Pipeline([("model", GradientBoostingRegressor(
            n_estimators=10, random_state=0))])
        scores = walk_forward_cv(pipeline, X, y, n_splits=3)
        assert len(scores) == 3


# ---------------------------------------------------------------------------
# 6. Feature Importance
# ---------------------------------------------------------------------------

class TestFeatureImportance:

    def test_tree_model_returns_dataframe(self, features_xy):
        from sklearn.ensemble import RandomForestRegressor
        X, y = features_xy
        pipeline = Pipeline([("model", RandomForestRegressor(n_estimators=10,
                                                              random_state=0))])
        pipeline.fit(X, y)
        fi = extract_feature_importance(pipeline, list(X.columns))
        assert isinstance(fi, pd.DataFrame)
        assert "feature" in fi.columns
        assert "importance" in fi.columns
        assert len(fi) == len(X.columns)

    def test_linear_model_returns_none(self, features_xy):
        X, y = features_xy
        pipeline = Pipeline([("model", LinearRegression())])
        pipeline.fit(X, y)
        fi = extract_feature_importance(pipeline, list(X.columns))
        assert fi is None

    def test_importance_sums_to_one(self, features_xy):
        from sklearn.ensemble import RandomForestRegressor
        X, y = features_xy
        pipeline = Pipeline([("model", RandomForestRegressor(n_estimators=10,
                                                              random_state=0))])
        pipeline.fit(X, y)
        fi = extract_feature_importance(pipeline, list(X.columns))
        assert fi["importance"].sum() == pytest.approx(1.0, abs=1e-5)

    def test_sorted_descending(self, features_xy):
        from sklearn.ensemble import RandomForestRegressor
        X, y = features_xy
        pipeline = Pipeline([("model", RandomForestRegressor(n_estimators=10,
                                                              random_state=0))])
        pipeline.fit(X, y)
        fi = extract_feature_importance(pipeline, list(X.columns))
        assert fi["importance"].is_monotonic_decreasing


# ---------------------------------------------------------------------------
# 7. Previsão Futura
# ---------------------------------------------------------------------------

class TestFutureForecast:

    def _trained_pipeline(self, X, y):
        pipeline = Pipeline([("model", LinearRegression())])
        pipeline.feature_names_in_ = np.array(list(X.columns))
        pipeline.fit(X, y)
        return pipeline

    def test_forecast_length(self, synthetic_df, features_xy):
        X, y = features_xy
        pipeline = self._trained_pipeline(X, y)
        df_ctx = build_lag_features(build_time_features(synthetic_df)).dropna()
        forecast = generate_future_forecast(pipeline, df_ctx, horizon=6)
        assert len(forecast) == 6

    def test_forecast_columns(self, synthetic_df, features_xy):
        X, y = features_xy
        pipeline = self._trained_pipeline(X, y)
        df_ctx = build_lag_features(build_time_features(synthetic_df)).dropna()
        forecast = generate_future_forecast(pipeline, df_ctx, horizon=3)
        assert "order_month" in forecast.columns
        assert "predicted_revenue" in forecast.columns

    def test_forecast_non_negative(self, synthetic_df, features_xy):
        X, y = features_xy
        pipeline = self._trained_pipeline(X, y)
        df_ctx = build_lag_features(build_time_features(synthetic_df)).dropna()
        forecast = generate_future_forecast(pipeline, df_ctx, horizon=6)
        assert (forecast["predicted_revenue"] >= 0).all()

    def test_forecast_dates_sequential(self, synthetic_df, features_xy):
        X, y = features_xy
        pipeline = self._trained_pipeline(X, y)
        df_ctx = build_lag_features(build_time_features(synthetic_df)).dropna()
        forecast = generate_future_forecast(pipeline, df_ctx, horizon=4)
        dates = pd.to_datetime(forecast["order_month"])
        diffs = dates.diff().dropna()
        assert (diffs > pd.Timedelta(0)).all()


# ---------------------------------------------------------------------------
# 8. Artefatos (I/O)
# ---------------------------------------------------------------------------

class TestArtifacts:

    def _make_pipeline(self, features_xy):
        X, y = features_xy
        p = Pipeline([("model", LinearRegression())])
        p.fit(X, y)
        return p

    def test_save_creates_files(self, features_xy, tmp_artifacts):
        pipeline = self._make_pipeline(features_xy)
        dummy_metrics = pd.DataFrame([{"model": "LR", "MAE": 1.0}])
        dummy_forecast = pd.DataFrame([{"order_month": "2024-01-01",
                                         "predicted_revenue": 100.0}])
        dummy_fi = pd.DataFrame([{"feature": "lag_1", "importance": 0.5}])
        save_artifacts(pipeline, dummy_metrics, dummy_forecast, dummy_fi, tmp_artifacts)
        assert (tmp_artifacts / "champion_model.pkl").exists()
        assert (tmp_artifacts / "model_metrics.csv").exists()
        assert (tmp_artifacts / "forecast.csv").exists()
        assert (tmp_artifacts / "feature_importance.csv").exists()

    def test_save_without_fi(self, features_xy, tmp_artifacts):
        pipeline = self._make_pipeline(features_xy)
        dummy_metrics = pd.DataFrame([{"model": "LR", "MAE": 1.0}])
        dummy_forecast = pd.DataFrame([{"order_month": "2024-01-01",
                                         "predicted_revenue": 100.0}])
        save_artifacts(pipeline, dummy_metrics, dummy_forecast, None, tmp_artifacts)
        assert not (tmp_artifacts / "feature_importance.csv").exists()

    def test_load_champion_model(self, features_xy, tmp_artifacts):
        pipeline = self._make_pipeline(features_xy)
        dummy_metrics = pd.DataFrame([{"model": "LR"}])
        dummy_forecast = pd.DataFrame([{"order_month": "2024-01-01",
                                         "predicted_revenue": 0.0}])
        save_artifacts(pipeline, dummy_metrics, dummy_forecast, None, tmp_artifacts)
        loaded = load_champion_model(tmp_artifacts)
        assert isinstance(loaded, Pipeline)

    def test_load_missing_model_raises(self, tmp_artifacts):
        with pytest.raises(FileNotFoundError):
            load_champion_model(tmp_artifacts / "nonexistent")


# ---------------------------------------------------------------------------
# 9. Pipeline End-to-End
# ---------------------------------------------------------------------------

class TestEndToEndPipeline:

    def test_run_returns_forecast_result(self):
        pipeline = DemandForecastPipeline(source="synthetic", save=False)
        result = pipeline.run()

        # Estrutura do resultado
        assert result.champion_model_name
        assert isinstance(result.metrics_df, pd.DataFrame)
        assert isinstance(result.forecast_df, pd.DataFrame)

    def test_metrics_df_has_expected_columns(self):
        pipeline = DemandForecastPipeline(source="synthetic", save=False)
        result = pipeline.run()
        expected_cols = {"model", "MAE", "RMSE", "MAPE (%)", "R²"}
        assert expected_cols.issubset(result.metrics_df.columns)

    def test_metrics_mae_non_negative(self):
        pipeline = DemandForecastPipeline(source="synthetic", save=False)
        result = pipeline.run()
        assert (result.metrics_df["MAE"] >= 0).all()

    def test_forecast_length(self):
        horizon = 4
        pipeline = DemandForecastPipeline(source="synthetic",
                                           forecast_horizon=horizon, save=False)
        result = pipeline.run()
        assert len(result.forecast_df) == horizon

    def test_all_models_evaluated(self):
        pipeline = DemandForecastPipeline(source="synthetic", save=False)
        result = pipeline.run()
        n_models = len(get_candidate_models())
        assert len(result.metrics_df) == n_models

    def test_champion_is_in_metrics(self):
        pipeline = DemandForecastPipeline(source="synthetic", save=False)
        result = pipeline.run()
        assert result.champion_model_name in result.metrics_df["model"].values

    def test_save_writes_artifacts(self, tmp_path):
        import demand_forecast as df_module
        original = df_module.ARTIFACTS_DIR
        df_module.ARTIFACTS_DIR = tmp_path / "artifacts"
        try:
            pipeline = DemandForecastPipeline(source="synthetic", save=True)
            result = pipeline.run()
            assert (result.artifacts_dir / "champion_model.pkl").exists() or \
                   (tmp_path / "artifacts" / "champion_model.pkl").exists()
        finally:
            df_module.ARTIFACTS_DIR = original

    def test_r2_reasonable_on_synthetic(self):
        """Para dados sintéticos com padrão claro, R² do campeão deve ser > 0."""
        pipeline = DemandForecastPipeline(source="synthetic", save=False)
        result = pipeline.run()
        champion_row = result.metrics_df[
            result.metrics_df["model"] == result.champion_model_name
        ].iloc[0]
        assert champion_row["R²"] > 0.0
