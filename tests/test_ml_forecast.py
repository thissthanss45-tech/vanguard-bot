"""Tests for ml_forecast module."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import ml_forecast


def _build_ohlcv(rows: int = 300, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    idx = pd.date_range("2024-01-01", periods=rows, freq="D", tz="UTC")
    close = 100.0 + np.cumsum(rng.normal(0, 1, rows))
    close = np.maximum(close, 1.0)
    df = pd.DataFrame(
        {
            "Open": close * (1 - rng.uniform(0, 0.005, rows)),
            "High": close * (1 + rng.uniform(0, 0.01, rows)),
            "Low": close * (1 - rng.uniform(0, 0.01, rows)),
            "Close": close,
            "Volume": rng.integers(500_000, 2_000_000, rows).astype(float),
        },
        index=idx,
    )
    return df


class TestBuildFeatures:
    def test_returns_dataframe_with_expected_columns(self):
        hist = _build_ohlcv(300)
        feat = ml_forecast.build_features(hist)
        assert isinstance(feat, pd.DataFrame)
        assert len(feat) == len(hist)
        # Key feature groups present
        assert "rsi_14" in feat.columns
        assert "macd_hist_norm" in feat.columns
        assert "bb_position" in feat.columns
        assert "adx_norm" in feat.columns
        assert "stoch_k" in feat.columns
        assert "cci_norm" in feat.columns
        assert "volume_ratio" in feat.columns

    def test_no_look_ahead_bias_in_last_row(self):
        """Last row features must not depend on future data."""
        hist = _build_ohlcv(300)
        feat_full = ml_forecast.build_features(hist)
        feat_cut = ml_forecast.build_features(hist.iloc[:-1])
        # Second-to-last row should be identical in both (no forward leakage)
        row_full = feat_full.iloc[-2].dropna()
        row_cut = feat_cut.iloc[-1].dropna()
        common = row_full.index.intersection(row_cut.index)
        pd.testing.assert_series_equal(
            row_full[common].reset_index(drop=True),
            row_cut[common].reset_index(drop=True),
            check_names=False,
        )


class TestTrainAndPredict:
    def test_returns_expected_keys(self):
        hist = _build_ohlcv(300)
        result = ml_forecast.train_and_predict(hist)
        assert "ml_bull_prob" in result
        assert "ml_bear_prob" in result
        assert "ml_accuracy_wf" in result
        assert "ml_available" in result
        assert "ml_confidence" in result

    def test_probabilities_sum_to_100(self):
        hist = _build_ohlcv(300)
        result = ml_forecast.train_and_predict(hist)
        if result["ml_available"]:
            # 3-class model: bull + bear + neutral ≈ 100 (±1 rounding)
            total = result["ml_bull_prob"] + result["ml_bear_prob"] + result["ml_neutral_prob"]
            assert 99 <= total <= 101

    def test_bull_prob_in_valid_range(self):
        hist = _build_ohlcv(300)
        result = ml_forecast.train_and_predict(hist)
        if result["ml_available"]:
            assert 0 <= result["ml_bull_prob"] <= 100
            assert 0 <= result["ml_bear_prob"] <= 100

    def test_walk_forward_accuracy_reasonable(self):
        hist = _build_ohlcv(400)
        result = ml_forecast.train_and_predict(hist)
        if result["ml_available"]:
            # Accuracy should be between 30-90% (not degenerate)
            assert 30.0 <= result["ml_accuracy_wf"] <= 90.0

    def test_fallback_on_insufficient_data(self):
        hist = _build_ohlcv(50)  # Too few rows
        result = ml_forecast.train_and_predict(hist)
        assert result["ml_available"] is False
        assert result["ml_bull_prob"] == 50
        assert result["ml_bear_prob"] == 50

    def test_fallback_result_structure(self):
        result = ml_forecast._fallback_result()
        assert result["ml_available"] is False
        assert result["ml_bull_prob"] == 50


class TestEnsembleProbability:
    def test_no_ml_returns_rule_prob(self):
        rule_bull = 65.0
        ml_result = {"ml_available": False}
        assert ml_forecast.ensemble_probability(rule_bull, ml_result) == rule_bull

    def test_low_accuracy_ml_returns_rule_prob(self):
        rule_bull = 60.0
        # Threshold is ≤40%: below that, pure rule (random baseline for 3-class = 33%)
        ml_result = {"ml_available": True, "ml_accuracy_wf": 38.0, "ml_bull_prob": 80}
        result = ml_forecast.ensemble_probability(rule_bull, ml_result)
        assert result == rule_bull

    def test_high_accuracy_ml_blends_toward_ml(self):
        rule_bull = 50.0
        ml_result = {"ml_available": True, "ml_accuracy_wf": 65.0, "ml_bull_prob": 75}
        result = ml_forecast.ensemble_probability(rule_bull, ml_result)
        # Should be between rule and ml
        assert 50.0 < result < 75.0

    def test_ensemble_result_is_float(self):
        ml_result = {"ml_available": True, "ml_accuracy_wf": 60.0, "ml_bull_prob": 70}
        result = ml_forecast.ensemble_probability(55.0, ml_result)
        assert isinstance(result, float)
