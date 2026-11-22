from __future__ import annotations

import numpy as np
import pytest

from analytics.magnitude_completeness import (
    analyze_magnitudes, b_value_aki_utsu, b_value_uncertainty,
    estimate_mc_maxc, gr_fit,
)


def _synthetic_gr(n=5000, b=1.0, mc=3.0, seed=7):
    """Synthetic Gutenberg–Richter magnitudes; test-only, clearly synthetic."""
    rng = np.random.default_rng(seed)
    # GR exponential exceedance: M = Mc + (-log10(U))/b
    return mc + (-np.log10(rng.random(n))) / b


def test_mc_reasonable_for_synthetic_gr():
    mags = _synthetic_gr()
    mc = estimate_mc_maxc(mags, 0.1, 0.2)
    assert mc is not None
    assert 2.5 <= mc <= 3.8


def test_b_value_reasonable_for_synthetic_gr():
    mags = _synthetic_gr(n=8000, b=1.0, mc=3.0, seed=11)
    mc = estimate_mc_maxc(mags)
    b = b_value_aki_utsu(mags, mc)
    assert b is not None
    assert 0.75 <= b <= 1.25
    se = b_value_uncertainty(mags, mc, b)
    assert se is not None and se > 0


def test_gr_fit_and_gof_fields():
    mags = _synthetic_gr()
    mc = estimate_mc_maxc(mags)
    fit = gr_fit(mags, mc)
    assert fit["n"] > 0
    assert fit["b_gr"] is not None
    assert 0.0 <= fit["ks_pvalue"] <= 1.0
    assert fit["r2"] is not None


def test_analyze_magnitudes_flags_insufficient():
    out = analyze_magnitudes([2.5, 2.6, 2.7], min_events=50, bootstrap_iterations=0)
    assert out["n"] == 3
    assert out["sufficient_data"] is False


def test_bootstrap_intervals_present():
    mags = _synthetic_gr(n=2000, b=1.0, mc=3.0, seed=3)
    out = analyze_magnitudes(mags, min_events=100, min_b_events=50, bootstrap_iterations=30)
    assert out["mc"] is not None
    assert out["mc_ci_low"] is not None
    assert out["b_ci_low"] is not None
    assert out["mc_ci_low"] <= out["mc_ci_high"]
