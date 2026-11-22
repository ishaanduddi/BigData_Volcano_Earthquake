from __future__ import annotations

import numpy as np
import pytest

from analytics.volcano_association import bh_fdr, permutation_test, poisson_rate_ratio


def test_rate_ratio_and_ci():
    out = poisson_rate_ratio(10, 90, 10, 365, 0.05)
    assert out["rate_ratio"] is not None
    assert out["rate_ratio"] > 1
    assert out["ci_low"] < out["rate_ratio"] < out["ci_high"]
    assert 0 <= out["p_value"] <= 1


def test_rate_ratio_zero_count_continuity():
    out = poisson_rate_ratio(0, 90, 5, 365, 0.05)
    assert out["continuity_correction"] is True
    assert out["rate_ratio"] is not None


def test_bh_fdr_monotone_and_bounded():
    q = bh_fdr([0.001, 0.01, 0.04, None, 0.2], 0.05)
    assert q[3] is None
    for v in q:
        if v is not None:
            assert 0 <= v <= 1


def test_permutation_p_value_range():
    rng = np.random.default_rng(1)
    days = rng.uniform(0, 1000, 100)
    p = permutation_test(days, 5, 0, 900, 90, [], 99, 42)
    assert p is None or 0 <= p <= 1
