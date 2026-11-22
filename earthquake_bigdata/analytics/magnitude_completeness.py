"""Magnitude-of-completeness (Mc) and Gutenberg–Richter b-value estimation.

Methods implemented
-------------------
* Mc: maximum-curvature method on the non-cumulative frequency-magnitude
  distribution, with the conventional +0.2 magnitude correction
  (Woessner & Wiemer, 2005).
* b: Aki–Utsu maximum-likelihood estimator with the Utsu binning correction;
  uncertainty from the Shi & Bolt (1982) approximation.
* Validation: binned Gutenberg–Richter fit (R^2) and a Kolmogorov–Smirnov
  distance/p-value for the observed vs fitted magnitude CDF above Mc.
* Uncertainty: deterministic bootstrap confidence intervals for Mc and b.

These are estimates, not exact values.  Results include sample size, scale
composition and fit diagnostics so that weak cells can be excluded or flagged.
"""
from __future__ import annotations

import math
from typing import Any, Dict, Optional

import numpy as np

LOG10E = math.log10(math.e)


def _clean(mags) -> np.ndarray:
    a = np.asarray(mags, dtype=float)
    return a[np.isfinite(a)]


def estimate_mc_maxc(mags, bin_width: float = 0.1, correction: float = 0.2) -> Optional[float]:
    """Maximum-curvature Mc with +correction. Returns None for empty input."""
    a = _clean(mags)
    if a.size == 0:
        return None
    if a.size == 1 or np.allclose(a, a[0]):
        return float(a[0] + correction)
    mmin = math.floor(a.min() / bin_width) * bin_width
    mmax = math.ceil(a.max() / bin_width) * bin_width
    edges = np.arange(mmin, mmax + bin_width * 1.5, bin_width)
    counts, _ = np.histogram(a, bins=edges)
    if counts.sum() == 0:
        return None
    idx = int(np.argmax(counts))
    mode_mid = float(edges[idx] + bin_width / 2.0)
    mc = mode_mid + correction
    return float(min(mc, a.max()))


def b_value_aki_utsu(mags, mc: float, bin_width: float = 0.1) -> Optional[float]:
    """Aki–Utsu MLE b-value with Utsu's binning correction."""
    a = _clean(mags)
    a = a[a >= mc]
    if a.size < 2:
        return None
    denom = float(a.mean() - (mc - bin_width / 2.0))
    if denom <= 1e-9:
        return None
    return float(LOG10E / denom)


def b_value_uncertainty(mags, mc: float, b: Optional[float] = None, bin_width: float = 0.1) -> Optional[float]:
    """Shi & Bolt (1982) approximate standard error of the Aki–Utsu b-value."""
    a = _clean(mags)
    a = a[a >= mc]
    n = a.size
    if n < 3:
        return None
    b = b if b is not None else b_value_aki_utsu(a, mc, bin_width)
    if b is None:
        return None
    mean = float(a.mean())
    ss = float(np.sum((a - mean) ** 2))
    if ss <= 0:
        return None
    return float(2.3 * (b ** 2) * math.sqrt(ss / (n * (n - 1))))


def gr_fit(mags, mc: float, bin_width: float = 0.1) -> Dict[str, Any]:
    """Fit log10(N)=a-bM above Mc and return fit diagnostics.

    N is the cumulative number of events with magnitude >= each bin centre.
    The KS statistic compares the observed CDF with the GR exponential CDF.
    """
    a = _clean(mags)
    a = a[a >= mc]
    n = a.size
    if n < 5:
        return {"a": None, "b_gr": None, "r2": None, "ks_stat": None, "ks_pvalue": None, "n": int(n)}
    edges = np.arange(mc, a.max() + bin_width * 1.5, bin_width)
    centers = edges[:-1] + bin_width / 2.0
    cumulative = np.array([np.sum(a >= m) for m in centers], dtype=float)
    valid = cumulative > 0
    if valid.sum() < 2:
        return {"a": None, "b_gr": None, "r2": None, "ks_stat": None, "ks_pvalue": None, "n": int(n)}
    x = centers[valid]
    y = np.log10(cumulative[valid])
    slope, intercept = np.polyfit(x, y, 1)
    pred = slope * x + intercept
    ss_res = float(np.sum((y - pred) ** 2))
    ss_tot = float(np.sum((y - y.mean()) ** 2))
    r2 = None if ss_tot <= 0 else float(1.0 - ss_res / ss_tot)
    b_gr = float(-slope)
    a_gr = float(intercept)
    # Discrete/continuous approximation; documented as approximate.
    u = np.sort(a)
    theo = 1.0 - np.power(10.0, -max(b_gr, 1e-9) * (u - mc))
    obs = np.arange(1, n + 1) / n
    d = float(np.max(np.abs(obs - theo)))
    # Asymptotic two-sided KS p-value.
    lam = (math.sqrt(n) + 0.12 + 0.11 / math.sqrt(n)) * d
    p = 2.0 * sum((-1) ** (k - 1) * math.exp(-2.0 * k * k * lam * lam) for k in range(1, 101))
    p = float(min(1.0, max(0.0, p)))
    return {
        "a": a_gr,
        "b_gr": b_gr,
        "r2": r2,
        "ks_stat": d,
        "ks_pvalue": p,
        "n": int(n),
    }


def bootstrap_intervals(
    mags,
    mc: float,
    b: Optional[float],
    bin_width: float,
    iterations: int,
    seed: int,
) -> Dict[str, Any]:
    """Percentile bootstrap CIs for Mc and b (deterministic per group seed)."""
    a = _clean(mags)
    if a.size < 10:
        return {"mc_ci_low": None, "mc_ci_high": None, "b_ci_low": None, "b_ci_high": None,
                "bootstrap_iterations": 0}
    rng = np.random.default_rng(seed)
    mcs, bs = [], []
    for _ in range(max(0, int(iterations))):
        sample = rng.choice(a, size=a.size, replace=True)
        m = estimate_mc_maxc(sample, bin_width)
        if m is None:
            continue
        mcs.append(m)
        bb = b_value_aki_utsu(sample, m, bin_width)
        if bb is not None:
            bs.append(bb)
    def q(v, lo=2.5, hi=97.5):
        return (float(np.percentile(v, lo)), float(np.percentile(v, hi))) if v else (None, None)
    mc_lo, mc_hi = q(mcs)
    b_lo, b_hi = q(bs)
    return {
        "mc_ci_low": mc_lo, "mc_ci_high": mc_hi,
        "b_ci_low": b_lo, "b_ci_high": b_hi,
        "bootstrap_iterations": int(iterations),
    }


def analyze_magnitudes(
    mags,
    min_events: int = 50,
    bin_width: float = 0.1,
    correction: float = 0.2,
    min_b_events: int = 50,
    bootstrap_iterations: int = 200,
    seed: int = 0,
) -> Dict[str, Any]:
    """Full Mc/b-value analysis for one region/window magnitude sample."""
    a = _clean(mags)
    n = int(a.size)
    out: Dict[str, Any] = {
        "n": n,
        "sufficient_data": n >= min_events,
        "mc": None, "mc_method": "maximum_curvature",
        "b_value": None, "b_uncertainty": None,
        "n_above_mc": 0,
        "a_value": None, "b_gr": None, "gr_r2": None,
        "ks_stat": None, "ks_pvalue": None,
        "median_mag": float(np.median(a)) if n else None,
        "max_mag": float(a.max()) if n else None,
        "min_mag": float(a.min()) if n else None,
    }
    if n == 0:
        return out
    mc = estimate_mc_maxc(a, bin_width, correction)
    out["mc"] = mc
    if mc is None:
        return out
    above = a[a >= mc]
    out["n_above_mc"] = int(above.size)
    b = b_value_aki_utsu(a, mc, bin_width)
    out["b_value"] = b
    out["b_uncertainty"] = b_value_uncertainty(a, mc, b, bin_width)
    fit = gr_fit(a, mc, bin_width)
    out["a_value"] = fit.get("a")
    out["b_gr"] = fit.get("b_gr")
    out["gr_r2"] = fit.get("r2")
    out["ks_stat"] = fit.get("ks_stat")
    out["ks_pvalue"] = fit.get("ks_pvalue")
    if above.size >= min_b_events and bootstrap_iterations > 0:
        out.update(bootstrap_intervals(a, mc, b, bin_width, bootstrap_iterations, seed))
    return out
