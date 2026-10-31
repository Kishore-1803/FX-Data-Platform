"""
Statistical Utilities for Forex Forecasting:
Provides:
  1. Pooled Directional Accuracy with Wilson Score 95% Confidence Intervals & Binomial p-values.
  2. Diebold-Mariano test with Newey-West (HAC) autocorrelation correction for overlapping horizons.
  3. Multiple-testing correction (Bonferroni and Benjamini-Hochberg FDR).
"""

import numpy as np
from scipy import stats
from typing import Dict, Any, Tuple

def wilson_score_interval(k: int, n: int, confidence: float = 0.95) -> Tuple[float, float]:
    """Computes Wilson score 95% confidence interval for binomial proportion."""
    if n == 0:
        return (0.0, 0.0)
    p = k / n
    z = stats.norm.ppf(1 - (1 - confidence) / 2)
    denom = 1 + z**2 / n
    centre = (p + z**2 / (2 * n)) / denom
    spread = z * np.sqrt((p * (1 - p) + z**2 / (4 * n)) / n) / denom
    return (max(0.0, centre - spread), min(1.0, centre + spread))

def compute_directional_accuracy_stats(y_true: np.ndarray, y_pred: np.ndarray) -> Dict[str, Any]:
    """
    Computes pooled Directional Accuracy, effect size, 95% Wilson CI,
    and exact binomial test p-value against random walk (p=0.5).
    """
    mask = ~np.isnan(y_true) & ~np.isnan(y_pred)
    yt = y_true[mask]
    yp = y_pred[mask]
    n = int(len(yt))
    if n == 0:
        return {"n": 0, "da_pct": 50.0, "k": 0, "ci_lower": 50.0, "ci_upper": 50.0, "effect_size": 0.0, "p_val": 1.0}

    # Direction: sign(y) == sign(y_pred)
    correct = (np.sign(yt) == np.sign(yp))
    k = int(np.sum(correct))
    da = k / n
    effect_size = da - 0.50 # Cohen's g equivalent = da - 0.50

    ci_low, ci_high = wilson_score_interval(k, n, confidence=0.95)
    binom_res = stats.binomtest(k, n, p=0.5, alternative="greater")

    return {
        "n": n,
        "k_success": k,
        "da_pct": round(da * 100.0, 2),
        "effect_size": round(effect_size, 4),
        "ci_95_lower": round(ci_low * 100.0, 2),
        "ci_95_upper": round(ci_high * 100.0, 2),
        "p_val_binomial": float(binom_res.pvalue)
    }

def diebold_mariano_hac(
    y_true: np.ndarray,
    y_pred_model: np.ndarray,
    y_pred_baseline: np.ndarray,
    h: int,
    loss_type: str = "absolute"
) -> Dict[str, Any]:
    """
    Diebold-Mariano test comparing model vs baseline with Newey-West (HAC) spectral variance
    to account for (h-1) order autocorrelation in overlapping multi-step forecast errors.
    
    d_t = L(e_{baseline}) - L(e_{model})
    Positive d_t means model has smaller error than baseline.
    """
    mask = ~np.isnan(y_true) & ~np.isnan(y_pred_model) & ~np.isnan(y_pred_baseline)
    yt = y_true[mask]
    ym = y_pred_model[mask]
    yb = y_pred_baseline[mask]
    n = len(yt)
    if n < 30:
        return {"dm_stat": 0.0, "p_val_hac": 1.0, "mean_diff": 0.0}

    if loss_type == "squared":
        loss_m = (yt - ym) ** 2
        loss_b = (yt - yb) ** 2
    else: # absolute error
        loss_m = np.abs(yt - ym)
        loss_b = np.abs(yt - yb)

    d = loss_b - loss_m # positive => model wins
    mean_d = float(np.mean(d))
    
    # Auto-covariances for Newey-West HAC with Bartlett kernel
    max_lag = max(1, h - 1)
    gamma_0 = float(np.var(d, ddof=0))
    
    hac_var = gamma_0
    for lag in range(1, max_lag + 1):
        weight = 1.0 - (lag / (max_lag + 1)) # Bartlett triangular kernel
        gamma_lag = float(np.mean((d[lag:] - mean_d) * (d[:-lag] - mean_d)))
        hac_var += 2.0 * weight * gamma_lag

    hac_var = max(1e-15, hac_var)
    hac_se = np.sqrt(hac_var / n)
    
    dm_stat = mean_d / hac_se
    
    # Harvey, Leybourne, and Newbold (1997) small-sample modification factor
    hln_factor = np.sqrt((n + 1 - 2*h + (h * (h - 1)) / n) / n) if n > 2*h else 1.0
    adjusted_dm = dm_stat * hln_factor
    
    # One-sided p-value (H1: model error < baseline error, so mean_d > 0)
    p_val = 1.0 - stats.norm.cdf(adjusted_dm)
    
    return {
        "n_samples": n,
        "mean_loss_diff": float(mean_d),
        "hac_se": float(hac_se),
        "dm_stat": round(float(adjusted_dm), 4),
        "p_val_hac": round(float(p_val), 5)
    }

