"""Evaluation metrics for energy demand forecasting models.

Implements the four metrics from the thesis + the reference paper:
    - RMSE  (penalises large errors)
    - MAE   (robust day-to-day error)
    - MAPE  (scale-independent, reported as percentage 0–100)
    - Pearson r  (trajectory tracking)

The reference paper reports MAPE as a percentage (×100), which we match.
"""

from dataclasses import dataclass
from typing import Dict

import numpy as np
from scipy.stats import pearsonr


@dataclass
class ForecastMetrics:
    """Container for a single evaluation result."""
    rmse: float
    mae: float
    mape_pct: float     # MAPE as percentage (0–100), matching reference
    pearson_r: float
    n_samples: int

    def to_dict(self) -> Dict[str, float]:
        return {
            "rmse": round(self.rmse, 2),
            "mae": round(self.mae, 2),
            "mape_pct": round(self.mape_pct, 2),
            "pearson_r": round(self.pearson_r, 4),
            "n_samples": self.n_samples,
        }


def compute_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> ForecastMetrics:
    """Compute all evaluation metrics on original-scale (MW) values.

    Parameters
    ----------
    y_true : 1-D array of actual load values (MW, inverse-transformed)
    y_pred : 1-D array of predicted load values (MW, inverse-transformed)
    """
    y_true = np.asarray(y_true, dtype=np.float64).ravel()
    y_pred = np.asarray(y_pred, dtype=np.float64).ravel()

    # Drop NaN pairs
    mask = np.isfinite(y_true) & np.isfinite(y_pred)
    y_true, y_pred = y_true[mask], y_pred[mask]
    n = len(y_true)

    if n == 0:
        return ForecastMetrics(
            rmse=np.nan, mae=np.nan, mape_pct=np.nan,
            pearson_r=np.nan, n_samples=0,
        )

    errors = y_pred - y_true

    # RMSE
    rmse = float(np.sqrt(np.mean(errors ** 2)))

    # MAE
    mae = float(np.mean(np.abs(errors)))

    # MAPE as percentage (matching reference: * 100)
    nonzero = np.abs(y_true) > 1.0
    if nonzero.sum() > 0:
        mape_pct = float(
            np.mean(np.abs(errors[nonzero] / y_true[nonzero])) * 100
        )
    else:
        mape_pct = np.nan

    # Pearson r using scipy (matches reference: pearsonr)
    if np.std(y_true) > 0 and np.std(y_pred) > 0:
        r, _ = pearsonr(y_true, y_pred)
        r = float(r)
    else:
        r = np.nan

    return ForecastMetrics(
        rmse=rmse, mae=mae, mape_pct=mape_pct,
        pearson_r=r, n_samples=n,
    )
