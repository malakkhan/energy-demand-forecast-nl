#!/usr/bin/env python3
"""Unified training script for all six baseline forecasting models.

Runs the full Rolling-Origin Cross-Validation loop for any combination
of models, horizons, and folds.

Usage
-----
    # All models, all horizons, all folds:
    python -u src/models/baselines/train_baselines.py

    # Specific model:
    python -u src/models/baselines/train_baselines.py --model van_de_sande_mlr

    # Specific model + horizon:
    python -u src/models/baselines/train_baselines.py --model ashtar_sarimax --horizon 60

    # Quick smoke test (1 fold, 1 horizon):
    python -u src/models/baselines/train_baselines.py --model van_de_sande_mlr --quick
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

# Ensure repo root on sys.path
_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from src.models.baselines import config as C
from src.models.baselines.data_loader import (
    load_dataset,
    prepare_shifted_dataset,
    rocv_folds,
)
from src.models.baselines.evaluation import (
    compute_metrics,
    plot_metric_evolution,
    print_summary_table,
    save_final_results,
    save_fold_result,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    format="%(asctime)s [%(name)s] %(levelname)s — %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("train_baselines")


# ---------------------------------------------------------------------------
# Model registry — lazy imports to avoid loading all frameworks upfront
# ---------------------------------------------------------------------------

def _get_model_runner(model_name: str):
    """Return the appropriate model runner class for a given model name."""
    if model_name == "van_de_sande_mlr":
        from src.models.baselines.mlr import MLRBaseline
        return MLRBaseline(C.MLRConfig())

    elif model_name == "van_de_sande_prophet_lstm":
        from src.models.baselines.prophet_lstm_pytorch import ProphetLSTMPyTorch
        return ProphetLSTMPyTorch(C.ProphetConfig(), C.LSTMConfig())

    elif model_name == "ashtar_sarimax":
        from src.models.baselines.sarimax import SARIMAXBaseline
        return SARIMAXBaseline(C.SARIMAXConfig())

    elif model_name == "ashtar_sarimax_lstm":
        from src.models.baselines.sarimax_lstm import SARIMAXLSTMBaseline
        return SARIMAXLSTMBaseline(C.SARIMAXConfig(), C.SARIMAXLSTMConfig())

    elif model_name == "ashtar_seq2seq":
        from src.models.baselines.seq2seq import Seq2SeqBaseline
        return Seq2SeqBaseline(C.Seq2SeqConfig())

    elif model_name == "curiel_stacked_prophet_qlstm":
        from src.models.baselines.qlstm import ProphetQLSTMXGBBaseline
        return ProphetQLSTMXGBBaseline(
            C.ProphetMultConfig(), C.QLSTMConfig(), C.XGBoostStackerConfig(),
        )

    else:
        raise ValueError(f"Unknown model: {model_name!r}")


def _model_needs_shifted_data(model_name: str) -> bool:
    """Whether the model uses the shift-based pipeline (MLR, Prophet-LSTM, QLSTM)."""
    return model_name in (
        "van_de_sande_mlr",
        "van_de_sande_prophet_lstm",
        "curiel_stacked_prophet_qlstm",
    )


def _model_lags(model_name: str):
    """Return autoregressive lag hours for shift-based models."""
    if model_name == "van_de_sande_mlr":
        return C.MLRConfig().autoregressive_lags_hours
    elif model_name == "van_de_sande_prophet_lstm":
        return [24, 168, 8760]
    elif model_name == "curiel_stacked_prophet_qlstm":
        return [24, 168, 8760]
    return [24, 168, 8760]


def _model_includes_exog(model_name: str) -> bool:
    """Whether the model uses exogenous predictors."""
    if model_name == "van_de_sande_mlr":
        return True
    elif model_name == "van_de_sande_prophet_lstm":
        return False  # Lags + Prophet only
    elif model_name == "curiel_stacked_prophet_qlstm":
        return True  # 17 selected features
    return False


# ---------------------------------------------------------------------------
# Main ROCV loop
# ---------------------------------------------------------------------------

def run_model(
    model_name: str,
    df: pd.DataFrame,
    rocv_cfg: C.ROCVConfig,
    output_dir: Path,
    quick: bool = False,
    single_horizon: int = None,
) -> pd.DataFrame:
    """Execute the full ROCV loop for one model."""
    logger.info("=" * 70)
    logger.info("  MODEL: %s", model_name)
    logger.info("=" * 70)

    horizons = [single_horizon] if single_horizon else rocv_cfg.forecast_horizons_days
    if quick:
        horizons = [horizons[0]]

    uses_shift = _model_needs_shifted_data(model_name)
    results = []

    for h_days in horizons:
        h_t0 = time.time()
        logger.info("━" * 60)
        logger.info("HORIZON = %d days (%d hours)", h_days, h_days * 24)
        logger.info("━" * 60)

        # Prepare data for this horizon
        if uses_shift:
            lags = _model_lags(model_name)
            include_exog = _model_includes_exog(model_name)
            df_horizon = prepare_shifted_dataset(
                df, h_days, lag_hours=lags, include_exogenous=include_exog,
            )
        else:
            df_horizon = df  # SARIMAX, Seq2Seq use raw data

        # Run each fold
        for fold_idx, train_end, val_end, test_origin in rocv_folds(df, rocv_cfg):
            fold_t0 = time.time()
            logger.info(
                "  FOLD %d — train_end=%s, val_end=%s, origin=%s, horizon=%dd",
                fold_idx, train_end.date(), val_end.date(),
                test_origin.date(), h_days,
            )

            # Get model runner (fresh instance per fold)
            runner = _get_model_runner(model_name)

            # Run fold
            if uses_shift:
                fold_result = runner.run_fold(
                    df_horizon, train_end, val_end, test_origin, h_days,
                )
            else:
                fold_result = runner.run_fold(
                    df_horizon, train_end, val_end, test_origin, h_days,
                )

            # Extract metrics
            metrics = fold_result.get("metrics")
            if metrics is None and "predictions_mw" in fold_result:
                metrics = compute_metrics(
                    fold_result["actuals_mw"], fold_result["predictions_mw"],
                )

            fold_elapsed = time.time() - fold_t0

            if metrics is not None:
                result_row = {
                    "model": model_name,
                    "fold": fold_idx,
                    "cutoff": str(test_origin.date()),
                    "horizon_days": h_days,
                    "horizon_hours": h_days * 24,
                    "n_train": fold_result.get("n_train", 0),
                    "n_test": fold_result.get("n_test", 0),
                    **metrics.to_dict(),
                    "elapsed_s": round(fold_elapsed, 1),
                }
                save_fold_result(result_row, results, output_dir, model_name)

                logger.info(
                    "    H=%3dd | RMSE=%7.1f  MAE=%7.1f  "
                    "MAPE=%.2f%%  MASE=%.3f  nMAE=%.4f  nRMSE=%.4f  "
                    "PCC=%.4f | %d pts  (%.1fs)",
                    h_days,
                    metrics.rmse, metrics.mae,
                    metrics.mape_pct, metrics.mase,
                    metrics.nmae, metrics.nrmse,
                    metrics.pcc, metrics.n_samples, fold_elapsed,
                )
            else:
                logger.warning(
                    "    Fold %d skipped — error: %s",
                    fold_idx, fold_result.get("error", "unknown"),
                )

            if quick:
                logger.info("    --quick mode: stopping after first fold.")
                break

        h_elapsed = time.time() - h_t0
        logger.info("  Horizon %d days complete in %.0fs.", h_days, h_elapsed)

    results_df = save_final_results(results, output_dir, model_name)
    return results_df


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Train baseline forecasting models via ROCV.",
    )
    parser.add_argument(
        "--data-path", type=str, default=None,
        help=f"Path to final Parquet. Default: {C.DATA_ROOT}",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help=f"Results directory. Default: {C.OUTPUT_DIR}",
    )
    parser.add_argument(
        "--model", type=str, choices=C.ALL_MODELS, default=None,
        help="Run only this model. Default: run all.",
    )
    parser.add_argument(
        "--horizon", type=int, default=None,
        help="Run only a single horizon (e.g. 60). Default: all.",
    )
    parser.add_argument(
        "--quick", action="store_true",
        help="Quick test: single fold, single horizon.",
    )
    parser.add_argument(
        "--fold-step", type=int, default=None,
        help="Override fold_step_months in ROCV config.",
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir) if args.output_dir else C.OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    t0 = time.time()
    logger.info("Baseline Models Training")
    logger.info("  Started:    %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("  Output dir: %s", output_dir)

    # ── Load data ──
    df = load_dataset(args.data_path)

    # ── ROCV config ──
    rocv_cfg = C.ROCVConfig()
    if args.fold_step is not None:
        rocv_cfg.fold_step_months = args.fold_step

    logger.info("ROCV horizons: %s days", rocv_cfg.forecast_horizons_days)
    logger.info("ROCV fold step: %d year(s)", rocv_cfg.fold_step_months)

    # ── Run models ──
    models = [args.model] if args.model else C.ALL_MODELS
    all_results = []

    for model_name in models:
        try:
            results_df = run_model(
                model_name=model_name,
                df=df,
                rocv_cfg=rocv_cfg,
                output_dir=output_dir,
                quick=args.quick,
                single_horizon=args.horizon,
            )
            all_results.append(results_df)
            print_summary_table(results_df, model_name)
            # Generate metric evolution plots
            try:
                plot_metric_evolution(results_df, model_name, output_dir)
            except Exception as plot_err:
                logger.warning("Plotting failed for %s: %s", model_name, plot_err)
        except Exception as e:
            logger.error("Model %s failed: %s", model_name, e, exc_info=True)

    # ── Combined results ──
    if all_results:
        combined = pd.concat(all_results, ignore_index=True)
        combined.to_csv(output_dir / "rocv_results_all_models.csv", index=False)

    # ── Run metadata ──
    meta = {
        "script": "train_baselines.py",
        "started": datetime.now().isoformat(),
        "data_path": str(args.data_path or C.DATA_ROOT),
        "models_run": models,
        "rocv": {
            "start_date": rocv_cfg.start_date,
            "min_train_years": rocv_cfg.min_train_years,
            "fold_step_months": rocv_cfg.fold_step_months,
            "horizons_days": rocv_cfg.forecast_horizons_days,
        },
        "total_elapsed_s": round(time.time() - t0, 1),
    }
    with open(output_dir / "run_metadata.json", "w") as f:
        json.dump(meta, f, indent=2)

    total = time.time() - t0
    logger.info("\n" + "=" * 70)
    logger.info("  DONE — %.0fs (%.1f min)", total, total / 60)
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
