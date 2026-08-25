"""Optuna hyperparameter search for CMAT.

Per-horizon search: each horizon is treated as its own optimisation problem.
The search runs on a designated fold (default fold 0) and returns the
best-performing configuration for that horizon.
"""

import argparse
import json
import logging
import math
import sys
from pathlib import Path

import optuna
from optuna.pruners import MedianPruner

# Baseline utilities
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from baselines import config as BC
from baselines.data_loader import rocv_folds, split_fold

from . import config as C
from .config import CMATConfig, CMATVariant, SEARCH_SPACE
from .train import load_dataset, train_one_fold

logger = logging.getLogger("cmat.search")


def _build_config_from_trial(
    trial: optuna.Trial,
    variant: CMATVariant,
    horizon_days: int,
    min_context_window: int = 1,
) -> CMATConfig:
    """Sample hyperparameters from Optuna trial with constraint pruning.

    For Tab/NTL variants, spatial params (P, ρ_img, h_c) are NOT searched
    since those modules are inactive — saves search budget.
    """

    # Categorical parameters (optionally constrained)
    w_choices = [w for w in SEARCH_SPACE["context_window_hours"]
                 if w >= min_context_window]
    W = trial.suggest_categorical("context_window_hours", w_choices)
    d = trial.suggest_categorical("embed_dim", SEARCH_SPACE["embed_dim"])
    h_s = trial.suggest_categorical("self_attn_heads",
                                    SEARCH_SPACE["self_attn_heads"])
    L = trial.suggest_categorical("transformer_depth",
                                  SEARCH_SPACE["transformer_depth"])
    p_drop = trial.suggest_categorical("dropout", SEARCH_SPACE["dropout"])
    B = trial.suggest_categorical("batch_size", SEARCH_SPACE["batch_size"])

    # Spatial parameters: only search for variants that use them
    uses_spatial = variant in (CMATVariant.EARLY_FUSION, CMATVariant.FULL)
    if uses_spatial:
        h_c = trial.suggest_categorical("cross_attn_heads",
                                        SEARCH_SPACE["cross_attn_heads"])
        P = trial.suggest_categorical("ntl_patch_size",
                                      SEARCH_SPACE["ntl_patch_size"])
        rho = trial.suggest_categorical("image_mask_ratio",
                                        SEARCH_SPACE["image_mask_ratio"])
    else:
        # Fixed defaults — these have no effect on Tab/NTL
        h_c = 4
        P = 16
        rho = 0.75

    # Log-uniform continuous parameters
    lr_lo, lr_hi = SEARCH_SPACE["learning_rate"]
    lr = trial.suggest_float("learning_rate", lr_lo, lr_hi, log=True)
    wd_lo, wd_hi = SEARCH_SPACE["weight_decay"]
    wd = trial.suggest_float("weight_decay", wd_lo, wd_hi, log=True)

    # Constraint pruning: h_s must divide d (h_c checked only if spatial)
    if d % h_s != 0:
        raise optuna.TrialPruned(
            f"Head divisibility violated: d={d}, h_s={h_s}"
        )
    if uses_spatial and d % h_c != 0:
        raise optuna.TrialPruned(
            f"Head divisibility violated: d={d}, h_c={h_c}"
        )

    return CMATConfig(
        context_window_hours=W,
        embed_dim=d,
        self_attn_heads=h_s,
        cross_attn_heads=h_c,
        transformer_depth=L,
        ntl_patch_size=P,
        image_mask_ratio=rho,
        dropout=p_drop,
        learning_rate=lr,
        weight_decay=wd,
        batch_size=B,
        variant=variant,
        horizon_hours=horizon_days * 24,
        # Training budget for search — must be generous enough to
        # let the model escape flat-mean local minima (needs ~10+ epochs)
        max_epochs=50,
        early_stop_patience=15,
        min_epochs_before_es=10,
    )


def run_search(
    horizon_days: int,
    variant: str = "full",
    n_trials: int = 50,
    search_fold: int = 0,
    study_name: str = None,
    storage: str = None,
    output_dir: str = None,
    min_context_window: int = 1,
) -> dict:
    """Run Optuna hyperparameter search for one horizon.

    Parameters
    ----------
    horizon_days : int
        Forecast horizon in days (60, 75, ..., 180).
    variant : str
        CMAT variant name ("tab", "ntl", "early", "full").
    n_trials : int
        Number of Optuna trials.
    search_fold : int
        ROCV fold index to use for the search.
    study_name : str
        Optuna study name (for persistence).
    storage : str
        Optuna storage URL (e.g. sqlite:///optuna.db).
    output_dir : str
        Directory to save the best config JSON.

    Returns
    -------
    best_config : dict
        Best hyperparameters found.
    """
    import torch

    variant_enum = CMATVariant(variant)
    output_path = Path(output_dir or C.OUTPUT_DIR)
    output_path.mkdir(parents=True, exist_ok=True)

    logger.info(
        "=== Optuna Search: horizon=%dd, variant=%s, n_trials=%d, fold=%d ===",
        horizon_days, variant, n_trials, search_fold,
    )

    # Load data
    df = load_dataset()

    # Get the specified fold
    rocv_cfg = BC.ROCVConfig()
    folds = list(rocv_folds(df, rocv_cfg))
    if search_fold >= len(folds):
        raise ValueError(f"Fold {search_fold} not available (max={len(folds)-1})")
    fold_idx, train_end, val_end, test_origin = folds[search_fold]

    train_df, val_df, test_df = split_fold(
        df, train_end, val_end, test_origin, horizon_days
    )
    logger.info("Fold %d: train=%d, val=%d, test=%d.",
                fold_idx, len(train_df), len(val_df), len(test_df))

    # NTL store (only for spatial variants)
    ntl_store = None
    if variant_enum in (CMATVariant.EARLY_FUSION, CMATVariant.FULL):
        from .ntl_images import NTLImageStore
        ntl_store = NTLImageStore()

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    def objective(trial: optuna.Trial) -> float:
        cfg = _build_config_from_trial(trial, variant_enum, horizon_days,
                                       min_context_window=min_context_window)

        try:
            result = train_one_fold(
                cfg, train_df, val_df, test_df,
                ntl_store=ntl_store, device=device,
                horizon_hours=horizon_days * 24,
            )
            if result is None:
                raise optuna.TrialPruned("Empty test set")
            val_loss = result["best_val_loss"]
            pcc = result["val_metrics"].pcc  # Use VALIDATION PCC, not test
            trial.set_user_attr("val_rmse", result["val_metrics"].rmse)
            trial.set_user_attr("val_pcc", result["val_metrics"].pcc)
            trial.set_user_attr("test_rmse", result["metrics"].rmse)
            trial.set_user_attr("test_pcc", result["metrics"].pcc)
            trial.set_user_attr("n_params", result["n_params"])
            trial.set_user_attr("n_epochs", result["n_epochs"])

            # Composite objective: penalize flat-mean predictions (low PCC).
            # val_loss * (2 - PCC):  PCC=1.0 → 1.0x, PCC=0.0 → 2.0x penalty.
            # This prevents Optuna from selecting configs that "cheat" by
            # predicting the mean to get low val_loss but useless forecasts.
            pcc_penalty = 2.0 - max(0.0, pcc)  # clamp negative PCC
            composite = val_loss * pcc_penalty
            logger.info("Trial %d: val_loss=%.6f, PCC=%.4f, composite=%.6f",
                        trial.number, val_loss, pcc, composite)
            return composite
        except Exception as exc:
            logger.error("Trial %d failed: %s", trial.number, exc)
            raise optuna.TrialPruned(str(exc))

    # Create/resume study
    sname = study_name or f"cmat_{variant}_h{horizon_days}d"
    study = optuna.create_study(
        study_name=sname,
        storage=storage,
        direction="minimize",
        pruner=MedianPruner(n_startup_trials=5, n_warmup_steps=5),
        load_if_exists=True,
    )

    # ── Crash-safe persistence ──
    # Save the best config after EVERY completed trial so that
    # wall-time kills, OOM, or cancellations never lose data.

    def _save_current_best(study: optuna.Study, label: str = "callback"):
        """Write the current best trial to disk."""
        try:
            best_trial = study.best_trial
        except ValueError:
            return  # no completed trials yet

        best_params = best_trial.params.copy()
        best_params["variant"] = variant
        best_params["horizon_days"] = horizon_days
        best_params["best_val_loss"] = best_trial.value
        best_params["best_trial_number"] = best_trial.number
        for k, v in best_trial.user_attrs.items():
            best_params[f"test_{k}"] = v

        out_json = output_path / f"best_config_{variant}_h{horizon_days}d.json"
        with open(out_json, "w") as f:
            json.dump(best_params, f, indent=2)
        logger.info("[%s] Best config saved → %s (trial %d, val=%.6f)",
                    label, out_json, best_trial.number, best_trial.value)

    def _after_trial_callback(study, trial):
        """Optuna callback: persist best config after every finished trial."""
        if trial.state == optuna.trial.TrialState.COMPLETE:
            n_complete = len([t for t in study.trials
                             if t.state == optuna.trial.TrialState.COMPLETE])
            logger.info("Completed trials so far: %d/%d", n_complete, n_trials)
            _save_current_best(study, label=f"after_trial_{n_complete}")

    # Register SIGTERM handler (SLURM sends SIGTERM before wall-time kill)
    import signal

    def _sigterm_handler(signum, frame):
        logger.warning("Received signal %d — saving best config before exit.", signum)
        _save_current_best(study, label="SIGTERM")
        sys.exit(0)

    signal.signal(signal.SIGTERM, _sigterm_handler)
    signal.signal(signal.SIGUSR1, _sigterm_handler)  # SLURM also uses USR1

    study.optimize(objective, n_trials=n_trials, callbacks=[_after_trial_callback])

    # Final save (redundant but explicit)
    _save_current_best(study, label="final")

    best_trial = study.best_trial
    best_params = best_trial.params.copy()
    best_params["variant"] = variant
    best_params["horizon_days"] = horizon_days
    best_params["best_val_loss"] = best_trial.value
    best_params["best_trial_number"] = best_trial.number
    for k, v in best_trial.user_attrs.items():
        best_params[f"test_{k}"] = v

    logger.info("Best trial: %s", best_params)

    return best_params


def load_best_config(
    variant: str,
    horizon_days: int,
    output_dir: str = None,
) -> CMATConfig:
    """Load the best config from a completed search."""
    output_path = Path(output_dir or C.OUTPUT_DIR)
    json_path = output_path / f"best_config_{variant}_h{horizon_days}d.json"

    if not json_path.exists():
        raise FileNotFoundError(f"No search results at {json_path}")

    with open(json_path) as f:
        params = json.load(f)

    cfg = CMATConfig(
        context_window_hours=params["context_window_hours"],
        embed_dim=params["embed_dim"],
        self_attn_heads=params["self_attn_heads"],
        cross_attn_heads=params.get("cross_attn_heads", 4),
        transformer_depth=params["transformer_depth"],
        ntl_patch_size=params.get("ntl_patch_size", 16),
        image_mask_ratio=params.get("image_mask_ratio", 0.75),
        dropout=params["dropout"],
        learning_rate=params["learning_rate"],
        weight_decay=params["weight_decay"],
        batch_size=params["batch_size"],
        variant=CMATVariant(variant),
        horizon_hours=horizon_days * 24,
    )
    return cfg


# ═══════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s  %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        description="CMAT Optuna hyperparameter search (per-horizon)."
    )
    parser.add_argument(
        "--horizon", type=int, required=True,
        help="Forecast horizon in days (60, 75, ..., 180)."
    )
    parser.add_argument(
        "--variant", type=str, default="full",
        choices=["tab", "ntl", "early", "full"],
        help="CMAT variant to search."
    )
    parser.add_argument(
        "--n-trials", type=int, default=50,
        help="Number of Optuna trials."
    )
    parser.add_argument(
        "--fold", type=int, default=0,
        help="ROCV fold index for search."
    )
    parser.add_argument(
        "--storage", type=str, default=None,
        help="Optuna storage URL (default: in-memory)."
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Output directory for best config JSON."
    )
    parser.add_argument(
        "--min-context-window", type=int, default=1,
        help="Minimum context window (hours). Prunes W values below this."
    )
    args = parser.parse_args()

    run_search(
        horizon_days=args.horizon,
        variant=args.variant,
        n_trials=args.n_trials,
        search_fold=args.fold,
        storage=args.storage,
        output_dir=args.output_dir,
        min_context_window=args.min_context_window,
    )


if __name__ == "__main__":
    main()
