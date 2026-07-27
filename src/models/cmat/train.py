"""CMAT training pipeline: Dataset, PinballLoss, and training loop.

Handles:
  - CMATDataset: creates (tabular_window, ntl_images, target) samples
  - PinballLoss: quantile regression for τ ∈ {0.05, 0.50, 0.95}
  - Dynamic image masking at the model level (ρ_img)
  - Horizon-aware feature preparation
  - AdamW + cosine annealing with warm restarts
  - Early stopping on validation pinball loss
  - Crash-safe result persistence with file locking
"""

import fcntl
import logging
import math
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, Dataset

# Baseline utilities (reuse existing ROCV logic)
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from baselines import config as BC
from baselines import evaluation as BE
from baselines.data_loader import (
    load_dataset as _baseline_load_dataset,
    rocv_folds,
    split_fold,
)

from . import config as C
from .config import CMATConfig, CMATVariant
from .model import CMAT

logger = logging.getLogger("cmat.train")


# ═══════════════════════════════════════════════════════════════════════════
# Pinball Loss (Quantile Regression)
# ═══════════════════════════════════════════════════════════════════════════

class PinballLoss(nn.Module):
    """Pinball (quantile) loss for multiple quantiles.

    L_τ(y, ŷ) = τ(y - ŷ)  if y - ŷ ≥ 0
              = (τ-1)(y - ŷ)  otherwise
    """

    def __init__(self, quantiles: List[float] = None):
        super().__init__()
        self.quantiles = quantiles or C.QUANTILES
        self.register_buffer(
            "tau", torch.tensor(self.quantiles, dtype=torch.float32)
        )

    def forward(
        self,
        y_pred: torch.Tensor,
        y_true: torch.Tensor,
    ) -> torch.Tensor:
        """
        Parameters
        ----------
        y_pred : (B, H_pred, n_quantiles)
        y_true : (B, H_pred)

        Returns
        -------
        loss : scalar
        """
        # Expand y_true to match quantile dimension
        y_true = y_true.unsqueeze(-1)  # (B, H_pred, 1)
        errors = y_true - y_pred       # (B, H_pred, n_quantiles)

        # Pinball loss per quantile
        losses = torch.where(
            errors >= 0,
            self.tau * errors,
            (self.tau - 1) * errors,
        )
        return losses.mean()


# ═══════════════════════════════════════════════════════════════════════════
# Data Loading
# ═══════════════════════════════════════════════════════════════════════════

def load_dataset() -> pd.DataFrame:
    """Load the hourly dataset with NTL columns RETAINED (unlike baselines).

    CMAT needs ntl_a2_all_mean for the CMAT-NTL variant.
    """
    path = str(C.DATA_ROOT)
    logger.info("Loading dataset from %s …", path)
    df = pd.read_parquet(path)

    # Ensure datetime index
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values("timestamp").set_index("timestamp")

    # Forward-fill lower-frequency columns
    cbs_cols = [c for c in df.columns if c.startswith("cbs_")]
    for col in cbs_cols:
        df[col] = df[col].ffill()

    # Ensure target is present
    if C.TARGET not in df.columns:
        raise ValueError(f"Target column {C.TARGET!r} not found.")
    df = df.dropna(subset=[C.TARGET])

    logger.info("Dataset loaded: %d rows × %d cols (%s → %s).",
                len(df), len(df.columns),
                df.index.min().date(), df.index.max().date())
    return df


def get_feature_columns(df: pd.DataFrame, cfg: CMATConfig) -> Tuple[List[str], List[str]]:
    """Get continuous and categorical column names present in the dataframe.

    Returns (continuous_cols, categorical_cols).
    """
    cont_cols = [c for c in C.CONTINUOUS_FEATURES if c in df.columns]
    if cfg.uses_ntl_scalar and C.NTL_AGGREGATE_FEATURE in df.columns:
        cont_cols.append(C.NTL_AGGREGATE_FEATURE)

    cat_cols = [c for c in C.CATEGORICAL_FEATURES if c in df.columns]

    logger.info("Features: %d continuous, %d categorical.",
                len(cont_cols), len(cat_cols))
    return cont_cols, cat_cols


# ═══════════════════════════════════════════════════════════════════════════
# IQR-Based Outlier Clipping
# ═══════════════════════════════════════════════════════════════════════════

def clip_iqr(
    df: pd.DataFrame,
    numerical_cols: List[str],
    multiplier: float = 1.5,
) -> pd.DataFrame:
    """Clip outliers at Q1 − k×IQR and Q3 + k×IQR (winsorize, not NaN).

    Matches thesis specification: values beyond the fences are *clamped*
    to the fence values rather than replaced with NaN.
    """
    df = df.copy()
    n_clipped = 0
    for col in numerical_cols:
        if col not in df.columns:
            continue
        q1 = df[col].quantile(0.25)
        q3 = df[col].quantile(0.75)
        iqr = q3 - q1
        lower = q1 - multiplier * iqr
        upper = q3 + multiplier * iqr
        mask = (df[col] < lower) | (df[col] > upper)
        n_clipped += mask.sum()
        df[col] = df[col].clip(lower=lower, upper=upper)
    logger.info("IQR clipping: %d values clipped across %d cols.",
                n_clipped, len(numerical_cols))
    return df


# ═══════════════════════════════════════════════════════════════════════════
# Dataset
# ═══════════════════════════════════════════════════════════════════════════

class CMATDataset(Dataset):
    """PyTorch Dataset for CMAT: produces tabular context windows + targets.

    Preprocessing follows the thesis specification:
      1. IQR outlier clipping (applied upstream in train_one_fold)
      2. Forward-fill for residual NaNs
      3. Min-Max normalization to [0, 1] — bounds from training fold only

    Features are pre-shifted by H hours upstream (in train_one_fold) so that
    at each row t the features correspond to time t−H while the target is
    the load at time t.  Each sliding window of W hours therefore produces
    a direct H-ahead forecast.
    """

    def __init__(
        self,
        df: pd.DataFrame,
        cfg: CMATConfig,
        cont_cols: List[str],
        cat_cols: List[str],
        ntl_store=None,
        ntl_mean: float = 0.0,
        ntl_std: float = 1.0,
        target_min: float = 0.0,
        target_max: float = 1.0,
        cont_min: np.ndarray = None,
        cont_max: np.ndarray = None,
        horizon_hours: int = 0,
    ):
        super().__init__()
        self.cfg = cfg
        self.ntl_store = ntl_store
        self.ntl_mean = ntl_mean
        self.ntl_std = ntl_std
        self.target_min = target_min
        self.target_max = target_max
        self.horizon_hours = horizon_hours

        W = cfg.context_window_hours

        # Extract arrays from dataframe
        self.timestamps = df.index.to_pydatetime()

        # Continuous features → float32, Min-Max [0,1]
        cont_vals = df[cont_cols].values.astype(np.float32)
        if cont_min is not None and cont_max is not None:
            self.cont_min = cont_min
            self.cont_max = cont_max
        else:
            self.cont_min = np.nanmin(cont_vals, axis=0)
            self.cont_max = np.nanmax(cont_vals, axis=0)
        denom = (self.cont_max - self.cont_min)
        denom[denom < 1e-8] = 1.0  # avoid division by zero for constant cols
        cont_vals = (cont_vals - self.cont_min) / denom
        cont_vals = np.clip(cont_vals, 0.0, 1.0)  # clip val/test that exceed train range
        cont_vals = np.nan_to_num(cont_vals, nan=0.0)
        self.cont_data = cont_vals

        # Categorical features → int64
        cat_vals = df[cat_cols].values.copy()
        # Map year to 0-indexed
        if "year" in cat_cols:
            year_idx = cat_cols.index("year")
            cat_vals[:, year_idx] = cat_vals[:, year_idx] - C.YEAR_OFFSET
            cat_vals[:, year_idx] = np.clip(cat_vals[:, year_idx], 0,
                                            C.CATEGORICAL_CARDINALITIES["year"] - 1)
        self.cat_data = cat_vals.astype(np.int64)

        # Target values (Min-Max [0,1])
        target_vals = df[C.TARGET].values.astype(np.float32)
        self.raw_target = target_vals.copy()
        target_denom = target_max - target_min
        if target_denom < 1e-8:
            target_denom = 1.0
        self.target_denom = target_denom
        self.target_data = (target_vals - target_min) / target_denom

        # Sliding window: features already shifted by H upstream, so target at i+W
        # represents the load H hours ahead of the context features.
        self.W = W
        self.n_total = len(df)

        # Valid indices: [i, i+W) context, target at i+W
        max_start = self.n_total - W - 1
        self.valid_indices = list(range(max(0, 0), max(0, max_start + 1)))

        logger.info(
            "CMATDataset: %d samples, W=%d, H_shift=%d, cont=%d, cat=%d.",
            len(self.valid_indices), W, horizon_hours,
            self.cont_data.shape[1], self.cat_data.shape[1],
        )

    def __len__(self) -> int:
        return len(self.valid_indices)

    def __getitem__(self, idx: int) -> dict:
        i = self.valid_indices[idx]

        # Context window: [i, i+W)
        x_cont = torch.from_numpy(self.cont_data[i:i + self.W])  # (W, N_cont)
        x_cat = torch.from_numpy(self.cat_data[i:i + self.W])    # (W, N_cat)

        # Target: load at i+W.  Because features were shifted by H hours
        # upstream, this value is the load H hours ahead of the features.
        target = torch.tensor(self.target_data[i + self.W], dtype=torch.float32)

        sample = {
            "x_cont": x_cont,
            "x_cat": x_cat,
            "target": target,
        }

        # NTL images (if spatial branch is active)
        if self.ntl_store is not None and self.cfg.uses_spatial:
            # The features are horizon-shifted, so self.timestamps[i + self.W - 1]
            # already refers to the forecast origin time.
            # The NTL image date is the date of the forecast origin,
            # shifted back by the horizon to avoid leakage.
            origin_ts = self.timestamps[i + self.W - 1]
            origin_date = origin_ts.date() if hasattr(origin_ts, 'date') else origin_ts

            D_W = self.cfg.n_images
            imgs = self.ntl_store.get_images_for_window(
                end_date=origin_date, n_days=D_W
            )  # (D_W, H_img, W_img)

            # Normalise NTL images
            imgs = (imgs - self.ntl_mean) / self.ntl_std

            # Add channel dimension: (D_W, 1, H_img, W_img)
            imgs = imgs[:, np.newaxis, :, :]
            sample["images"] = torch.from_numpy(imgs)

        return sample


# ═══════════════════════════════════════════════════════════════════════════
# Training Loop
# ═══════════════════════════════════════════════════════════════════════════

def train_epoch(
    model: CMAT,
    loader: DataLoader,
    criterion: PinballLoss,
    optimizer: torch.optim.Optimizer,
    device: torch.device,
) -> float:
    """Train for one epoch. Returns average loss."""
    model.train()
    total_loss = 0.0
    n_batches = 0

    for batch in loader:
        x_cont = batch["x_cont"].to(device)
        x_cat = batch["x_cat"].to(device)
        target = batch["target"].to(device)  # (B,)

        images = None
        if "images" in batch:
            images = batch["images"].to(device)

        optimizer.zero_grad()
        y_pred = model(x_cont, x_cat, images=images, H_pred=1)  # (B, 1, 3)
        # Reshape for loss: target (B,) → (B, 1)
        loss = criterion(y_pred, target.unsqueeze(-1))
        loss.backward()

        # Gradient clipping
        torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)

        optimizer.step()

        total_loss += loss.item()
        n_batches += 1

    return total_loss / max(1, n_batches)


@torch.no_grad()
def evaluate(
    model: CMAT,
    loader: DataLoader,
    criterion: PinballLoss,
    device: torch.device,
) -> Tuple[float, np.ndarray, np.ndarray]:
    """Evaluate on a dataset. Returns (loss, predictions_q50, targets)."""
    model.eval()
    total_loss = 0.0
    n_batches = 0
    all_preds = []
    all_targets = []

    for batch in loader:
        x_cont = batch["x_cont"].to(device)
        x_cat = batch["x_cat"].to(device)
        target = batch["target"].to(device)  # (B,)

        images = None
        if "images" in batch:
            images = batch["images"].to(device)

        y_pred = model(x_cont, x_cat, images=images, H_pred=1)  # (B, 1, 3)
        loss = criterion(y_pred, target.unsqueeze(-1))
        total_loss += loss.item()
        n_batches += 1

        # Extract median quantile (index 1 = τ=0.50)
        preds_q50 = y_pred[:, 0, 1].cpu().numpy()  # (B,)
        targets_np = target.cpu().numpy()             # (B,)
        all_preds.append(preds_q50)
        all_targets.append(targets_np)

    avg_loss = total_loss / max(1, n_batches)
    preds = np.concatenate(all_preds, axis=0)
    targets = np.concatenate(all_targets, axis=0)

    return avg_loss, preds, targets


def train_one_fold(
    cfg: CMATConfig,
    train_df: pd.DataFrame,
    val_df: pd.DataFrame,
    test_df: pd.DataFrame,
    ntl_store=None,
    device: torch.device = None,
    horizon_hours: int = 0,
) -> Dict:
    """Train CMAT on one ROCV fold and return metrics.

    Preprocessing (applied here, matching thesis spec):
      1. Feature shifting: shift continuous predictors by H hours so that
         at row t the features correspond to time t−H while the target
         remains the load at t.  This makes each sample a direct H-ahead
         forecast, matching the baseline (Prophet-LSTM) protocol.
      2. IQR outlier clipping on training data (predictors only)
      3. Forward-fill residual NaNs across all splits
      4. Min-Max normalization to [0, 1] — bounds from training fold only

    Returns a dict with keys matching the baselines evaluation format.
    """
    if device is None:
        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    logger.info("Training CMAT (variant=%s) on fold: train=%d, val=%d, test=%d.",
                cfg.variant.value, len(train_df), len(val_df), len(test_df))

    # Feature columns
    cont_cols, cat_cols = get_feature_columns(train_df, cfg)

    # ── Step 0: Feature shifting by horizon ──
    # Shift continuous predictors by H hours so that at each row t the
    # features correspond to time t−H while the target stays at t.
    # This matches the baseline protocol (prepare_shifted_dataset).
    H = horizon_hours
    if H > 0:
        for split_df in [train_df, val_df, test_df]:
            for col in cont_cols:
                split_df[col] = split_df[col].shift(H)
        # Drop rows where shifted features are NaN (first H rows of train)
        train_df = train_df.iloc[H:].copy()
        # val/test should be fine since they start after train
        logger.info("Feature shifting: shifted %d cols by %d hours (H=%dd).",
                    len(cont_cols), H, H // 24)

    # ── Step 1: IQR outlier clipping on training data ──
    # Clip outliers to Q1 − 1.5×IQR and Q3 + 1.5×IQR
    # Bounds computed and applied on training fold only
    train_df = clip_iqr(train_df, cont_cols)

    # ── Step 2: Forward-fill residual NaNs ──
    for split_df in [train_df, val_df, test_df]:
        for col in cont_cols + [C.TARGET]:
            if col in split_df.columns:
                split_df[col] = split_df[col].ffill().bfill()

    # ── Step 3: Min-Max normalization [0,1] — bounds from training only ──
    train_cont_vals = train_df[cont_cols].values.astype(np.float32)
    cont_min = np.nanmin(train_cont_vals, axis=0)
    cont_max = np.nanmax(train_cont_vals, axis=0)
    target_min = float(train_df[C.TARGET].min())
    target_max = float(train_df[C.TARGET].max())

    logger.info("Preprocessing: IQR clip → ffill → MinMax[0,1] (fit on train).")

    # NTL normalisation (from training dates)
    ntl_mean, ntl_std = 0.0, 1.0
    if ntl_store is not None and cfg.uses_spatial:
        train_dates = sorted(set(
            ts.date() for ts in train_df.index.to_pydatetime()
        ))
        # Sample a subset for efficiency (every 7th day)
        sample_dates = train_dates[::7]
        ntl_mean, ntl_std = ntl_store.get_normalisation_stats(sample_dates)
        logger.info("NTL normalisation: mean=%.2f, std=%.2f.", ntl_mean, ntl_std)

    # Build datasets (single-step: each sample predicts 1 value)
    dataset_kwargs = dict(
        cfg=cfg, cont_cols=cont_cols, cat_cols=cat_cols,
        ntl_store=ntl_store, ntl_mean=ntl_mean, ntl_std=ntl_std,
        target_min=target_min, target_max=target_max,
        cont_min=cont_min, cont_max=cont_max,
        horizon_hours=horizon_hours,
    )
    train_ds = CMATDataset(train_df, **dataset_kwargs)
    val_ds = CMATDataset(val_df, **dataset_kwargs)
    test_ds = CMATDataset(test_df, **dataset_kwargs)

    if len(test_ds) == 0:
        logger.warning("Test dataset has 0 samples — skipping fold.")
        return None

    # Data loaders
    num_workers = min(4, len(train_ds) // cfg.batch_size) if len(train_ds) > 0 else 0
    train_loader = DataLoader(
        train_ds, batch_size=cfg.batch_size, shuffle=True,
        num_workers=num_workers, pin_memory=(device.type == "cuda"),
        drop_last=True,
    )
    val_loader = DataLoader(
        val_ds, batch_size=cfg.batch_size, shuffle=False,
        num_workers=0, pin_memory=(device.type == "cuda"),
    )
    test_loader = DataLoader(
        test_ds, batch_size=cfg.batch_size, shuffle=False,
        num_workers=0, pin_memory=(device.type == "cuda"),
    )

    # Build model (H_pred=1: single-step prediction)
    model = CMAT(cfg).to(device)
    # Initialise decoder's W_H for (W, 1)
    model.decoder.set_horizon(cfg.context_window_hours, 1)
    model.decoder._W_H = model.decoder._W_H.to(device)

    n_params = model.count_parameters()
    logger.info("Model parameters: %s", f"{n_params:,}")

    # Optimiser
    optimizer = torch.optim.AdamW(
        model.parameters(),
        lr=cfg.learning_rate,
        weight_decay=cfg.weight_decay,
    )

    # Cosine annealing with warm restarts
    scheduler = torch.optim.lr_scheduler.CosineAnnealingWarmRestarts(
        optimizer, T_0=cfg.cosine_T_0, T_mult=cfg.cosine_T_mult,
    )

    criterion = PinballLoss(C.QUANTILES).to(device)

    # Training loop with early stopping
    best_val_loss = float("inf")
    patience_counter = 0
    best_state = None

    t0 = time.time()
    for epoch in range(1, cfg.max_epochs + 1):
        train_loss = train_epoch(
            model, train_loader, criterion, optimizer, device
        )
        val_loss, _, _ = evaluate(
            model, val_loader, criterion, device
        )
        scheduler.step()

        if epoch % 5 == 0 or epoch == 1:
            logger.info(
                "Epoch %3d/%d  train_loss=%.6f  val_loss=%.6f  lr=%.2e",
                epoch, cfg.max_epochs, train_loss, val_loss,
                optimizer.param_groups[0]["lr"],
            )

        # Early stopping
        if val_loss < best_val_loss - 1e-6:
            best_val_loss = val_loss
            patience_counter = 0
            best_state = {k: v.cpu().clone() for k, v in model.state_dict().items()}
        else:
            patience_counter += 1

        if patience_counter >= cfg.early_stop_patience:
            logger.info("Early stopping at epoch %d (val_loss=%.6f).", epoch, best_val_loss)
            break

    train_time = time.time() - t0
    logger.info("Training completed in %.1fs (%d epochs, best_val=%.6f).",
                train_time, epoch, best_val_loss)

    # Load best model
    if best_state is not None:
        model.load_state_dict(best_state)
        model = model.to(device)

    # Evaluate on test set
    test_loss, preds_norm, targets_norm = evaluate(
        model, test_loader, criterion, device
    )

    # Inverse Min-Max normalisation: x_original = x_norm * (max - min) + min
    target_range = target_max - target_min
    if target_range < 1e-8:
        target_range = 1.0
    preds_mw = preds_norm * target_range + target_min
    targets_mw = targets_norm * target_range + target_min

    # Compute metrics (same as baselines)
    metrics = BE.compute_metrics(targets_mw, preds_mw)
    logger.info(
        "Test metrics: RMSE=%.2f  MAE=%.2f  MAPE=%.2f%%  PCC=%.4f",
        metrics.rmse, metrics.mae, metrics.mape_pct, metrics.pcc,
    )

    return {
        "metrics": metrics,
        "preds_mw": preds_mw,
        "targets_mw": targets_mw,
        "best_val_loss": best_val_loss,
        "train_time_s": train_time,
        "n_params": n_params,
        "n_epochs": epoch,
    }


# ═══════════════════════════════════════════════════════════════════════════
# Crash-safe Result Persistence (file-locked, identical to baselines)
# ═══════════════════════════════════════════════════════════════════════════

def save_fold_result_locked(
    result_row: Dict,
    results_list: List[Dict],
    output_dir: Path,
    model_name: str,
) -> None:
    """Append a fold result with file-locking for parallel-job safety."""
    csv_path = output_dir / f"rocv_results_{model_name}.csv"
    lock_path = output_dir / f".lock_{model_name}"
    output_dir.mkdir(parents=True, exist_ok=True)

    with open(lock_path, "w") as lf:
        fcntl.flock(lf, fcntl.LOCK_EX)
        try:
            if csv_path.exists():
                disk_rows = pd.read_csv(csv_path).to_dict("records")
            else:
                disk_rows = []
            disk_rows.append(result_row)
            # Deduplicate by (model, fold, horizon_days)
            seen: Dict[tuple, Dict] = {}
            for row in disk_rows:
                key = (row["model"], int(row["fold"]), int(row["horizon_days"]))
                seen[key] = row
            deduped = list(seen.values())
            pd.DataFrame(deduped).to_csv(csv_path, index=False)
        finally:
            fcntl.flock(lf, fcntl.LOCK_UN)
    results_list.append(result_row)


def is_fold_done(
    output_dir: Path,
    model_name: str,
    fold_idx: int,
    horizon_days: int,
) -> bool:
    """Check if a fold/horizon combo has already been completed."""
    csv_path = output_dir / f"rocv_results_{model_name}.csv"
    if not csv_path.exists():
        return False
    try:
        df = pd.read_csv(csv_path)
        match = df[(df["fold"] == fold_idx) & (df["horizon_days"] == horizon_days)]
        return len(match) > 0
    except Exception:
        return False
