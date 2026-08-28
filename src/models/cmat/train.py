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

import logging
import os
import random
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
from sklearn.decomposition import PCA as _PCA
from sklearn.preprocessing import StandardScaler as _StandardScaler
from .config import CMATConfig, CMATVariant
from .model import CMAT

logger = logging.getLogger("cmat.train")


# ═══════════════════════════════════════════════════════════════════════════
# Pinball Loss (Quantile Regression)
# ═══════════════════════════════════════════════════════════════════════════

# PinballLoss removed -- replaced with nn.MSELoss for point forecasting.
# Previously used quantile regression (tau = 0.05, 0.50, 0.95).


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
    criterion: nn.Module,
    optimizer: torch.optim.Optimizer,
    device: torch.device,
    scaler: torch.amp.GradScaler = None,
) -> float:
    """Train for one epoch with optional AMP. Returns average loss."""
    model.train()
    total_loss = 0.0
    n_batches = 0
    use_amp = scaler is not None

    for batch in loader:
        x_cont = batch["x_cont"].to(device, non_blocking=True)
        x_cat = batch["x_cat"].to(device, non_blocking=True)
        target = batch["target"].to(device, non_blocking=True)

        images = None
        if "images" in batch:
            images = batch["images"].to(device, non_blocking=True)

        optimizer.zero_grad(set_to_none=True)

        with torch.amp.autocast("cuda", enabled=use_amp, dtype=torch.bfloat16):
            y_pred = model(x_cont, x_cat, images=images, H_pred=1)
            loss = criterion(y_pred.squeeze(-1).squeeze(-1), target)

        if use_amp:
            scaler.scale(loss).backward()
            scaler.unscale_(optimizer)
            torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
            scaler.step(optimizer)
            scaler.update()
        else:
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
            optimizer.step()

        total_loss += loss.item()
        n_batches += 1

    return total_loss / max(1, n_batches)


@torch.no_grad()
def evaluate(
    model: CMAT,
    loader: DataLoader,
    criterion: nn.Module,
    device: torch.device,
    use_amp: bool = False,
) -> Tuple[float, np.ndarray, np.ndarray]:
    """Evaluate on a dataset with optional AMP. Returns (loss, predictions_q50, targets)."""
    model.eval()
    total_loss = 0.0
    n_batches = 0
    all_preds = []
    all_targets = []

    for batch in loader:
        x_cont = batch["x_cont"].to(device, non_blocking=True)
        x_cat = batch["x_cat"].to(device, non_blocking=True)
        target = batch["target"].to(device, non_blocking=True)

        images = None
        if "images" in batch:
            images = batch["images"].to(device, non_blocking=True)

        with torch.amp.autocast("cuda", enabled=use_amp, dtype=torch.bfloat16):
            y_pred = model(x_cont, x_cat, images=images, H_pred=1)
            loss = criterion(y_pred.squeeze(-1).squeeze(-1), target)

        total_loss += loss.item()
        n_batches += 1

        preds_point = y_pred.squeeze(-1).squeeze(-1).float().cpu().numpy()
        targets_np = target.float().cpu().numpy()
        all_preds.append(preds_point)
        all_targets.append(targets_np)

    avg_loss = total_loss / max(1, n_batches)
    preds = np.concatenate(all_preds, axis=0)
    targets = np.concatenate(all_targets, axis=0)

    return avg_loss, preds, targets


def _set_seed(seed: int) -> None:
    """Seed all RNGs for reproducible training."""
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed)




def _fit_fold_pca(
    train_df: pd.DataFrame,
    val_df: pd.DataFrame,
    test_df: pd.DataFrame,
    variance_threshold: float = C.PCA_VARIANCE_THRESHOLD,
    max_null_frac: float = C.PCA_MAX_NULL_FRAC,
) -> Tuple[List[str], pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Fit PCA on training data only; transform all splits.

    Selects n_components to explain >= variance_threshold of variance.
    Returns the new PCA column names and updated dataframes.
    """
    # Identify raw CBS + KNMI validated columns in the dataset
    all_cols = train_df.columns.tolist()
    raw_cols = sorted([
        c for c in all_cols
        if (c.startswith("cbs_") or c.startswith("knmi_val_"))
        and not c.startswith("pca_")
    ])

    if not raw_cols:
        logger.warning("No CBS/KNMI columns found for per-fold PCA.")
        return [], train_df, val_df, test_df

    # Drop columns with too many NaNs in training data
    train_null_frac = train_df[raw_cols].isna().mean()
    keep_cols = sorted(train_null_frac[train_null_frac <= max_null_frac].index.tolist())

    if len(keep_cols) < 2:
        logger.warning("Too few CBS/KNMI columns with sufficient coverage (%d). Skipping PCA.", len(keep_cols))
        return [], train_df, val_df, test_df

    logger.info("Per-fold PCA: %d/%d raw columns retained (>%.0f%% coverage).",
                len(keep_cols), len(raw_cols), (1 - max_null_frac) * 100)

    # Fit on complete training rows only
    train_complete = train_df[keep_cols].dropna()
    if len(train_complete) < 10:
        logger.warning("Too few complete training rows for PCA (%d). Skipping.", len(train_complete))
        return [], train_df, val_df, test_df

    X_train = train_complete.values.astype(np.float64)
    scaler = _StandardScaler().fit(X_train)
    X_scaled = scaler.transform(X_train)

    # Fit full PCA to determine n_components for 95% variance
    pca_full = _PCA().fit(X_scaled)
    cum_var = np.cumsum(pca_full.explained_variance_ratio_)
    n_components = int(np.searchsorted(cum_var, variance_threshold) + 1)
    n_components = min(n_components, len(keep_cols))

    logger.info("Per-fold PCA: %d components explain %.1f%% variance (threshold=%.0f%%).",
                n_components, cum_var[n_components - 1] * 100, variance_threshold * 100)

    # Refit with selected n_components
    pca = _PCA(n_components=n_components).fit(X_scaled)

    # Transform all splits
    pca_col_names = [f"pca_fold_{i:02d}" for i in range(1, n_components + 1)]

    for split_df in [train_df, val_df, test_df]:
        # Initialise with NaN
        for col in pca_col_names:
            split_df[col] = np.nan

        # Transform complete rows
        complete_mask = split_df[keep_cols].notna().all(axis=1)
        if complete_mask.any():
            X = split_df.loc[complete_mask, keep_cols].values.astype(np.float64)
            X_s = scaler.transform(X)
            pca_vals = pca.transform(X_s)
            split_df.loc[complete_mask, pca_col_names] = pca_vals

    return pca_col_names, train_df, val_df, test_df

def train_one_fold(
    cfg: CMATConfig,
    train_df: pd.DataFrame,
    val_df: pd.DataFrame,
    test_df: pd.DataFrame,
    ntl_store=None,
    device: torch.device = None,
    horizon_hours: int = 0,
    fold_idx: int = 0,
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

    # ── Reproducibility: seed all RNGs ──
    _set_seed(cfg.seed)

    logger.info("Training CMAT (variant=%s, seed=%d) on fold: train=%d, val=%d, test=%d.",
                cfg.variant.value, cfg.seed, len(train_df), len(val_df), len(test_df))

    # Feature columns
    cont_cols, cat_cols = get_feature_columns(train_df, cfg)

    # ── Step 0: Feature shifting by horizon ──
    # Shift continuous predictors by H hours so that at each row t the
    # features correspond to time t−H while the target stays at t.
    # This matches the baseline protocol (prepare_shifted_dataset).
    #
    # IMPORTANT: We must shift on the CONCATENATED timeline, not on each
    # split independently.  split_fold() returns .copy(), so shift(H) on
    # an isolated test_df (which has exactly H rows) would produce 100%
    # NaN.  By concatenating first, the shift can pull valid data from
    # the preceding split.
    H = horizon_hours
    if H > 0:
        full_df = pd.concat([train_df, val_df, test_df])
        for col in cont_cols:
            full_df[col] = full_df[col].shift(H)

        # Re-split using the original indices
        train_df = full_df.loc[train_df.index].iloc[H:].copy()  # drop first H NaN rows
        val_df = full_df.loc[val_df.index].copy()
        test_df = full_df.loc[test_df.index].copy()

        logger.info("Feature shifting: shifted %d cols by %d hours (H=%dd).",
                    len(cont_cols), H, H // 24)

    # ── Step 1: IQR outlier clipping on training data ──
    # Clip outliers to Q1 − 1.5×IQR and Q3 + 1.5×IQR
    # Bounds computed and applied on training fold only
    train_df = clip_iqr(train_df, cont_cols)

    # ── Step 1b: Per-fold PCA (fitted on training data only) ──
    pca_col_names, train_df, val_df, test_df = _fit_fold_pca(
        train_df, val_df, test_df,
    )
    if pca_col_names:
        cont_cols = cont_cols + pca_col_names
        logger.info("Added %d per-fold PCA columns. Total continuous: %d.",
                    len(pca_col_names), len(cont_cols))

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

    # Data loaders — use more workers and prefetching for GPU throughput.
    #
    # Exception: for spatial variants (ntl_store is not None), force
    # num_workers=0. CMATDataset reads from ntl_store's preloaded ~15.5GB
    # image array. Storing it as one contiguous array instead of a
    # Dict[date, np.ndarray] (see ntl_images.py) avoids the classic
    # per-element refcount copy-on-write leak, but empirically that alone
    # is not enough: forking num_workers processes against an already
    # ~15.5GB-resident process gets accounted against SLURM's cgroup
    # memory limit at spawn time and OOM-kills the job within minutes,
    # before training even starts (confirmed 2026-08-26 — killed right at
    # first-batch worker spawn, not a gradual leak). num_workers=0 avoids
    # forking entirely. Tab/NTL variants (no ntl_store) are unaffected and
    # keep full worker parallelism.
    is_cuda = device.type == "cuda"
    if ntl_store is not None:
        # Override via CMAT_SPATIAL_NUM_WORKERS for controlled experiments
        # (default 0 — the only setting confirmed safe so far).
        num_workers = int(os.environ.get("CMAT_SPATIAL_NUM_WORKERS", "0"))
    else:
        num_workers = min(8, len(train_ds) // cfg.batch_size) if len(train_ds) > 0 else 0
    loader_kwargs = dict(
        pin_memory=is_cuda,
        persistent_workers=(num_workers > 0),
        prefetch_factor=4 if num_workers > 0 else None,
    )
    train_loader = DataLoader(
        train_ds, batch_size=cfg.batch_size, shuffle=True,
        num_workers=num_workers, drop_last=True, **loader_kwargs,
    )
    val_workers = min(4, num_workers)
    val_loader = DataLoader(
        val_ds, batch_size=cfg.batch_size, shuffle=False,
        num_workers=val_workers,
        pin_memory=is_cuda,
        persistent_workers=(val_workers > 0),
    )
    test_workers = 0 if ntl_store is not None else 8
    test_loader = DataLoader(
        test_ds, batch_size=cfg.batch_size, shuffle=False,
        num_workers=test_workers,
        pin_memory=is_cuda,
        persistent_workers=(test_workers > 0),
    )

    # Build model (H_pred=1: single-step prediction)
    # n_cont must match cont_cols exactly (base features + this fold's
    # per-fold PCA components, whose count varies fold-to-fold).
    model = CMAT(cfg, n_cont=len(cont_cols)).to(device)
    # Initialise decoder's W_H for (W, 1)
    model.decoder.set_horizon(cfg.context_window_hours, 1)
    model.decoder._W_H = model.decoder._W_H.to(device)

    n_params = model.count_parameters()
    logger.info("Model parameters: %s", f"{n_params:,}")

    # Compile model for GPU kernel fusion (~15-30% speedup)
    # Disabled due to missing 'nvcc' in SLURM environment (InductorError).
    # if is_cuda and hasattr(torch, 'compile'):
    #     try:
    #         model = torch.compile(model)
    #         logger.info("torch.compile() enabled.")
    #     except Exception as exc:
    #         logger.warning("torch.compile() failed, continuing without: %s", exc)

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

    criterion = nn.MSELoss().to(device)

    # AMP (mixed precision) — bf16 on A100 for ~2x speedup
    use_amp = is_cuda and torch.cuda.is_bf16_supported()
    scaler = torch.amp.GradScaler("cuda", enabled=use_amp) if use_amp else None
    if use_amp:
        logger.info("AMP enabled (bfloat16).")

    # Training loop with early stopping
    best_val_loss = float("inf")
    patience_counter = 0
    best_state = None

    t0 = time.time()
    for epoch in range(1, cfg.max_epochs + 1):
        train_loss = train_epoch(
            model, train_loader, criterion, optimizer, device, scaler=scaler
        )
        val_loss, _, _ = evaluate(
            model, val_loader, criterion, device, use_amp=use_amp
        )
        scheduler.step()

        if epoch % 5 == 0 or epoch == 1:
            logger.info(
                "Epoch %3d/%d  train_loss=%.6f  val_loss=%.6f  lr=%.2e",
                epoch, cfg.max_epochs, train_loss, val_loss,
                optimizer.param_groups[0]["lr"],
            )

        # Early stopping (with minimum epoch guard)
        if val_loss < best_val_loss - 1e-6:
            best_val_loss = val_loss
            patience_counter = 0
            best_state = {k: v.cpu().clone() for k, v in model.state_dict().items()}
        elif epoch >= cfg.min_epochs_before_es:
            # Only start counting patience after minimum epochs
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

    # Save checkpoint for post-hoc analysis (permutation importance, etc.)
    ckpt_dir = Path(C.CHECKPOINT_DIR)
    ckpt_dir.mkdir(parents=True, exist_ok=True)
    seed = cfg.seed
    ckpt_path = ckpt_dir / f"ckpt_{cfg.variant.value}_h{horizon_hours // 24}d_f{fold_idx}_s{seed}.pt"
    torch.save({
        "model_state_dict": best_state,
        "config": cfg,
        "cont_min": cont_min,
        "cont_max": cont_max,
        "target_min": target_min,
        "target_max": target_max,
        "ntl_mean": ntl_mean,
        "ntl_std": ntl_std,
        "fold_idx": fold_idx,
        "horizon_days": horizon_hours // 24,
        "seed": seed,
        "n_params": n_params,
    }, ckpt_path)
    logger.info("Saved checkpoint → %s", ckpt_path)

    # Inverse Min-Max normalisation: x_original = x_norm * (max - min) + min
    target_range = target_max - target_min
    if target_range < 1e-8:
        target_range = 1.0

    # Evaluate on validation set (for Optuna objective — no test leakage)
    val_loss_final, val_preds_norm, val_targets_norm = evaluate(
        model, val_loader, criterion, device
    )
    val_preds_mw = val_preds_norm * target_range + target_min
    val_targets_mw = val_targets_norm * target_range + target_min
    val_metrics = BE.compute_metrics(val_targets_mw, val_preds_mw)
    logger.info(
        "Val metrics: RMSE=%.2f  MAE=%.2f  MAPE=%.2f%%  PCC=%.4f",
        val_metrics.rmse, val_metrics.mae, val_metrics.mape_pct, val_metrics.pcc,
    )

    # Evaluate on test set
    t0_infer = time.time()
    test_loss, preds_norm, targets_norm = evaluate(
        model, test_loader, criterion, device
    )
    infer_time = time.time() - t0_infer
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
        "val_metrics": val_metrics,
        "preds_mw": preds_mw,
        "targets_mw": targets_mw,
        "best_val_loss": best_val_loss,
        "train_time_s": train_time,
        "infer_time_s": infer_time,
        "n_params": n_params,
        "n_epochs": epoch,
        "n_train": len(train_ds),
        "n_val": len(val_ds),
        "n_test": len(test_ds),
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
    """Append a fold result to this task's own shard file.

    Each (model, horizon, seed) combination owns an exclusive shard file
    under ``_shards/``, so concurrent SLURM array tasks (possibly on
    different nodes) never contend for the same path. This avoids
    relying on fcntl.flock() for cross-node mutual exclusion, which is
    not reliably honoured on this GPFS-mounted scratch filesystem and
    previously caused silent data loss when multiple baseline tasks
    wrote a shared CSV concurrently. Shards are combined into the final
    rocv_results_{model_name}.csv via baselines/merge_shards.py.
    """
    results_list.append(result_row)

    shard_dir = output_dir / "_shards"
    shard_dir.mkdir(parents=True, exist_ok=True)
    h = int(result_row["horizon_days"])
    seed = int(result_row.get("seed", 42))
    shard_path = shard_dir / f"{model_name}_h{h}_s{seed}.csv"
    pd.DataFrame(results_list).to_csv(shard_path, index=False)


def is_fold_done(
    output_dir: Path,
    model_name: str,
    fold_idx: int,
    horizon_days: int,
    seed: int = None,
) -> bool:
    """Check if a fold/horizon/seed combo has already been completed,
    by looking at this (model, horizon, seed)'s own shard file."""
    shard_path = output_dir / "_shards" / f"{model_name}_h{horizon_days}_s{seed if seed is not None else 42}.csv"
    if not shard_path.exists():
        return False
    try:
        df = pd.read_csv(shard_path)
        return len(df[df["fold"] == fold_idx]) > 0
    except Exception:
        return False
