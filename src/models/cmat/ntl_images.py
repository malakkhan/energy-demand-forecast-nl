"""NTL image data loader: reads VNP46A2 HDF5 files → fixed-size 2D tensors.

Provides ``NTLImageStore``, which:
  1. Builds a date → HDF5 filepath index at init.
  2. Computes the NL polygon mask once (shared across all images).
  3. Reads, crops, masks, and pads individual daily images on demand.
  4. Caches recently accessed images via LRU.
"""

import json
import logging
import os
import re
from datetime import date, datetime, timedelta
from functools import lru_cache
from typing import Dict, List, Optional

import h5py
import numpy as np
from shapely import contains_xy
from shapely.geometry import shape

from . import config as C

logger = logging.getLogger("cmat.ntl_images")

# HDF5 internals
_VIIRS_GROUP = "HDFEOS/GRIDS/VIIRS_Grid_DNB_2d/Data Fields"
_NTL_DATASET = "Gap_Filled_DNB_BRDF-Corrected_NTL"
_QF_DATASET = "Mandatory_Quality_Flag"
_LAT_DATASET = "lat"
_LON_DATASET = "lon"
_FILL_VALUE = -999.9

# Retry for GPFS transient errors
_MAX_RETRIES = 3
_RETRY_BACKOFF = 0.5


def _parse_date_from_filename(filename: str) -> Optional[date]:
    """Extract acquisition date from VNP46A2 filename (AYYYYDDD format)."""
    match = re.search(r"\.A(\d{4})(\d{3})\.", os.path.basename(filename))
    if not match:
        return None
    year = int(match.group(1))
    doy = int(match.group(2))
    return (datetime(year, 1, 1) + timedelta(days=doy - 1)).date()


def _open_h5(filepath: str) -> h5py.File:
    """Open an HDF5 file with retries for transient I/O errors."""
    import time
    last_exc = None
    for attempt in range(_MAX_RETRIES):
        try:
            return h5py.File(filepath, "r")
        except (OSError, IOError) as exc:
            last_exc = exc
            if attempt < _MAX_RETRIES - 1:
                time.sleep(_RETRY_BACKOFF * (2 ** attempt))
    raise last_exc


class NTLImageStore:
    """Provides on-demand access to NL-cropped VNP46A2 NTL images.

    Parameters
    ----------
    viirs_dir : str or Path
        Directory containing VNP46A2 HDF5 files.
    geojson_path : str or Path
        Path to GADM NL polygon GeoJSON.
    cache_size : int
        Number of images to keep in the LRU cache.
    """

    def __init__(
        self,
        viirs_dir: str = None,
        geojson_path: str = None,
        cache_size: int = 256,
    ):
        self.viirs_dir = str(viirs_dir or C.VIIRS_DIR)
        self.geojson_path = str(geojson_path or C.GEOJSON_PATH)

        # Build date → filepath index
        self._date_index: Dict[date, str] = {}
        self._build_index()

        # Compute NL spatial mask (once)
        self._lat_idx = None
        self._lon_idx = None
        self._nl_mask = None  # 2D bool: True for NL pixels
        self._compute_mask()

        # Set up LRU cache with configurable size
        self._get_image_cached = lru_cache(maxsize=cache_size)(
            self._read_image_uncached
        )

        logger.info(
            "NTLImageStore: %d images indexed, mask shape %s, "
            "cache_size=%d.",
            len(self._date_index),
            self._nl_mask.shape if self._nl_mask is not None else "N/A",
            cache_size,
        )

    # ------------------------------------------------------------------
    # Index building
    # ------------------------------------------------------------------

    def _build_index(self):
        """Scan viirs_dir for HDF5 files and build date → path mapping."""
        h5_files = sorted(
            f for f in os.listdir(self.viirs_dir) if f.endswith(".h5")
        )
        for fname in h5_files:
            d = _parse_date_from_filename(fname)
            if d is not None:
                self._date_index[d] = os.path.join(self.viirs_dir, fname)
        logger.info("Indexed %d VNP46A2 files (%s → %s).",
                     len(self._date_index),
                     min(self._date_index) if self._date_index else "N/A",
                     max(self._date_index) if self._date_index else "N/A")

    @property
    def available_dates(self) -> List[date]:
        return sorted(self._date_index.keys())

    # ------------------------------------------------------------------
    # NL polygon mask
    # ------------------------------------------------------------------

    def _compute_mask(self):
        """Compute bounding-box indices and NL polygon mask from one HDF5."""
        if not self._date_index:
            logger.warning("No HDF5 files found — cannot compute mask.")
            return

        ref_path = next(iter(self._date_index.values()))
        with _open_h5(ref_path) as hf:
            grp = hf[_VIIRS_GROUP]
            lat = grp[_LAT_DATASET][:]
            lon = grp[_LON_DATASET][:]

        lat_mask = (lat >= C.NL_LAT_MIN) & (lat <= C.NL_LAT_MAX)
        lon_mask = (lon >= C.NL_LON_MIN) & (lon <= C.NL_LON_MAX)
        self._lat_idx = np.where(lat_mask)[0]
        self._lon_idx = np.where(lon_mask)[0]

        lat_vals = lat[lat_mask]
        lon_vals = lon[lon_mask]

        # Polygon mask
        with open(self.geojson_path) as f:
            gj = json.load(f)
        nl_geom = shape(gj["features"][0]["geometry"])

        lat_grid, lon_grid = np.meshgrid(lat_vals, lon_vals, indexing="ij")
        in_nl = contains_xy(nl_geom, lon_grid.ravel(), lat_grid.ravel())
        self._nl_mask = in_nl.reshape(lat_grid.shape)  # (H_raw, W_raw)

        logger.info(
            "NL mask: %d×%d crop, %d/%d pixels inside polygon.",
            self._nl_mask.shape[0], self._nl_mask.shape[1],
            self._nl_mask.sum(), self._nl_mask.size,
        )

    # ------------------------------------------------------------------
    # Image reading
    # ------------------------------------------------------------------

    def _read_image_uncached(self, d: date) -> np.ndarray:
        """Read a single NTL image for date d, crop, mask, and pad.

        Returns
        -------
        img : np.ndarray, shape (C.NTL_IMG_H, C.NTL_IMG_W), dtype float32
            Cropped and padded NTL radiance. Non-NL pixels are zeroed.
        """
        filepath = self._date_index.get(d)
        if filepath is None:
            # Return a zero image for missing dates
            return np.zeros((C.NTL_IMG_H, C.NTL_IMG_W), dtype=np.float32)

        try:
            with _open_h5(filepath) as hf:
                grp = hf[_VIIRS_GROUP]
                r_start = int(self._lat_idx[0])
                r_end = int(self._lat_idx[-1]) + 1
                c_start = int(self._lon_idx[0])
                c_end = int(self._lon_idx[-1]) + 1
                ntl_crop = grp[_NTL_DATASET][r_start:r_end, c_start:c_end]
        except Exception as exc:
            logger.warning("Error reading %s: %s — returning zero image.", d, exc)
            return np.zeros((C.NTL_IMG_H, C.NTL_IMG_W), dtype=np.float32)

        # Convert to float32, replace fill values with 0
        img = ntl_crop.astype(np.float32)
        img[np.isclose(img, _FILL_VALUE, atol=0.1)] = 0.0
        img[img < 0] = 0.0  # clamp negative radiance

        # Zero out non-NL pixels (sea, foreign territory)
        img[~self._nl_mask] = 0.0

        # Pad width from 985 → 992 (right-side zero padding)
        if img.shape[1] < C.NTL_IMG_W:
            pad_w = C.NTL_IMG_W - img.shape[1]
            img = np.pad(img, ((0, 0), (0, pad_w)), mode="constant",
                         constant_values=0.0)

        return img

    def get_image(self, d: date) -> np.ndarray:
        """Get a single NTL image with caching.

        Returns shape (NTL_IMG_H, NTL_IMG_W), dtype float32.
        """
        return self._get_image_cached(d)

    def get_images_for_window(
        self,
        end_date: date,
        n_days: int,
    ) -> np.ndarray:
        """Get a stack of daily NTL images for the lookback window.

        Parameters
        ----------
        end_date : date
            The last date in the window (inclusive).
        n_days : int
            Number of days (D_W).

        Returns
        -------
        imgs : np.ndarray, shape (n_days, NTL_IMG_H, NTL_IMG_W), float32
        """
        imgs = np.empty(
            (n_days, C.NTL_IMG_H, C.NTL_IMG_W), dtype=np.float32
        )
        for k in range(n_days):
            # Most recent day is last in the stack
            d = end_date - timedelta(days=n_days - 1 - k)
            imgs[k] = self.get_image(d)
        return imgs

    def get_normalisation_stats(
        self,
        dates: List[date],
    ) -> tuple:
        """Compute mean and std over a set of dates for normalisation.

        Returns (mean, std) as scalar float32 values computed over all
        non-zero NL pixels across the provided dates.
        """
        all_vals = []
        for d in dates:
            img = self.get_image(d)
            vals = img[img > 0]
            if len(vals) > 0:
                all_vals.append(vals)
        if not all_vals:
            return 0.0, 1.0
        combined = np.concatenate(all_vals)
        return float(combined.mean()), float(combined.std() + 1e-8)
