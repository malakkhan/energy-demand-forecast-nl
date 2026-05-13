# Netherlands Energy Demand Forecasting — Exploratory Data Analysis Report

**Generated:** 2026-05-13  
**Pipeline Version:** 3-phase Spark ETL + batch analysis  
**Final Dataset:** `data/processed/nl_hourly_dataset.parquet`

---

## Table of Contents

1. [Dataset Overview](#1-dataset-overview)
2. [Data Quality & Coverage](#2-data-quality--coverage)
3. [Gap Analysis](#3-gap-analysis)
4. [ENTSO-E Electricity Load](#4-entso-e-electricity-load)
5. [VIIRS Nighttime Light](#5-viirs-nighttime-light)
   - [5.3 Spatial Filtering — Netherlands GADM Polygon Mask](#53-spatial-filtering--netherlands-gadm-polygon-mask)
   - [5.4 A1 vs A2 Comparison](#54-a1-vs-a2-comparison)
   - [5.5 Valid Pixel Analysis — Cloud Seasonality & Measurement Bias](#55-valid-pixel-analysis--cloud-seasonality--measurement-bias)
   - [5.6 VIIRS A2 Non-Selective (Including Imputed Pixels)](#56-viirs-a2-non-selective-including-imputed-pixels)
6. [CBS Economic Indicators](#6-cbs-economic-indicators)
7. [KNMI Meteorological Observations](#7-knmi-meteorological-observations)
8. [KNMI Validated vs Non-Validated Comparison](#8-knmi-validated-vs-non-validated-comparison)
9. [Cross-Variable Analysis](#9-cross-variable-analysis)
10. [Key Findings](#10-key-findings--recommendations)

---

## 1. Dataset Overview

The final merged dataset spans **14 years** of hourly observations for the Netherlands, combining electricity demand, satellite imagery, economic indicators, and meteorological data.

| Property | Value |
|----------|-------|
| **Rows** | 122,736 hourly records |
| **Columns** | 114 features |
| **Time Span** | 2012-01-01 → 2025-12-31 (UTC) |
| **File Size** | 9.2 MB (Parquet, Snappy compressed) |
| **Temporal Resolution** | Hourly |

### Data Sources

| Source | Description | Records | Coverage |
|--------|-------------|---------|----------|
| **ENTSO-E** | Electricity load (MW) | 175,296 | 100.0% |
| **VIIRS A2 (selective)** | Gap-filled NTL composite, quality ≤ 1 only (daily) | 5,080 | 54.7% |
| **VIIRS A2 (non-selective)** | Gap-filled NTL composite, all non-fill pixels (daily) | 5,080 | 97.5% |
| **VIIRS A1** | At-sensor radiance (daily) | 5,102 | 96.4% |
| **CBS Tariffs** | Gas & electricity tariffs (semi-annual) | — | 57.1% |
| **CBS GDP** | Quarterly GDP growth | — | 100.0% |
| **CBS CPI** | Monthly consumer price index | — | 100.0% |
| **CBS GEP** | Gas & electricity prices (semi-annual) | — | 100.0% |
| **KNMI** | Hourly meteorological (non-validated) | 97,765 | 77.3% |
| **KNMI Validated** | Hourly meteorological (validated) | 125,878 | 100.0% |

---

## 2. Data Quality & Coverage

### 2.1 Phase 1 — Extraction Quality

| Source | Files | Records | Errors | Null % (key column) |
|--------|-------|---------|--------|---------------------|
| VIIRS A2 | 5,080 HDF5 | pixel-level | 0 | — |
| VIIRS A1 | 5,104 HDF5 | pixel-level | 0 | — |
| CBS Energy | 1 CSV | 166 monthly rows | 0 | 0% |
| CBS GDP | 1 CSV | 102 quarterly rows | 0 | 0% |
| CBS CPI | 1 CSV | 266 monthly rows | 0 | 0% |
| CBS GEP | 1 CSV | 204 monthly rows | 0 | 0% |
| ENTSO-E | 9 XLSX | 175,296 hourly | 0 | 0% |
| KNMI | 97,765 NetCDF | 97,765 hourly | 0 | 0.11% (temp) |
| KNMI Validated | 125,878 NetCDF | 125,878 hourly | 0 | 0% |

### 2.2 Phase 2 — Aggregation Quality

| Source | Rows | Size (MB) | Null % |
|--------|------|-----------|--------|
| VIIRS A2 selective daily | 5,080 | 0.4 | 0% |
| VIIRS A2 non-selective daily | 5,080 | 0.2 | 0% |
| VIIRS A1 daily | 5,102 | 0.3 | 0% |
| CBS combined | 363 × 72 cols | 0.1 | varies by series |
| ENTSO-E | 175,296 | 2.0 | 0% |
| KNMI | 97,765 | 3.3 | 0.08–0.15% |
| KNMI Validated | 125,878 | 3.9 | 0% |

### 2.3 Phase 3 — Final Merged Dataset Nulls

Key columns with non-trivial null rates in the final hourly dataset:

| Column | Null % | Null Rows | Reason |
|--------|--------|-----------|--------|
| `entsoe_load_mw` | 0.01% | 13 | DST transition gaps |
| `ntl_a2_mean` | 45.35% | 55,656 | Daily → hourly broadcast; 10% minimum coverage filter removes low-quality days (54.7% hourly coverage) |
| `ntl_a2_all_mean` | 2.52% | 3,096 | Daily → hourly broadcast; matches satellite orbit gaps only |
| `ntl_a1_mean` | 3.60% | 4,416 | 10% coverage filter removes a small fraction of heavily clouded days |
| `cbs_gas_total_tax` | 42.86% | 52,608 | Series starts 2019 (semi-annual) |
| `cbs_elec_total_tax` | 42.86% | 52,608 | Series starts 2019 (semi-annual) |
| `knmi_temp_c` | 22.72% | 27,888 | Non-validated starts 2015-01 |
| `knmi_val_temp_c` | 0.02% | 26 | 26 hours outside validated range |

> [!NOTE]
> The KNMI non-validated columns have ~23% nulls because the dataset only starts from 2015-01-31, while the hourly spine begins 2012-01-01. The validated dataset covers the full 2012–2025 range with near-zero nulls.

---

## 3. Gap Analysis

### 3.1 KNMI Non-Validated

| Metric | Value |
|--------|-------|
| **Range** | 2015-01-31 → 2026-04-27 |
| **Expected hours** | 98,520 |
| **Missing hours** | 755 (0.77%) |
| **Gap ranges** | 34 |

Major gaps are concentrated in 2015–2016 (83% of all missing hours). The largest is a 10-day gap from 2015-04-01 to 2015-04-10 (240 hours). These are genuine upstream data absences from the KNMI API.

### 3.2 KNMI Validated

| Metric | Value |
|--------|-------|
| **Range** | 2012-01-01 → 2026-05-12 |
| **Total files** | 125,878 |
| **Extracted records** | 125,878 |
| **Corrupt files** | 0 |

> [!NOTE]
> 2 files were initially corrupt (0-byte / truncated HDF5) after the bulk download: `20190819-18.nc` and `20221103-01.nc`. Both were re-downloaded, verified, and successfully re-extracted. The dataset now has **0 errors and full coverage**.

### 3.3 VIIRS A1 (At-Sensor Radiance)

| Metric | Value |
|--------|-------|
| **Range** | 2012-01-19 → 2026-04-19 |
| **Missing days** | 101 (1.94%) |

Notable gaps: satellite maintenance (2018-02, 12 days), satellite anomaly (2019-04, 17 days), known outage (2022-07, 15 days).

### 3.4 VIIRS A2 (Gap-Filled Composite)

| Metric | Value |
|--------|-------|
| **Range** | 2012-01-19 → 2026-04-08 |
| **Missing days** | 114 (2.19%) |

A2 ends 11 days earlier than A1 (derived product processing lag). 71 days exist in A1 but not A2; 47 days exist in A2 but not A1.

---

## 4. ENTSO-E Electricity Load

### 4.1 Time Series Overview

![ENTSO-E time series](figures/entsoe_01_timeseries.png)

### 4.2 Annual Statistics

| Year | Mean (MW) | Std (MW) | Year | Mean (MW) | Std (MW) |
|------|-----------|----------|------|-----------|----------|
| 2012 | 12,268 | 2,366 | 2019 | 13,005 | 1,935 |
| 2013 | 13,009 | 2,450 | 2020 | 12,312 | 1,899 |
| 2014 | 12,707 | 2,350 | 2021 | 12,145 | 1,847 |
| 2015 | 12,929 | 2,075 | 2022 | 11,460 | 1,972 |
| 2016 | 13,039 | 2,076 | 2023 | 12,466 | 1,862 |
| 2017 | 13,130 | 2,082 | 2024 | 13,097 | 1,963 |
| 2018 | 13,302 | 1,944 | 2025 | 13,345 | 2,018 |

> [!IMPORTANT]
> 2022 shows a clear demand trough (mean 11,460 MW) — consistent with the European energy crisis driving conservation and high prices. Demand has recovered by 2024–2025.

### 4.3 STL Decomposition

![ENTSO-E STL decomposition](figures/entsoe_02_stl.png)

| Component | Variance Explained |
|-----------|-------------------|
| **Seasonal** | **58.0%** |
| **Trend** | 31.7% |
| **Residual** | 7.3% |

Electricity demand is **strongly seasonal**, with a dominant 24-hour diurnal cycle. The trend component captures long-term shifts (energy crisis dip, post-crisis recovery).

### 4.4 Autocorrelation

![ENTSO-E ACF/PACF](figures/entsoe_03_acf_pacf.png)

**Top ACF peaks:**

| Lag (hours) | Correlation | Interpretation |
|-------------|-------------|----------------|
| 1 | 0.961 | Adjacent-hour persistence |
| 168 | 0.924 | **Weekly cycle** (7 × 24h) |
| 24 | 0.842 | **Daily cycle** |
| 144 | 0.810 | 6-day lag |

The strong 168h (weekly) autocorrelation confirms a robust day-of-week pattern.

### 4.5 Seasonal Sub-Series

![ENTSO-E sub-series](figures/entsoe_04_subseries.png)

**Hourly profile (median MW):**
- Trough: 02:00 UTC (10,345 MW) — nighttime minimum
- Morning ramp: 05:00–08:00 (12,262 → 14,100 MW)
- Plateau: 08:00–18:00 (~14,000 MW)
- Evening peak: 17:00 (14,106 MW)
- Evening decline: 18:00–23:00

**Day-of-week profile (median MW):**
- Weekend low: Sunday 11,145 MW
- Weekday high: Thursday 13,346 MW
- Saturday intermediate: 11,704 MW

**Monthly profile (median MW):**
- Winter peak: January 14,039 MW
- Summer trough: May 11,748 MW

---

## 5. VIIRS Nighttime Light

### 5.1 VIIRS A2 (Gap-Filled Composite)

![VIIRS A2 time series](figures/viirs_a2_01_timeseries.png)

| Statistic | `ntl_mean` | `ntl_sum` |
|-----------|------------|----------|
| Mean | 7.68 nW/cm²/sr | 1,190,484 |
| Std deviation | 4.16 | 788,287 |
| Min | 0.94 | 30,909 |
| Max | 37.71 | 3,186,378 |
| Valid pixel % (mean) | 31.17% | — |

> [!NOTE]
> **10% minimum coverage filter applied.** Days where the contributing pixel count (quality ≤ 1 for selective A2) is below 10% of the 285,719 NL pixels have `ntl_mean` and `ntl_sum` set to null. This reduced A2 selective hourly coverage from 73.8% to 54.7% but removed unreliable statistics from nights with extreme spatial selection bias (e.g., ≤28,571 valid pixels out of 285,719).

![VIIRS A2 STL](figures/viirs_a2_02_stl.png)

| Component | Variance % |
|-----------|-----------|
| Seasonal | 51.0% |
| Residual | 41.0% |
| Trend | 7.8% |

The A2 product is heavily seasonal-dominated (51.0%), reflecting the survivorship bias discussed in §5.5.4 — the seasonal cycle of cloud masking imprints a strong artificial seasonal pattern. After the 10% coverage filter removed the noisiest low-coverage days, the trend component increased from negligible (1.1% pre-filter) to 7.8%, suggesting the filter exposed a cleaner long-run signal.

![VIIRS A2 ACF/PACF](figures/viirs_a2_03_acf_pacf.png)

![VIIRS A2 sub-series](figures/viirs_a2_04_subseries.png)

### 5.2 VIIRS A1 (At-Sensor Radiance)

![VIIRS A1 time series](figures/viirs_a1_01_timeseries.png)

| Statistic | `ntl_mean` | `ntl_sum` |
|-----------|------------|----------|
| Mean | 6.88 nW/cm²/sr | 1,919,982 |
| Std deviation | 3.59 | 1,038,732 |
| Min | 0.35 | 25,910 |
| Max | 30.51 | 8,717,507 |
| Valid pixel % (mean) | 96.18% | — |

A1 sum has a much larger range (25K–8.7M vs A2’s 31K–3.2M) because A1’s permissive quality filter admits nearly all NL pixels (96.18%), resulting in consistently high pixel counts that amplify the sum.

![VIIRS A1 STL](figures/viirs_a1_02_stl.png)

| Component | Variance % |
|-----------|-----------|
| Residual | 63.5% |
| Seasonal | 34.8% |
| Trend | 1.5% |

A1 has high residual variance (63.5%) — expected for raw at-sensor radiance before gap-filling.

![VIIRS A1 ACF/PACF](figures/viirs_a1_03_acf_pacf.png)

![VIIRS A1 sub-series](figures/viirs_a1_04_subseries.png)

### 5.3 Spatial Filtering — Netherlands GADM Polygon Mask

Both VNP46A1 and VNP46A2 products are spatially filtered to the Netherlands in a **three-stage process**: tile selection at download time, a fast bounding-box crop, and a precise GADM polygon mask during Phase 1 extraction.

#### Stage 1: MODIS/VIIRS Sinusoidal Tile Selection (Download)

VIIRS Black Marble products are distributed on the MODIS sinusoidal grid, where the Earth is divided into ~10°×10° tiles identified by horizontal (h) and vertical (v) indices. The download script ([download_nl_viirs.py](../src/download/download_nl_viirs.py), line 77) requests only tile **h18v03**, which covers a swath of Northern Europe including the entire Netherlands, Belgium, parts of Germany, the southern North Sea, and small parts of the UK and France.

```
TARGET_TILE = "h18v03"
```

Each h18v03 HDF5 file contains a 2400×2400 pixel grid at ~500m resolution in the sinusoidal projection, with embedded `lat` and `lon` datasets that provide the WGS84 coordinates for each pixel. Only files matching this tile ID are downloaded from the LAADS DAAC archive.

#### Stage 2: WGS84 Bounding Box Crop (Fast First Pass)

The h18v03 tile is first cropped to a rectangular bounding box in WGS84 coordinates ([phase1_extract.py](../src/pipeline/phase1_extract.py), lines 72–76):

| Bound | Value |
|-------|-------|
| Latitude south | **50.75°N** |
| Latitude north | **53.55°N** |
| Longitude west | **3.35°E** |
| Longitude east | **7.25°E** |

This reduces the 2400×2400 tile to a 672×937 subgrid (**629,664 candidate pixels**). This step is purely for I/O efficiency — it limits the HDF5 slice and reduces the number of pixels that need polygon testing.

#### Stage 3: GADM Polygon Mask (Precise)

A GADM 4.1 Netherlands Level 0 boundary polygon ([`data/geo/gadm41_NLD_0.json`](../data/geo/gadm41_NLD_0.json)) is used to identify which bounding-box pixels fall inside Dutch land territory and islands. The mask is computed using Shapely's vectorised `contains_xy()` function:

```python
from shapely import contains_xy
from shapely.geometry import shape

nl_geom = shape(geojson["features"][0]["geometry"])
in_nl = contains_xy(nl_geom, lon_grid.ravel(), lat_grid.ravel())
```

| Property | Value |
|----------|-------|
| Source | GADM 4.1 Level 0 (Netherlands) |
| CRS | WGS84 (EPSG:4326) |
| Geometry type | MultiPolygon (16 polygons: mainland + islands) |
| Total vertices | 2,043 |
| Computation time | ~0.3 s (fully vectorised, Shapely 2.0+) |
| Bounding-box pixels | 629,664 |
| **Pixels inside NL polygon** | **285,719 (45.4%)** |
| Pixels excluded | 343,945 (54.6%) |

The mask is computed **once** at startup and cached across all worker processes (shared via fork-inheritance). Each worker applies the mask as a NumPy boolean index after the HDF5 bounding-box slice, so only Dutch-territory pixels are written to Parquet.

**Excluded pixel types:**
- **North Sea** — open water west of the coastline (near-zero NTL radiance)
- **Belgian border** — slivers south of Limburg
- **German border** — slivers east of Drenthe, Overijssel, and Groningen
- **Inland waterways** that fall outside the GADM land polygon

> [!NOTE]
> **EEZ and offshore platforms.** The GADM Level 0 boundary covers Dutch **land territory and islands** (including the Wadden Islands), but excludes the Netherlands' Exclusive Economic Zone (EEZ) in the North Sea. The Dutch EEZ extends to ~2.54°E longitude — west of our bounding box (3.35°E) — so most of it is already excluded by Stage 2. Some near-shore offshore platforms (~3.5°E+) may emit NTL, but their contribution to the national spatial mean is negligible.
>
> ENTSO-E electricity load data for the Netherlands covers the Dutch transmission system (TenneT's bidding zone), which includes offshore wind farms connected to the national grid. The NTL spatial scope (land only) and ENTSO-E scope (bidding zone including offshore) are therefore **not perfectly aligned**, but this mismatch is minor for load forecasting purposes.

> [!TIP]
> The bounding box coordinates and GADM polygon path are configurable in `phase1_extract.py`. To change the spatial scope (e.g., Benelux region, specific provinces), replace `gadm41_NLD_0.json` with a different boundary file and adjust the bounding-box constants if needed. GADM also provides Level 1 (provinces) and Level 2 (municipalities) boundaries.

### 5.4 A1 vs A2 Comparison

![VIIRS A1 vs A2](figures/viirs_a1a2_comparison.png)

| Metric | Value |
|--------|-------|
| A1/A2 ratio mean | 1.24 |
| A1/A2 ratio median | 0.95 |
| A1/A2 ratio std | 1.01 |

The 10% coverage filter has further tightened the A1/A2 ratio (std 1.01 vs 4.78 before the filter, vs 85.32 before polygon masking). With the noisiest low-coverage A2 days removed, the ratio is now well-behaved with mean ≈ 1.24 and median ≈ 0.95.

### 5.5 Valid Pixel Analysis — Cloud Seasonality & Measurement Bias

#### 5.5.1 How valid pixels are defined

A pixel is counted as **valid** in Phase 2 aggregation ([phase2_aggregate.py](../src/pipeline/phase2_aggregate.py), lines 216–218) when two conditions are both met:

1. `is_fill == False` — the raw HDF5 value is not the fill sentinel −999.9 (genuine "no observation").
2. `quality_flag <= 1` — quality tier is either 0 (best/clear-sky) or 1 (good/fair).

The total pixel pool is fixed by the GADM Netherlands polygon mask (see §5.3), which retains exactly **285,719 pixels per granule** from the h18v03 sinusoidal tile after excluding non-Dutch territory (North Sea, border regions).

The two products encode quality differently:

| Product | HDF5 dataset | Encoding | Threshold |
|---------|-------------|----------|-----------|
| **A1** (`VNP46A1`) | `QF_DNB` (uint16 bitmask) | Bits 0–1 extracted to uint8: 0=best, 1=fair, 2=poor, 3=no retrieval | `bits 0–1 <= 1` |
| **A2** (`VNP46A2`) | `Mandatory_Quality_Flag` (uint8) | 0=best (clear-sky), 1=good (additional QA applied), 2=degraded (cloud/smoke/fire/dust), 3=no retrieval, 255=fill | `flag <= 1` |

The **same numeric threshold (`<= 1`)** is applied to both, but the underlying semantics differ: A2's flag 2 explicitly encodes cloud contamination, whereas A1's QF_DNB tier 1 ("fair") still admits many partially compromised pixels.

#### 5.5.2 Why A2 has so many invalid pixels

The Netherlands (~52°N) has one of Europe's highest cloud cover rates — the sky is overcast on the majority of days in all seasons. This disproportionately affects A2 because:

- **A2 cloud masking is strict.** `Mandatory_Quality_Flag` values 2 and 3 cover all cloud-contaminated and non-retrieved pixels. Over the Netherlands, the overwhelming majority of nighttime pixels fall into these categories on any given night.
- **A1 is more permissive.** The raw `QF_DNB` bitmask uses a coarser 2-bit tier that admits more lightly contaminated pixels as "fair" (tier 1). Some cloud-edge pixels that A2 flags as quality 2 pass A1's filter as quality 1.
- **A2 gap-filling ≠ valid pixels.** Despite being the "gap-filled" product, A2's gap-filled pixels (drawn from historical composites when the current night is cloud-covered) are assigned `quality_flag 2+` because they are *imputed*, not directly observed. Our `quality_flag <= 1` filter therefore **excludes all imputed A2 pixels**, so the "gap-filled" label is misleading in the context of our valid-pixel count.

Net result: **A1 passes 96.18% of pixels as valid; A2 passes only 31.17%.** The remaining 68.83% of non-fill A2 pixels are imputed from historical composites. By month, the imputed fraction is:

| Jan | Feb | Mar | Apr | May | Jun | Jul | Aug | Sep | Oct | Nov | Dec |
|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|
| 63.2% | 56.7% | 51.2% | 59.9% | 95.8% | **100%** | 99.5% | 69.0% | **45.2%** | 57.8% | 61.0% | 67.4% |

June is 100% imputed — not a single directly observed A2 pixel exists across the 14-year record. September has the lowest imputed fraction (45.2%) due to autumn high-pressure blocking events.

A2 also occasionally reports ~571,438 pixels per day (double the standard 285,719), reflecting two satellite overpasses composited into one file on certain nights. This does not increase valid pixel counts significantly.

#### 5.5.3 Seasonal pattern of invalid pixels

Valid pixel coverage shows a pronounced seasonal cycle that tracks NL cloud climatology:

**A2 selective monthly mean NTL radiance (nW/cm²/sr):**

| Jan | Feb | Mar | Apr | May | Jun | Jul | Aug | Sep | Oct | Nov | Dec |
|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|
| 8.15 | 8.20 | 7.56 | 7.56 | 7.04 | — | 6.22 | 7.24 | 7.85 | 7.75 | 7.42 | 7.48 |

**June has no A2-selective subseries entry at all** — every June in the 14-year record produced zero directly observed pixels with ≥10% coverage. The 10% filter also removes many May and July nights, tightening the summer gap further.

**A2 selective monthly sum NTL (nW/cm²/sr × pixels):**

| Jan | Feb | Mar | Apr | May | Jun | Jul | Aug | Sep | Oct | Nov | Dec |
|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|
| 1,170,562 | 1,362,500 | 1,350,959 | 1,176,344 | 623,634 | — | 268,632 | 933,867 | 1,373,237 | 1,231,077 | 1,096,149 | 1,047,401 |

The sum shows a much stronger seasonal pattern than the mean, because it captures both radiance intensity and spatial coverage. Winter months have 4–5× the sum of July, reflecting both higher valid pixel counts and higher per-pixel radiance.

**A2 non-selective monthly mean NTL radiance (nW/cm²/sr):**

| Jan | Feb | Mar | Apr | May | Jun | Jul | Aug | Sep | Oct | Nov | Dec |
|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|
| 6.42 | 6.60 | 6.66 | 6.35 | 5.62 | 5.10 | 4.94 | 5.61 | 6.66 | 6.52 | 6.27 | 6.16 |

Including imputed pixels restores June (5.10 nW/cm²/sr) and eliminates the spurious summer peak. The seasonal pattern shows a winter peak (Feb–Mar: 6.60–6.66) and a summer trough (Jul: 4.94).

**A1 monthly mean NTL radiance (nW/cm²/sr):**

| Jan | Feb | Mar | Apr | May | Jun | Jul | Aug | Sep | Oct | Nov | Dec |
|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|
| 8.31 | 8.14 | 7.03 | 6.28 | 5.73 | 5.41 | 5.18 | 6.09 | 6.95 | 7.44 | 7.60 | 8.09 |

A1 includes June (5.41 nW/cm²/sr) due to its permissive quality filter (96.18% valid). Its clear winter peak (Jan: 8.31) reflects longer nights at 52°N.

**A1 monthly sum NTL (nW/cm²/sr × pixels):**

| Jan | Feb | Mar | Apr | May | Jun | Jul | Aug | Sep | Oct | Nov | Dec |
|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|
| 2,374,320 | 2,324,323 | 2,008,691 | 1,793,874 | 1,556,164 | 1,174,997 | 1,319,182 | 1,739,984 | 1,986,396 | 2,126,691 | 2,170,667 | 2,312,405 |

The A1 sum shows a clean monotonic seasonal pattern (Jan peak: 2.37M, Jun trough: 1.17M) with no missing months. This makes `ntl_a1_sum` the most interpretable NTL feature for seasonal analysis.

The general seasonal pattern of **invalid pixels** is:
- **Summer (Jun–Aug)**: fewest valid pixels — convective clouds, short nights, high water vapour.
- **Winter (Dec–Feb)**: more valid pixels relative to summer — stratiform/frontal cloud systems pass more frequently, and nights are long (~16.5 h at 52°N).
- **Spring/autumn shoulder seasons** show intermediate cloud cover.

#### 5.5.4 Why A2 peaks in summer while A1 peaks in winter — and what this means for the correlation with ENTSO-E

**A1 peaks in winter (Jan: 8.31, Jul: 5.18 nW/cm²/sr trough) and correlates with ENTSO-E load.**

Mean: r = +0.108 | Sum: r = +0.121 — the sum is the stronger predictor because it captures both radiance intensity and spatial coverage (proportional to the fraction of NL sky that is cloud-free).

**A2-selective peaks in winter (Dec: 7.48, Feb: 8.20) with the polygon mask and 10% coverage filter, but still shows artifacts from survivorship bias and is weakly correlated with ENTSO-E load (mean: r = +0.037, sum: r = +0.008).**

This is a **survivorship bias artefact** from strict cloud quality filtering, operating through two mechanisms:

1. **Extreme sample selection in summer.** In July and August, the vast majority of nights are overcast. The tiny fraction of clear summer nights that pass `quality_flag <= 1` are drawn from anticyclonic high-pressure events: dry, clean atmosphere, minimal aerosol scattering, optimal satellite viewing geometry. These conditions produce intrinsically higher apparent radiance than the more typical hazy/humid/partly-cloudy nights that dominate the broader statistical population. In winter, cloud cover is also high but more nights are partly clear, so the valid sample is larger, more representative, and the mean is pulled toward typical conditions.

2. **Night-length suppression of winter A2 values.** In winter, nights are long but cloud cover is also high. On a partly-cloudy winter night, fewer contiguous clear-sky pixels accumulate (frontal bands may cover large areas), and the valid A2 observations that do exist may not represent the full night's emission — they represent brief clear windows. This further suppresses winter A2 values relative to what might be expected from first principles.

**Consequence for selective A2:** Its seasonal cycle is dominated by cloud-sampling artifacts rather than actual variation in ground-level nighttime light emission. It cannot serve as a reliable proxy for electricity demand. `ntl_a2_mean` should be used with extreme caution or excluded from models targeting seasonal patterns.

**The non-selective A2 (`ntl_a2_all_mean`) partially resolves this artifact.** By including gap-filled imputed pixels (quality_flag 2–3), it provides values for all days including June (where selective A2 has zero coverage), and achieves 97.5% hourly coverage — matching A1 at 96.4%. Including imputed pixels recovers a substantial load correlation: A2-all mean has r = 0.112 vs load — nearly matching A1 mean (r = 0.108). More strikingly, A2-all sum also achieves r = 0.112, while A1 sum leads at r = 0.121. The A2-all STL decomposition is more interpretable (trend 39.5%, seasonal 33.0%, residual 27.9%) than selective A2 (trend 7.8%, seasonal 51.0%, residual 41.0%), confirming that the 10% coverage filter plus imputation produces a cleaner signal.

> [!IMPORTANT]
> The apparent A2-selective "summer peak" is **not physical** — actual nighttime light emission in the Netherlands does not increase in summer. It is entirely an artifact of selection bias from strict cloud masking combined with short summer nights. Any model that ingests `ntl_a2_mean` as a raw feature will learn this spurious summer-peaking pattern, potentially confounding the real energy demand signal. Use `ntl_a2_all_mean` or `ntl_a1_mean` instead.

### 5.6 VIIRS A2 Non-Selective (Including Imputed Pixels)

The non-selective A2 variant (`viirs_a2_all_daily`) includes all non-fill A2 pixels in the daily mean — both directly observed (quality 0–1) and gap-filled/imputed (quality 2–3). This eliminates the survivorship bias of the selective version at the cost of mixing observed and synthetic pixel values.

#### 5.6.1 Time Series and STL

![VIIRS A2-all time series](figures/viirs_a2_all_01_timeseries.png)

| Statistic | `ntl_mean` | `ntl_sum` |
|-----------|------------|----------|
| Mean | 6.08 nW/cm²/sr | 1,737,340 |
| Std deviation | 1.44 | 410,318 |
| Min | 3.21 | 915,715 |
| Max | 11.17 | 3,192,627 |
| Valid pixel % (mean) | 31.17% | — |
| Imputed pixel % (mean) | 68.83% | — |
| Hourly coverage | 97.5% | — |

The lower mean (6.08 vs 7.68 for selective) and much smaller standard deviation (1.44 vs 4.16) reflect the smoothing effect of including imputed composite values. Extreme outliers (max 37.71 in selective) are absent because imputed pixels are constrained to climatological composites. The sum ranges are much tighter (916K–3.2M vs 31K–3.2M for selective) because nearly all non-fill pixels contribute consistently.

![VIIRS A2-all STL](figures/viirs_a2_all_02_stl.png)

| Component | Variance % |
|-----------|-----------|
| **Trend** | **39.5%** |
| Seasonal | 33.0% |
| **Residual** | 27.9% |

The STL variance balance is markedly different from selective A2 (trend 7.8%, seasonal 51.0%, residual 41.0%). Non-selective A2 is more trend-dominated, indicating the imputed pixels absorb much of the spurious seasonal variance while exposing real long-run trends.

#### 5.6.2 ACF / PACF and Sub-Series

![VIIRS A2-all ACF/PACF](figures/viirs_a2_all_03_acf_pacf.png)

![VIIRS A2-all sub-series](figures/viirs_a2_all_04_subseries.png)

**Monthly mean radiance (nW/cm²/sr):**

| Jan | Feb | Mar | Apr | May | Jun | Jul | Aug | Sep | Oct | Nov | Dec |
|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|
| 6.42 | 6.60 | 6.66 | 6.35 | 5.62 | 5.10 | 4.94 | 5.61 | 6.66 | 6.52 | 6.27 | 6.16 |

**Monthly sum NTL (nW/cm²/sr × pixels):**

| Jan | Feb | Mar | Apr | May | Jun | Jul | Aug | Sep | Oct | Nov | Dec |
|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|
| 1,835,320 | 1,886,163 | 1,902,434 | 1,815,356 | 1,605,073 | 1,455,840 | 1,410,547 | 1,603,603 | 1,901,614 | 1,862,853 | 1,791,985 | 1,759,045 |

The seasonal structure is physically plausible: a winter peak (Feb–Mar: 6.60–6.66), a genuine summer trough (Jun–Jul: 5.10–4.94), and a secondary autumn peak (Sep: 6.66). The sum is much more stable than the selective A2 sum (range 1.41M–1.90M vs 269K–1.37M), reflecting the consistent pixel counts from including imputed values.

#### 5.6.3 Selective vs Non-Selective Comparison

![A2 selective vs all comparison](figures/viirs_a2_selective_vs_all.png)

![A2 imputed fraction by month](figures/viirs_a2_imputed_fraction_monthly.png)

**Key differences between the two A2 variants:**

| Metric | A2 Selective | A2 Non-Selective |
|--------|-------------|-----------------|
| Hourly coverage | 54.7% | 97.5% |
| Mean radiance | 7.68 nW/cm²/sr | 6.08 nW/cm²/sr |
| Std deviation | 4.16 | 1.44 |
| Sum (mean) | 1,190,484 | 1,737,340 |
| STL seasonal % | 51.0% | 33.0% |
| STL trend % | 7.8% | 39.5% |
| STL residual % | 41.0% | 27.9% |
| NTL mean–load r | +0.037 | +0.112 |
| NTL sum–load r | +0.008 | +0.112 |
| June available | No | Yes |

**Coverage and correlation comparison with A1:**

| Product | Coverage | Mean–load r | Sum–load r |
|---------|----------|-------------|------------|
| VIIRS A1 | 96.4% | +0.108 | **+0.121** |
| VIIRS A2 non-selective | 97.5% | +0.112 | +0.112 |
| VIIRS A2 selective | 54.7% | +0.037 | +0.008 |

#### 5.6.4 Implications for Modeling

The 10% coverage filter reveals a clearer picture of NTL–load relationships:

- **`ntl_a1_sum` is the strongest NTL predictor** (r = +0.121), outperforming `ntl_a1_mean` (r = +0.108). The sum captures both radiance intensity and spatial cloud-free coverage, both of which correlate with nighttime activity and electricity demand.
- **A2-all now matches A1 mean** in correlation strength (r = 0.112 vs 0.108), a significant improvement from the pre-filter values. This suggests the filter removed the noisiest observations that were diluting A2-all’s signal.
- **A2 selective sum is nearly useless** (r = +0.008) because the highly variable valid pixel count dominates the sum signal, introducing noise rather than information.

The non-selective A2 is useful as:
- A **gap-free control** for ablation experiments comparing direct vs imputed NTL
- A **smooth activity proxy** when month-level or annual economic trends are the target (trend component is 39.5%)
- A **bias check** — large divergence between selective and non-selective A2 on a given day flags nights where the valid-pixel sample was so small that survivorship bias likely dominated

For short-term load forecasting, **`ntl_a1_sum` is the recommended primary NTL feature**, followed by `ntl_a2_all_mean` or `ntl_a2_all_sum` as alternatives.

---

## 6. CBS Economic Indicators

### 6.1 Time Series

![CBS time series](figures/cbs_01_timeseries.png)

### 6.2 STL Decomposition

````carousel
![CBS CPI Energy STL](figures/cbs_02_stl_cpi_energy.png)
<!-- slide -->
![CBS GEP Gas HH Total STL](figures/cbs_02_stl_gep_gas_hh_total.png)
<!-- slide -->
![CBS GDP YoY STL](figures/cbs_02_stl_gdp_yy.png)
````

| Indicator | Trend % | Seasonal % | Residual % |
|-----------|---------|------------|------------|
| CPI Energy | **86.0%** | 1.7% | 5.2% |
| GEP Gas HH Total | **90.9%** | 1.0% | 2.3% |
| GDP YoY | **55.8%** | 1.8% | 16.7% |

All CBS indicators are **trend-dominated** — they capture slow macroeconomic shifts rather than short-term seasonality.

### 6.3 Autocorrelation

![CBS ACF/PACF](figures/cbs_03_acf_pacf.png)

Key ACF at lag 1: CPI Energy 0.970, GEP Gas 0.978, GDP YoY 0.919, Consumption HH 0.896 — all extremely persistent series.

### 6.4 Seasonal Sub-Series

![CBS sub-series](figures/cbs_04_subseries.png)

---

## 7. KNMI Meteorological Observations

### 7.1 Non-Validated

![KNMI time series](figures/knmi_01_timeseries.png)

**8 variables extracted**, station-averaged across ~61 Dutch weather stations:

| Variable | Column | Description |
|----------|--------|-------------|
| Temperature | `knmi_temp_c` | 1.5m air temperature (°C) |
| Dew point | `knmi_dewpoint_c` | Dew point temperature (°C) |
| Wind speed (10-min) | `knmi_wind_speed_ms` | 10-min mean wind speed (m/s) |
| Wind speed (hourly) | `knmi_wind_speed_hourly_ms` | Hourly mean wind speed (m/s) |
| Wind gust | `knmi_wind_gust_ms` | Maximum wind gust (m/s) |
| Solar radiation | `knmi_solar_rad_jcm2` | Global solar radiation (J/cm²) |
| Sunshine | `knmi_sunshine_h` | Sunshine duration (h) |
| Humidity | `knmi_humidity_pct` | Relative humidity (%) |

#### STL Decomposition (Temperature)

![KNMI STL temperature](figures/knmi_02_stl_temp.png)

| Component | Variance % |
|-----------|-----------|
| Trend | **89.6%** |
| Seasonal | 7.8% |
| Residual | 1.1% |

Temperature is overwhelmingly trend-dominated (annual cycle captured as "trend" by the STL period).

#### ACF / PACF

![KNMI ACF/PACF](figures/knmi_03_acf_pacf.png)

Temperature ACF at lag 24h = 0.934 — very strong daily persistence.

#### Seasonal Sub-Series

![KNMI sub-series](figures/knmi_04_subseries.png)

**Hourly temperature profile:**
- Minimum: 04:00 UTC (10.22°C mean)
- Maximum: 13:00 UTC (13.98°C mean)
- Diurnal range: ~3.8°C

**Monthly temperature profile:**
- Coldest: January (5.78°C)
- Warmest: August (18.82°C)

#### Weather–Load Relationship

![KNMI weather vs load](figures/knmi_05_weather_load.png)

| Weather Variable | Pearson r vs Load |
|-----------------|-------------------|
| Temperature | **−0.207** |
| Wind speed | **+0.162** |
| Humidity | +0.040 |
| Solar radiation | −0.009 |

Temperature shows the strongest weather–load relationship: **colder → higher demand** (heating load). Wind speed has a positive correlation, likely reflecting wind chill increasing heating demand.

### 7.2 Validated

![KNMI Validated time series](figures/knmi_val_01_timeseries.png)

![KNMI Validated STL](figures/knmi_val_02_stl_temp.png)

| Component | Variance % |
|-----------|-----------|
| Trend | **90.2%** |
| Seasonal | 7.1% |
| Residual | 1.1% |

![KNMI Validated ACF/PACF](figures/knmi_val_03_acf_pacf.png)

![KNMI Validated sub-series](figures/knmi_val_04_subseries.png)

![KNMI Validated weather vs load](figures/knmi_val_05_weather_load.png)

| Weather Variable | Pearson r vs Load |
|-----------------|-------------------|
| Temperature | **−0.178** |
| Wind speed | **+0.160** |
| Solar radiation | +0.028 |
| Humidity | −0.016 |

---

## 8. KNMI Validated vs Non-Validated Comparison

![Temperature comparison](figures/knmi_val_vs_nonval_01_temp.png)

![Correlation comparison](figures/knmi_val_vs_nonval_02_correlation.png)

### 8.1 Overlap Statistics

| Metric | Value |
|--------|-------|
| Overlapping hours | 97,765 |
| Mean temp difference (val − non-val) | **−0.81°C** |
| Std of temp difference | 0.44°C |
| Max absolute difference | 3.37°C |

### 8.2 Pearson Correlations (Validated vs Non-Validated)

| Variable | r |
|----------|---|
| Temperature | **0.9978** |
| Dew point | 0.9974 |
| Wind speed | **0.9992** |
| Solar radiation | 0.9916 |
| Humidity | 0.9989 |

> [!IMPORTANT]
> All correlations exceed 0.99 — the validated and non-validated datasets are nearly identical in variability. The systematic −0.81°C offset in temperature is likely due to different QC procedures (validated data undergoes additional bias correction). For modeling, the validated dataset is preferred due to its longer coverage (2012–2026 vs 2015–2026) and zero internal nulls.

---

## 9. Cross-Variable Analysis

### 9.1 Correlation Heatmap

![Correlation heatmap](figures/multi_01_correlation_heatmap.png)

**Strongest correlations with `entsoe_load_mw`:**

| Feature | Pearson r | Direction |
|---------|-----------|-----------|
| `knmi_temp_c` | −0.207 | Cold → more load |
| `knmi_val_temp_c` | −0.178 | Cold → more load |
| `knmi_wind_speed_ms` | +0.162 | Windy → more load |
| `knmi_val_wind_speed_ms` | +0.160 | Windy → more load |
| `cbs_elec_total_tax` | +0.131 | Higher tax → more load |
| `cbs_cpi_gas` | −0.123 | Higher gas CPI → less load |
| `cbs_cpi_energy` | −0.122 | Higher energy CPI → less load |
| `ntl_a1_sum` | **+0.121** | More total NTL → more load |
| `ntl_a1_mean` | +0.108 | Brighter NTL → more load |
| `ntl_a2_mean` (selective) | +0.037 | Weak; survivorship bias |
| `ntl_a2_sum` (selective) | +0.008 | Nearly uncorrelated; variable pixel count dominates |

**Notable inter-feature correlations:**
- `cbs_cpi_energy` ↔ `cbs_cpi_gas`: r = 0.990 (near-perfect collinearity)
- `cbs_gdp_yy` ↔ `cbs_consumption_hh_yy`: r = 0.925
- `cbs_gas_total_tax` ↔ `cbs_population_million`: r = 0.973
- `knmi_temp_c` ↔ `knmi_val_temp_c`: r = 0.998
- `knmi_humidity_pct` ↔ `knmi_solar_rad_jcm2`: r = −0.716

### 9.2 Variance Inflation Factors (VIF)

![VIF chart](figures/multi_02_vif.png)

| Feature | VIF | Concern |
|---------|-----|---------|
| `ntl_a1_mean` | 22.3B | 🔴 **Extreme** (collinear with `ntl_a1_sum`) |
| `ntl_a1_sum` | 22.3B | 🔴 **Extreme** (collinear with `ntl_a1_mean`) |
| `knmi_humidity_pct` | 56,939 | 🔴 Extreme |
| `knmi_val_humidity_pct` | 50,845 | 🔴 Extreme |
| `cbs_cpi_energy` | 28,166 | 🔴 Extreme |
| `knmi_temp_c` | 10,295 | 🔴 Extreme |
| `cbs_cpi_gas` | 9,620 | 🔴 Extreme |
| `knmi_val_temp_c` | 8,145 | 🔴 Extreme |
| `knmi_wind_speed_ms` | 5,708 | 🔴 Extreme |
| `cbs_population_million` | 1,640 | 🔴 Severe |
| `cbs_gep_gas_hh_total` | 675 | 🟡 High |
| `ntl_a2_mean` | 6.4 | ✅ Acceptable |
| `ntl_a2_sum` | 4.7 | ✅ Acceptable |

> [!WARNING]
> **`ntl_a1_mean` and `ntl_a1_sum` are near-perfectly collinear** (VIF ≈ 22 billion). Because A1 has ~96% valid pixels, `ntl_a1_sum ≈ ntl_a1_mean × 274,700` (nearly constant multiplier). **Use only one of the two** in any regression model. For NTL-selective (A2), the VIF is manageable (6.4 and 4.7) because the variable pixel count decorrelates sum and mean.
>
> **Severe multicollinearity detected** in KNMI and CBS features. The validated and non-validated KNMI variables are near-duplicates (r > 0.99). For regression modeling, use **only one** of the two KNMI datasets. Similarly, CBS CPI energy/gas/electricity are highly collinear — consider using only one CPI variant or applying PCA.

### 9.3 Cross-Correlation Functions (CCF)

![CCF chart](figures/multi_03_ccf.png)

**Peak cross-correlations with ENTSO-E load:**

| Feature | Peak Lag (h) | Peak r | r at lag 0 |
|---------|-------------|--------|------------|
| `knmi_solar_rad_jcm2` | −10 | **−0.522** | −0.009 |
| `knmi_val_solar_rad_jcm2` | +158 | **−0.544** | +0.028 |
| `knmi_humidity_pct` | −11 | **+0.415** | +0.040 |
| `knmi_temp_c` | −83 | **−0.410** | −0.207 |
| `knmi_val_temp_c` | −107 | **−0.399** | −0.178 |
| `knmi_wind_speed_ms` | +3 | +0.171 | +0.162 |
| `cbs_elec_total_tax` | +168 | +0.134 | +0.131 |
| `ntl_a1_sum` | +142 | +0.133 | +0.121 |
| `ntl_a1_mean` | +141 | +0.120 | +0.108 |
| `ntl_a2_mean` | +24 | +0.051 | +0.037 |
| `ntl_a2_sum` | −168 | +0.035 | +0.008 |

Solar radiation has the strongest lagged cross-correlation (r = −0.52 at lag −10h), but near-zero at lag 0 due to the diurnal phase offset between peak sunlight and peak load. `ntl_a1_sum` peaks at lag +142h (r = 0.133), slightly stronger than `ntl_a1_mean` at +141h (r = 0.120).

### 9.4 Pairplot

![Pairplot](figures/multi_04_pairplot.png)

---

## 10. Key Findings & Recommendations

### 10.1 Key Findings

1. **Electricity demand is strongly seasonal** — 58% of variance is seasonal (24h diurnal + 168h weekly cycles), with a clear weekday/weekend split and winter/summer pattern.

2. **Temperature is the strongest weather predictor** (r = −0.21 at lag 0, up to −0.41 at optimal lag), confirming the NL heating-driven demand profile.

3. **Solar radiation has strong lagged influence** (r = −0.52 at lag −10h) — daytime solar generation offsets grid demand with a phase delay.

4. **2022 energy crisis is visible** — mean demand dropped to 11,460 MW (lowest since 2012), likely due to conservation behavior and high prices. This constitutes a structural break in the target series.

5. **Validated KNMI data is superior** — 100% spine coverage (vs 77.3% for non-validated), zero internal nulls, and r > 0.99 agreement with non-validated data on overlapping periods.

6. **Severe multicollinearity exists** in the raw feature space — VIF values exceed 60,000 for humidity and temperature pairs.

7. **`ntl_a1_sum` is the strongest NTL predictor** (r = +0.121), outperforming `ntl_a1_mean` (r = +0.108). Both peak in winter (Jan: 8.31 nW/cm²/sr mean, 2.37M sum) driven by longer nights at 52°N. A2-selective is weakly correlated with load (mean: r = +0.037, sum: r = +0.008). June is entirely absent from A2-selective — zero directly observed pixels with ≥10% coverage across the full 14-year record.

8. **10% minimum coverage filter** nulls out NTL mean/sum on days with <28,571 valid pixels (10% of 285,719). This reduced A2 selective coverage from 73.8% to 54.7% but dramatically improved A2-all's load correlation (mean: r = 0.112 vs 0.053 pre-filter). A1 was minimally affected (96.4% vs 97.1%).

9. **A2 non-selective achieves 97.5% hourly coverage** and now matches A1's load correlation strength (mean r = +0.112 vs A1 mean r = +0.108). Including imputed pixels eliminates the spurious summer peak and restores a physically plausible seasonal pattern (winter peak, summer trough). `ntl_a2_all_sum` also achieves r = +0.112.

<!--
### 10.2 Modeling Recommendations

> [!TIP]
> **Feature selection for modeling:**
> - Use **KNMI validated** (`knmi_val_*`) exclusively — drop non-validated columns
> - Choose **one** of: `cbs_cpi_energy`, `cbs_cpi_gas`, `cbs_cpi_electricity` (r > 0.82 pairwise)
> - Drop `cbs_gas_total_tax` (r = 0.97 with `cbs_population_million`)
> - Consider PCA on the CBS feature block to reduce collinearity
> - Include **lag features** for temperature (−83h) and solar radiation (−10h) based on CCF peaks
> - **NTL ranking**: use `ntl_a1_sum` (r = +0.121) as the primary NTL feature; use **either** `ntl_a1_sum` **or** `ntl_a1_mean` (VIF ≈ 22B between them); consider `ntl_a2_all_mean` or `ntl_a2_all_sum` (r = +0.112, 97.5% coverage) as a gap-free alternative; avoid `ntl_a2_mean` and `ntl_a2_sum` (54.7% coverage, survivorship bias)

**Additional recommendations:**

**R7 — NTL valid-pixel quality threshold.** (✅ **Implemented**) A 10% minimum valid-pixel coverage filter is now applied in Phase 2 aggregation. Days where `contributing_pixel_count / 285,719 < 0.10` (fewer than 28,571 pixels) have `ntl_mean` and `ntl_sum` set to null. This reduced A2 selective coverage from 73.8% to 54.7%, but dramatically improved A2-all's load correlation from r = 0.053 to r = 0.112. The filter is applied at the Spark aggregation stage, so all downstream datasets automatically inherit it.

**R8 — Night-length normalisation for A1.** A1's winter peak is partly geometric: longer nights at 52°N yield more photon exposure at the same ground-level emission rate. Dividing `ntl_a1_mean` by astronomical night length (computable from latitude and day-of-year) removes this seasonal geometric bias and leaves a residual that better reflects true activity/emission changes. This would make A1 a cleaner economic proxy.

**R9 — Use NTL sum as the primary feature over mean.** The analysis reveals `ntl_a1_sum` (r = +0.121) outperforms `ntl_a1_mean` (r = +0.108) because the sum captures both radiance intensity and the fraction of NL sky that is cloud-free (a weather proxy itself). However, `ntl_a1_mean` and `ntl_a1_sum` are near-perfectly collinear (VIF ≈ 22B) because A1's valid pixel count is nearly constant (96.18%). **Use only one in any regression model.** For A2 selective, the sum is actively harmful (r = +0.008) due to wild pixel-count variation. For A2-all, sum and mean perform identically (both r = +0.112).

**R10 — Structural break for the 2022 energy crisis.** The 2022 demand trough (mean 11,460 MW, lowest since 2012) is a genuine structural break caused by price-driven industrial curtailment and household conservation. A model trained on 2012–2025 without a crisis indicator will mis-learn the 2022 period. Include a binary `energy_crisis` flag (True for approximately 2022–H1 2023) or use piecewise regression/changepoint detection around this break.

**R11 — Explicit calendar and time features.** The 24h (ACF = 0.84) and 168h (ACF = 0.92) autocorrelations confirm that calendar structure is the dominant signal. Explicitly encoding `hour_of_day`, `day_of_week`, `month_of_year`, `is_public_holiday` (NL calendar), `week_of_year`, and `is_dst_transition` will substantially outperform any model that must learn these from raw timestamps. Dutch public holidays (King's Day, Liberation Day, etc.) produce load dips of ~2,000 MW and are not derivable from date arithmetic alone.

**R12 — Solar radiation phase correction as an explicit feature.** The CCF shows `knmi_val_solar_rad_jcm2` peaks at lag +158h (r = −0.544) with near-zero correlation at lag 0. Creating an explicit `solar_rad_lag_minus_10h` feature (the empirically optimal short lag from the non-validated CCF) captures the phase-delayed grid demand suppression from daytime PV generation without requiring the model to learn the lag internally.

**R13 — Model architecture recommendation.** For this 122k-hour dataset:
- **Primary baseline**: gradient-boosted trees (LightGBM or XGBoost) with all calendar features, lagged targets (at 1h, 24h, 168h), and weather variables. These handle multicollinearity internally via feature selection and are the proven standard for short-term electricity load forecasting.
- **Statistical baseline**: SARIMA(p,d,q)(P,D,Q)[24] with weekly seasonality, as a lower-bound benchmark.
- **Evaluation metric**: MAPE or RMSE on the 2023–2025 hold-out, using walk-forward validation (train 2012–2020, validate 2021–2022, test 2023–2025) to respect the temporal order and correctly reflect the post-crisis regime.

**R14 — NTL as a long-run activity indicator, not a short-run predictor.** The CCF for A1 sum peaks at lag +142h (r = 0.133), not lag 0. A1 mean peaks at +141h (r = 0.120). The information NTL adds is about multi-day or weekly economic activity levels rather than hour-by-hour load. Consider constructing a monthly NTL feature (after night-length normalisation) and using it as an economic activity covariate alongside GDP and CPI, rather than treating it as a daily predictor.
-->

### 10.3 Data Quality Summary

| Dataset | Gap % | Status |
|---------|-------|--------|
| ENTSO-E Load | 0.01% | ✅ Excellent |
| KNMI Validated | 0.00% | ✅ Excellent |
| KNMI Non-Validated | 0.77% | ✅ Healthy |
| VIIRS A1 | 3.60% | ✅ Healthy |
| VIIRS A2 (non-selective) | 2.52%\* | ✅ Healthy |
| VIIRS A2 (selective) | 45.35%\* | ⚠️ Cloud-limited + 10% coverage filter |
| CBS (all series) | 0.00% | ✅ Complete |

\* Reported as hourly null % in the final merged dataset (daily gaps × 24h broadcast).

All gaps are genuine upstream data absences — no download errors remain. The pipeline is fully operational and the dataset is ready for model training.
