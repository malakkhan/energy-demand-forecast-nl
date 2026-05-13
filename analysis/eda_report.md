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
   - [5.4 Valid Pixel Analysis — Cloud Seasonality & Measurement Bias](#54-valid-pixel-analysis--cloud-seasonality--measurement-bias)
   - [5.5 VIIRS A2 Non-Selective (Including Imputed Pixels)](#55-viirs-a2-non-selective-including-imputed-pixels)
6. [CBS Economic Indicators](#6-cbs-economic-indicators)
7. [KNMI Meteorological Observations](#7-knmi-meteorological-observations)
8. [KNMI Validated vs Non-Validated Comparison](#8-knmi-validated-vs-non-validated-comparison)
9. [Cross-Variable Analysis](#9-cross-variable-analysis)
10. [Key Findings & Recommendations](#10-key-findings--recommendations)

---

## 1. Dataset Overview

The final merged dataset spans **14 years** of hourly observations for the Netherlands, combining electricity demand, satellite imagery, economic indicators, and meteorological data.

| Property | Value |
|----------|-------|
| **Rows** | 122,736 hourly records |
| **Columns** | 114 features |
| **Time Span** | 2012-01-01 → 2025-12-31 (UTC) |
| **File Size** | 9.0 MB (Parquet, Snappy compressed) |
| **Temporal Resolution** | Hourly |

### Data Sources

| Source | Description | Records | Coverage |
|--------|-------------|---------|----------|
| **ENTSO-E** | Electricity load (MW) | 175,296 | 100.0% |
| **VIIRS A2 (selective)** | Gap-filled NTL composite, quality ≤ 1 only (daily) | 5,080 | 76.1% |
| **VIIRS A2 (non-selective)** | Gap-filled NTL composite, all non-fill pixels (daily) | 5,080 | 97.5% |
| **VIIRS A1** | At-sensor radiance (daily) | 5,102 | 97.6% |
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
| `ntl_a2_mean` | 23.93% | 29,376 | Daily → hourly broadcast; only 76.1% of days have valid cloud-free pixels |
| `ntl_a2_all_mean` | 2.52% | 3,096 | Daily → hourly broadcast; matches satellite orbit gaps only |
| `ntl_a1_mean` | 2.42% | 2,976 | Satellite maintenance windows |
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

| Statistic | Value |
|-----------|-------|
| Mean radiance | 5.75 nW/cm²/sr |
| Std deviation | 3.43 |
| Min | 0.0 |
| Max | 90.62 |
| Valid pixel % (mean) | 28.66% |

![VIIRS A2 STL](figures/viirs_a2_02_stl.png)

| Component | Variance % |
|-----------|-----------|
| Seasonal | 49.1% |
| Residual | 45.8% |
| Trend | 5.3% |

The A2 product has very high residual variance (45.8%), indicating substantial cloud-driven noise despite gap-filling.

![VIIRS A2 ACF/PACF](figures/viirs_a2_03_acf_pacf.png)

![VIIRS A2 sub-series](figures/viirs_a2_04_subseries.png)

### 5.2 VIIRS A1 (At-Sensor Radiance)

![VIIRS A1 time series](figures/viirs_a1_01_timeseries.png)

| Statistic | Value |
|-----------|-------|
| Mean radiance | 5.48 nW/cm²/sr |
| Std deviation | 3.16 |
| Min | 0.84 |
| Max | 27.99 |
| Valid pixel % (mean) | 96.32% |

![VIIRS A1 STL](figures/viirs_a1_02_stl.png)

| Component | Variance % |
|-----------|-----------|
| Residual | 66.0% |
| Seasonal | 32.9% |
| Trend | 0.5% |

A1 has even higher residual variance (66%) — expected for raw at-sensor radiance before gap-filling.

![VIIRS A1 ACF/PACF](figures/viirs_a1_03_acf_pacf.png)

![VIIRS A1 sub-series](figures/viirs_a1_04_subseries.png)

### 5.3 A1 vs A2 Comparison

![VIIRS A1 vs A2](figures/viirs_a1a2_comparison.png)

| Metric | Value |
|--------|-------|
| A1/A2 ratio mean | 3.87 |
| A1/A2 ratio median | 0.90 |
| A1/A2 ratio std | 85.32 |

The high ratio variance is driven by outlier days when cloud masking yields very different valid pixel counts between products.

### 5.4 Valid Pixel Analysis — Cloud Seasonality & Measurement Bias

#### 5.4.1 How valid pixels are defined

A pixel is counted as **valid** in Phase 2 aggregation ([phase2_aggregate.py](../src/pipeline/phase2_aggregate.py), lines 216–218) when two conditions are both met:

1. `is_fill == False` — the raw HDF5 value is not the fill sentinel −999.9 (genuine "no observation").
2. `quality_flag <= 1` — quality tier is either 0 (best/clear-sky) or 1 (good/fair).

The total pixel pool is fixed by the NL bounding box (50.75°–53.55°N, 3.35°–7.25°E), which clips exactly **629,664 pixels per granule** from the h18v03 sinusoidal tile.

The two products encode quality differently:

| Product | HDF5 dataset | Encoding | Threshold |
|---------|-------------|----------|-----------|
| **A1** (`VNP46A1`) | `QF_DNB` (uint16 bitmask) | Bits 0–1 extracted to uint8: 0=best, 1=fair, 2=poor, 3=no retrieval | `bits 0–1 <= 1` |
| **A2** (`VNP46A2`) | `Mandatory_Quality_Flag` (uint8) | 0=best (clear-sky), 1=good (additional QA applied), 2=degraded (cloud/smoke/fire/dust), 3=no retrieval, 255=fill | `flag <= 1` |

The **same numeric threshold (`<= 1`)** is applied to both, but the underlying semantics differ: A2's flag 2 explicitly encodes cloud contamination, whereas A1's QF_DNB tier 1 ("fair") still admits many partially compromised pixels.

#### 5.4.2 Why A2 has so many invalid pixels

The Netherlands (~52°N) has one of Europe's highest cloud cover rates — the sky is overcast on the majority of days in all seasons. This disproportionately affects A2 because:

- **A2 cloud masking is strict.** `Mandatory_Quality_Flag` values 2 and 3 cover all cloud-contaminated and non-retrieved pixels. Over the Netherlands, the overwhelming majority of nighttime pixels fall into these categories on any given night.
- **A1 is more permissive.** The raw `QF_DNB` bitmask uses a coarser 2-bit tier that admits more lightly contaminated pixels as "fair" (tier 1). Some cloud-edge pixels that A2 flags as quality 2 pass A1's filter as quality 1.
- **A2 gap-filling ≠ valid pixels.** Despite being the "gap-filled" product, A2's gap-filled pixels (drawn from historical composites when the current night is cloud-covered) are assigned `quality_flag 2+` because they are *imputed*, not directly observed. Our `quality_flag <= 1` filter therefore **excludes all imputed A2 pixels**, so the "gap-filled" label is misleading in the context of our valid-pixel count.

Net result: **A1 passes 96.32% of pixels as valid; A2 passes only 28.66%.** The remaining 71.34% of non-fill A2 pixels are imputed from historical composites. By month, the imputed fraction is:

| Jan | Feb | Mar | Apr | May | Jun | Jul | Aug | Sep | Oct | Nov | Dec |
|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|
| 67.5% | 60.1% | 53.8% | 61.5% | 95.7% | **100%** | 99.3% | 70.6% | **49.8%** | 62.1% | 65.5% | 71.0% |

June is 100% imputed — not a single directly observed A2 pixel exists across the 14-year record. September has the lowest imputed fraction (49.8%) due to autumn high-pressure blocking events.

A2 also occasionally reports 1,259,328 pixels per day (double the standard 629,664), reflecting two satellite overpasses composited into one file on certain nights. This does not increase valid pixel counts significantly.

#### 5.4.3 Seasonal pattern of invalid pixels

Valid pixel coverage shows a pronounced seasonal cycle that tracks NL cloud climatology:

**A2 selective monthly mean NTL radiance (nW/cm²/sr):**

| Jan | Feb | Mar | Apr | May | Jun | Jul | Aug | Sep | Oct | Nov | Dec |
|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|
| 5.82 | 5.84 | 5.36 | 5.57 | 6.67 | — | 7.97 | 5.57 | 5.80 | 5.77 | 5.44 | 5.78 |

**June has no A2-selective subseries entry at all** — every June in the 14-year record produced zero directly observed pixels. This is the nadir of NL cloud cover: long days, convective cloud formation (cumulus/cumulonimbus), and very short (~6 h) summer nights leave virtually no valid observing windows.

**A2 non-selective monthly mean NTL radiance (nW/cm²/sr):**

| Jan | Feb | Mar | Apr | May | Jun | Jul | Aug | Sep | Oct | Nov | Dec |
|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|
| 4.51 | 4.64 | 4.76 | 4.67 | 4.32 | 4.15 | 4.10 | 4.34 | 4.80 | 4.65 | 4.48 | 4.39 |

Including imputed pixels restores June (4.15 nW/cm²/sr) and eliminates the spurious summer peak. The seasonal pattern now shows a spring shoulder (Mar–Apr) and a secondary autumn peak (Sep: 4.80), with a genuine summer trough driven by the short NL summer nights.

**A1 monthly mean NTL radiance (nW/cm²/sr):**

| Jan | Feb | Mar | Apr | May | Jun | Jul | Aug | Sep | Oct | Nov | Dec |
|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|
| 6.62 | 6.32 | 5.45 | 4.91 | 4.61 | 5.00 | 4.40 | 4.72 | 5.30 | 5.78 | 6.02 | 6.52 |

A1 includes June (5.00 nW/cm²/sr) due to its permissive quality filter. Its clear winter peak (Jan: 6.62) reflects longer nights at 52°N.

The general seasonal pattern of **invalid pixels** is:
- **Summer (Jun–Aug)**: fewest valid pixels — convective clouds, short nights, high water vapour.
- **Winter (Dec–Feb)**: more valid pixels relative to summer — stratiform/frontal cloud systems pass more frequently, and nights are long (~16.5 h at 52°N).
- **Spring/autumn shoulder seasons** show intermediate cloud cover.

#### 5.4.4 Why A2 peaks in summer while A1 peaks in winter — and what this means for the correlation with ENTSO-E

**A1 peaks in winter (Jan: 6.62, Jul: 4.40 nW/cm²/sr trough) and correlates with ENTSO-E load (r = +0.092).**

The reason is geometric: at 52°N, January has ~16.5 hours of darkness and July only ~6. A1 measures at-sensor radiance integrated over whatever fraction of the night is cloud-free. Longer nights = more cumulative photon exposure per night at the same ground-level emission rate. This drives A1's winter peak, which coincidentally aligns with ENTSO-E's winter peak (Jan median: 14,039 MW) caused by heating demand and early-evening peak loads.

**A2-selective peaks in summer (Jul: 7.97, essentially flat Nov–Feb at ~5.4–5.8 nW/cm²/sr) and is nearly uncorrelated with ENTSO-E load (r ≈ 0.010).**

This is a **survivorship bias artefact** from strict cloud quality filtering, operating through two mechanisms:

1. **Extreme sample selection in summer.** In July and August, the vast majority of nights are overcast. The tiny fraction of clear summer nights that pass `quality_flag <= 1` are drawn from anticyclonic high-pressure events: dry, clean atmosphere, minimal aerosol scattering, optimal satellite viewing geometry. These conditions produce intrinsically higher apparent radiance than the more typical hazy/humid/partly-cloudy nights that dominate the broader statistical population. In winter, cloud cover is also high but more nights are partly clear, so the valid sample is larger, more representative, and the mean is pulled toward typical conditions.

2. **Night-length suppression of winter A2 values.** In winter, nights are long but cloud cover is also high. On a partly-cloudy winter night, fewer contiguous clear-sky pixels accumulate (frontal bands may cover large areas), and the valid A2 observations that do exist may not represent the full night's emission — they represent brief clear windows. This further suppresses winter A2 values relative to what might be expected from first principles.

**Consequence for selective A2:** Its seasonal cycle is dominated by cloud-sampling artifacts rather than actual variation in ground-level nighttime light emission. It cannot serve as a reliable proxy for electricity demand. `ntl_a2_mean` should be used with extreme caution or excluded from models targeting seasonal patterns.

**The non-selective A2 (`ntl_a2_all_mean`) partially resolves this artifact.** By including gap-filled imputed pixels (quality_flag 2–3), it provides values for all days including June (where selective A2 has zero coverage), and achieves 97.5% hourly coverage — matching A1 at 97.6%. However, including imputed pixels does **not** recover a strong load correlation: A2-all has r = 0.006 vs load, even lower than selective A2 (r = 0.010), compared to A1's r = 0.081. The imputed values reflect historical climatological composites rather than actual emission variability, which limits their predictive value. The A2-all STL decomposition is more interpretable (trend 34.8%, seasonal 28.2%, residual 31.3%) than selective A2 (trend 5.3%, seasonal 49.1%, residual 45.8%), but the signal remains weaker than A1.

> [!IMPORTANT]
> The apparent A2-selective "summer peak" is **not physical** — actual nighttime light emission in the Netherlands does not increase in summer. It is entirely an artifact of selection bias from strict cloud masking combined with short summer nights. Any model that ingests `ntl_a2_mean` as a raw feature will learn this spurious summer-peaking pattern, potentially confounding the real energy demand signal. Use `ntl_a2_all_mean` or `ntl_a1_mean` instead.

### 5.5 VIIRS A2 Non-Selective (Including Imputed Pixels)

The non-selective A2 variant (`viirs_a2_all_daily`) includes all non-fill A2 pixels in the daily mean — both directly observed (quality 0–1) and gap-filled/imputed (quality 2–3). This eliminates the survivorship bias of the selective version at the cost of mixing observed and synthetic pixel values.

#### 5.5.1 Time Series and STL

![VIIRS A2-all time series](figures/viirs_a2_all_01_timeseries.png)

| Statistic | Value |
|-----------|-------|
| Mean radiance | 4.48 nW/cm²/sr |
| Std deviation | 0.72 |
| Min | 2.70 |
| Max | 7.82 |
| Valid pixel % (mean) | 28.66% |
| Imputed pixel % (mean) | 71.34% |
| Hourly coverage | 97.5% |

The lower mean (4.48 vs 5.75 for selective) and much smaller standard deviation (0.72 vs 3.43) reflect the smoothing effect of including imputed composite values. Extreme outliers (max 90.62 in selective) are absent because imputed pixels are constrained to climatological composites.

![VIIRS A2-all STL](figures/viirs_a2_all_02_stl.png)

| Component | Variance % |
|-----------|-----------|
| **Trend** | **34.8%** |
| **Residual** | 31.3% |
| Seasonal | 28.2% |

The STL variance balance is markedly different from selective A2 (trend 5.3%, seasonal 49.1%, residual 45.8%). Non-selective A2 is more trend- and residual-dominated, indicating the imputed pixels absorb much of the spurious seasonal variance while exposing real long-run trends.

#### 5.5.2 ACF / PACF and Sub-Series

![VIIRS A2-all ACF/PACF](figures/viirs_a2_all_03_acf_pacf.png)

![VIIRS A2-all sub-series](figures/viirs_a2_all_04_subseries.png)

**Monthly mean radiance (nW/cm²/sr):**

| Jan | Feb | Mar | Apr | May | Jun | Jul | Aug | Sep | Oct | Nov | Dec |
|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|
| 4.51 | 4.64 | 4.76 | 4.67 | 4.32 | 4.15 | 4.10 | 4.34 | 4.80 | 4.65 | 4.48 | 4.39 |

The seasonal structure is now physically plausible: a spring shoulder (Mar–Apr peak at 4.76–4.67), a genuine summer trough (Jun–Jul: 4.15–4.10), and a secondary autumn peak (Sep: 4.80). June is no longer absent. The trough is consistent with reduced artificial lighting demand in summer (longer daylight hours, less commercial/residential lighting), though the imputed pixel majority means this pattern partly reflects the climatological composites used for gap-filling.

#### 5.5.3 Selective vs Non-Selective Comparison

![A2 selective vs all comparison](figures/viirs_a2_selective_vs_all.png)

![A2 imputed fraction by month](figures/viirs_a2_imputed_fraction_monthly.png)

**Key differences between the two A2 variants:**

| Metric | A2 Selective | A2 Non-Selective |
|--------|-------------|-----------------|
| Hourly coverage | 76.1% | 97.5% |
| Mean radiance | 5.75 nW/cm²/sr | 4.48 nW/cm²/sr |
| Std deviation | 3.43 | 0.72 |
| STL seasonal % | 49.1% | 28.2% |
| STL trend % | 5.3% | 34.8% |
| STL residual % | 45.8% | 31.3% |
| NTL–load Pearson r | +0.010 | +0.006 |
| June available | No | Yes |

**Coverage comparison with A1:**

| Product | Hourly coverage | NTL–load r |
|---------|----------------|------------|
| VIIRS A1 | 97.6% | +0.081 |
| VIIRS A2 non-selective | 97.5% | +0.006 |
| VIIRS A2 selective | 76.1% | +0.010 |

#### 5.5.4 Implications for Modeling

Despite near-identical coverage (97.5% vs 97.6%), A2-all has a substantially weaker load correlation (r = 0.006) than A1 (r = 0.081). This confirms that the coverage gap of selective A2 was not the main driver of its low correlation — it is the nature of the signal itself. Imputed A2 pixels carry climatological composite values rather than real-time emission data, diluting any economic or behavioural signal present in the directly observed subset.

The non-selective A2 is useful as:
- A **gap-free control** for ablation experiments comparing direct vs imputed NTL
- A **smooth activity proxy** when month-level or annual economic trends are the target (trend component is 34.8%)
- A **bias check** — large divergence between selective and non-selective A2 on a given day flags nights where the valid-pixel sample was so small that survivorship bias likely dominated

For short-term load forecasting, **A1 remains the preferred NTL input**.

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
| `ntl_a1_mean` | +0.092 | Brighter NTL → more load |
| `ntl_a2_mean` (selective) | +0.010 | Weak; dominated by survivorship bias |
| `ntl_a2_all_mean` (non-selective) | +0.006 | Weakest; imputed pixels dilute real signal |

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
| `knmi_humidity_pct` | 61,509 | 🔴 Extreme |
| `knmi_val_humidity_pct` | 54,998 | 🔴 Extreme |
| `cbs_cpi_energy` | 26,599 | 🔴 Extreme |
| `knmi_temp_c` | 11,016 | 🔴 Extreme |
| `cbs_cpi_gas` | 9,016 | 🔴 Extreme |
| `knmi_val_temp_c` | 8,748 | 🔴 Extreme |
| `knmi_wind_speed_ms` | 5,997 | 🔴 Extreme |
| `cbs_population_million` | 1,614 | 🔴 Severe |
| `cbs_gep_gas_hh_total` | 681 | 🟡 High |
| `ntl_a2_mean` | 5.18 | ✅ Acceptable |
| `ntl_a2_all_mean` | — | ✅ Low (correlated with `ntl_a2_mean`, r ≈ 0.12) |
| `ntl_a1_mean` | 3.73 | ✅ Acceptable |

> [!WARNING]
> **Severe multicollinearity detected.** The validated and non-validated KNMI variables are near-duplicates (r > 0.99). For regression modeling, use **only one** of the two KNMI datasets. Similarly, CBS CPI energy/gas/electricity are highly collinear — consider using only one CPI variant or applying PCA.

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
| `ntl_a1_mean` | +46 | +0.101 | +0.092 |

Solar radiation has the strongest lagged cross-correlation (r = −0.52 at lag −10h), but near-zero at lag 0 due to the diurnal phase offset between peak sunlight and peak load.

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

7. **A1 NTL peaks in winter (Jan: 6.62 nW/cm²/sr), correlating with energy demand (r = +0.092)** — driven by longer nights at 52°N. A2-selective peaks in summer (Jul: 7.97 nW/cm²/sr) due to cloud survivorship bias and is nearly uncorrelated with load (r = +0.010). June is entirely absent from A2-selective — zero directly observed pixels across the full 14-year record.

8. **A2's apparent "gap-filling" does not produce valid pixels under strict quality filtering.** Gap-filled A2 pixels carry `quality_flag 2+` (imputed, not directly observed) and are excluded by `quality_flag <= 1`. On average, 71.34% of non-fill A2 pixels are imputed; in June this reaches 100%.

9. **A2 non-selective achieves 97.5% hourly coverage** (matching A1 at 97.6%) but does not recover a strong load correlation (r = +0.006). Including imputed pixels eliminates the spurious summer peak and restores a physically plausible seasonal pattern (spring shoulder, summer trough, autumn peak), but the synthetic pixel values limit predictive power. A1 remains the superior NTL input for load forecasting.

### 10.2 Modeling Recommendations

> [!TIP]
> **Feature selection for modeling:**
> - Use **KNMI validated** (`knmi_val_*`) exclusively — drop non-validated columns
> - Choose **one** of: `cbs_cpi_energy`, `cbs_cpi_gas`, `cbs_cpi_electricity` (r > 0.82 pairwise)
> - Drop `cbs_gas_total_tax` (r = 0.97 with `cbs_population_million`)
> - Consider PCA on the CBS feature block to reduce collinearity
> - Include **lag features** for temperature (−83h) and solar radiation (−10h) based on CCF peaks
> - **NTL ranking**: use `ntl_a1_mean` (r = +0.092) as the primary NTL feature; consider `ntl_a2_all_mean` (r = +0.006, 97.5% coverage) as a smooth trend covariate; avoid `ntl_a2_mean` (r = +0.010, 76.1% coverage, spurious summer peak)

**Additional recommendations:**

**R7 — NTL valid-pixel quality threshold.** Before using any `ntl_a1_mean` or `ntl_a2_mean` value, apply a minimum valid-pixel coverage filter. Days where `ntl_valid_count / 629664 < 0.10` (fewer than 10% of NL pixels valid) should be treated as missing rather than used directly — the mean over such a tiny and heavily selected spatial sample is dominated by survivorship bias. For A2, this threshold would further reduce usable observations in summer (where coverage already approaches zero) but would make the remaining values more trustworthy.

**R8 — Night-length normalisation for A1.** A1's winter peak is partly geometric: longer nights at 52°N yield more photon exposure at the same ground-level emission rate. Dividing `ntl_a1_mean` by astronomical night length (computable from latitude and day-of-year) removes this seasonal geometric bias and leaves a residual that better reflects true activity/emission changes. This would make A1 a cleaner economic proxy.

**R9 — Use NTL as a monthly aggregate, not daily.** Given A2-selective's 23.9% hourly null rate and A1's 66% residual variance (high day-to-day weather noise), a 30-day rolling mean or monthly block average produces a far more stable NTL feature. `ntl_a2_all_mean` (non-selective) already provides near-complete daily coverage but its 0.72 std reflects the smoothing from imputed pixels — further monthly aggregation adds little. Broadcasting any noisy daily NTL value to all 24 hours amplifies measurement error. Monthly NTL aggregates align better with the slow-moving economic activity signal that NTL is intended to capture.

**R10 — Structural break for the 2022 energy crisis.** The 2022 demand trough (mean 11,460 MW, lowest since 2012) is a genuine structural break caused by price-driven industrial curtailment and household conservation. A model trained on 2012–2025 without a crisis indicator will mis-learn the 2022 period. Include a binary `energy_crisis` flag (True for approximately 2022–H1 2023) or use piecewise regression/changepoint detection around this break.

**R11 — Explicit calendar and time features.** The 24h (ACF = 0.84) and 168h (ACF = 0.92) autocorrelations confirm that calendar structure is the dominant signal. Explicitly encoding `hour_of_day`, `day_of_week`, `month_of_year`, `is_public_holiday` (NL calendar), `week_of_year`, and `is_dst_transition` will substantially outperform any model that must learn these from raw timestamps. Dutch public holidays (King's Day, Liberation Day, etc.) produce load dips of ~2,000 MW and are not derivable from date arithmetic alone.

**R12 — Solar radiation phase correction as an explicit feature.** The CCF shows `knmi_val_solar_rad_jcm2` peaks at lag +158h (r = −0.544) with near-zero correlation at lag 0. Creating an explicit `solar_rad_lag_minus_10h` feature (the empirically optimal short lag from the non-validated CCF) captures the phase-delayed grid demand suppression from daytime PV generation without requiring the model to learn the lag internally.

**R13 — Model architecture recommendation.** For this 122k-hour dataset:
- **Primary baseline**: gradient-boosted trees (LightGBM or XGBoost) with all calendar features, lagged targets (at 1h, 24h, 168h), and weather variables. These handle multicollinearity internally via feature selection and are the proven standard for short-term electricity load forecasting.
- **Statistical baseline**: SARIMA(p,d,q)(P,D,Q)[24] with weekly seasonality, as a lower-bound benchmark.
- **Evaluation metric**: MAPE or RMSE on the 2023–2025 hold-out, using walk-forward validation (train 2012–2020, validate 2021–2022, test 2023–2025) to respect the temporal order and correctly reflect the post-crisis regime.

**R14 — NTL as a long-run activity indicator, not a short-run predictor.** The CCF for A1 peaks at lag +46h (r = 0.101), not lag 0. The information NTL adds is about multi-day or monthly economic activity levels rather than hour-by-hour load. Consider constructing a monthly NTL feature (after night-length normalisation) and using it as an economic activity covariate alongside GDP and CPI, rather than treating it as a daily predictor.

### 10.3 Data Quality Summary

| Dataset | Gap % | Status |
|---------|-------|--------|
| ENTSO-E Load | 0.01% | ✅ Excellent |
| KNMI Validated | 0.00% | ✅ Excellent |
| KNMI Non-Validated | 0.77% | ✅ Healthy |
| VIIRS A1 | 1.94% | ✅ Healthy |
| VIIRS A2 (non-selective) | 2.52%\* | ✅ Healthy |
| VIIRS A2 (selective) | 23.93%\* | ⚠️ Cloud-limited |
| CBS (all series) | 0.00% | ✅ Complete |

\* Reported as hourly null % in the final merged dataset (daily gaps × 24h broadcast).

All gaps are genuine upstream data absences — no download errors remain. The pipeline is fully operational and the dataset is ready for model training.
