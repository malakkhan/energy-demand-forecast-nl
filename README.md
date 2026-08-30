# Energy Demand Forecast — Netherlands

A three-phase ETL pipeline and multi-model forecasting framework for hourly Netherlands electricity-demand prediction. The pipeline ingests VIIRS VNP46A1/A2 satellite nighttime-lights imagery, CBS consumer energy tariffs, CBS Consumer Price Index, CBS gas and electricity prices by consumption band, CBS quarterly economic statistics, ENTSO-E electricity load data, KNMI hourly in-situ meteorological observations (both non-validated and validated), and Dutch public/school holiday calendars, transforming them into a single, contiguous, year-partitioned Parquet dataset (145 columns, 2012–2025). The modelling layer evaluates six literature baselines and a novel Cross-Modal Attention Transformer (CMAT) across four ablation variants using Rolling-Origin Cross-Validation (ROCV) on SURF's Snellius supercomputer.

---

## Table of Contents

1. [Setup & Prerequisites](#setup--prerequisites)
2. [Data Download Scripts](#data-download-scripts)
3. [Data Processing Pipeline](#data-processing-pipeline)
   - [Phase 1 — Extract](#phase-1--extract)
   - [Phase 2 — Aggregate](#phase-2--aggregate)
   - [Phase 3 — Merge](#phase-3--merge)
4. [Exploratory Data Analysis](#exploratory-data-analysis)
5. [Forecasting Models](#forecasting-models)
   - [Baseline Models](#baseline-models)
   - [CMAT — Cross-Modal Attention Transformer](#cmat--cross-modal-attention-transformer)
6. [Results & Report Generation](#results--report-generation)
7. [Running on SLURM (Snellius)](#running-on-slurm-snellius)
8. [Observability — Logs, Jobs & Efficiency](#observability--logs-jobs--efficiency)
9. [Directory Layout](#directory-layout)

---

## Setup & Prerequisites

### Platform

This project is designed to run on **Snellius** (SURF's national supercomputer). Key assumptions about the environment:

| Aspect | Detail |
|---|---|
| **Cluster** | Snellius (SURF), `rome` partition (CPU) + `gpu_a100` partition (GPU) |
| **CPU Nodes** | Rome nodes: **128 CPU cores**, **229 GB RAM** |
| **GPU Nodes** | A100 nodes: **18 CPU cores**, **1× NVIDIA A100 40 GB**, **96 GB RAM** |
| **Filesystem** | GPFS at `/projects/prjs2061/` (shared project space) |
| **Scratch** | `$TMPDIR` / `$SLURM_TMPDIR` — node-local SSD, used for Spark shuffle spill |
| **Scheduler** | SLURM (sbatch / squeue / seff) |
| **Python** | 3.9+ (system Python on Snellius; a `pip-compile`-locked `requirements.txt` is provided) |
| **Java** | OpenJDK 17+ (required by PySpark/Spark 4.x; available via `module load` on Snellius) |
| **CUDA** | 12.1+ (required for CMAT GPU training; load via `module load 2023 CUDA/12.1.1`) |

### Python Dependencies

All dependencies are pinned in `requirements.txt` (generated from `requirements.in` via `pip-compile`):

```
pip install -r requirements.txt
```

Key packages:

| Package | Purpose |
|---|---|
| `h5py` | Reading VIIRS HDF5 satellite files |
| `numpy` | Vectorised pixel-level array operations |
| `shapely` | Polygon-based spatial masking (Netherlands GADM boundary) |
| `pandas` | Small-file parsing (CBS, ENTSO-E, quality metrics) |
| `pyarrow` | Direct Parquet I/O in Phase 1 (bypasses JVM overhead) |
| `pyspark` (4.x) | Distributed aggregation and joins in Phases 2 & 3 |
| `openpyxl` | Excel reading for ENTSO-E `.xlsx` workbooks |
| `requests` | KNMI Open Data API downloads |
| `torch` | PyTorch — LSTM baselines, Seq2Seq, and CMAT training |
| `scikit-learn` | MLR baseline, PCA, preprocessing, imputation |
| `prophet` | Prophet component of Prophet-LSTM baseline |
| `statsmodels` | SARIMAX baseline |
| `optuna` | Hyperparameter search for CMAT |

### Environment Variables (`.env`)

A `.env` file at the repository root stores API tokens. It is **git-ignored** and must be created manually:

```bash
# .env
EDL_TOKEN="<your NASA Earthdata Login JWT>"
LAADS_TOKEN="<your LAADS DAAC JWT>"
```

Source it before running download scripts:

```bash
source .env
```

| Variable | Required By | How to Obtain |
|---|---|---|
| `EDL_TOKEN` | `download_nl_viirs.py` (fallback) | [NASA Earthdata Login](https://urs.earthdata.nasa.gov/) → Profile → Generate Token |
| `LAADS_TOKEN` | `download_nl_viirs.py` (primary) | [LAADS DAAC](https://ladsweb.modaps.eosdis.nasa.gov/) → Profile → App Keys |
| `KNMI_API_KEY` | `download_nl_knmi.py` (non-validated dataset) | [KNMI Data Platform](https://dataplatform.knmi.nl/) → API Keys |
| `KNMI_VALIDATED_API_KEY` | `download_nl_knmi.py` (validated dataset) | [KNMI Data Platform](https://dataplatform.knmi.nl/) → API Keys → Bulk Download Policy for `hourly-in-situ-meteorological-observations-validated` |

The VIIRS download script checks `LAADS_TOKEN` first, then falls back to `EDL_TOKEN`. At least one must be set.

---

## Data Download Scripts

Raw data files are downloaded into `/projects/prjs2061/data/` subdirectories. These scripts are run **interactively** (not via SLURM) and are designed to be resumable.

### `download_nl_viirs.py` — VIIRS Nighttime Lights (VNP46A1 / VNP46A2)

Downloads VIIRS Black Marble satellite imagery from NASA's [LAADS DAAC](https://ladsweb.modaps.eosdis.nasa.gov/) archive. Each file is an HDF5 granule for the **h18v03** MODIS sinusoidal tile, which covers Western Europe including the entire Netherlands.

**Usage:**

```bash
source .env

# Download A2 (gap-filled BRDF-corrected NTL)
python3 -u src/download/download_nl_viirs.py -p A2 -d /projects/prjs2061/data/viirs/A2 -s 2012-01-01

# Download A1 (at-sensor raw radiance)
python3 -u src/download/download_nl_viirs.py -p A1 -d /projects/prjs2061/data/viirs/A1 -s 2012-01-01

# Download only specific days (re-download corrupt files, etc.)
python3 -u src/download/download_nl_viirs.py -p A2 -d /projects/prjs2061/data/viirs/A2 -s 2016-10-07 -e 2016-10-07
```

**CLI options:**

| Flag | Default | Description |
|---|---|---|
| `-p`, `--product` | `A2` | Product variant: `A1` (raw radiance) or `A2` (gap-filled NTL) |
| `-d`, `--dest` | `/projects/prjs2061/data/viirs` | Output directory for `.h5` files |
| `-s`, `--start` | `2012-01-01` | Start date (`YYYY-MM-DD`) |
| `-e`, `--end` | Today | End date (`YYYY-MM-DD`) |

**Key behaviour:**

- **Resume-safe**: Skips files that already exist locally with matching file size. To re-download a corrupt file, delete it first.
- **Dual-archive fallback**: Tries the Standard historical archive (`Collection 5200`) first; if a day is missing (e.g. very recent data), falls back to the NRT (Near Real-Time) archive.
- **cURL fallback**: If Python's `urllib` fails (e.g. SSL issues), transparently retries with system `curl`.
- **Long-running**: For a full 2012-present download (~5000 files per product), this takes many hours. Use `nohup` and `python3 -u` (unbuffered) for terminal-safe operation:

  ```bash
  nohup python3 -u src/download/download_nl_viirs.py -p A2 -d /projects/prjs2061/data/viirs/A2 >> download_A2.log 2>&1 &
  ```

### `download_nl_knmi.py` — KNMI Meteorological Data

Downloads hourly in-situ meteorological observations from the [KNMI Open Data Platform](https://dataplatform.knmi.nl/) via their REST API.

**Usage:**

```bash
python3 src/download/download_nl_knmi.py --dest /projects/prjs2061/data/knmi --start-year 2015
```

**CLI options:**

| Flag | Default | Description |
|---|---|---|
| `-k`, `--key` | `$KNMI_API_KEY` or `$KNMI_VALIDATED_API_KEY` | API key (auto-selected by dataset; override with `-k`) |
| `-n`, `--dataset` | `hourly-in-situ-meteorological-observations` | KNMI dataset name (append `-validated` for validated data) |
| `-V`, `--version` | `1.0` | Dataset version |
| `-d`, `--dest` | `/projects/prjs2061/data/knmi` (or `knmi_validated`) | Output directory (auto-selected when `--dataset` contains `validated`) |
| `--start-year` | `2015` | Only download files for this year and later |
| `--end-year` | No limit | Only download files up to and including this year |
| `--overwrite` | `false` | Re-download even if file exists locally |

**Key behaviour:**

- Paginates through the file listing API (500 files per page), filters by year from filename.
- Uses concurrent `ThreadPoolExecutor` downloads (up to 10 threads for small files, 1 for large).
- Handles HTTP 429 rate-limiting with automatic 20-second backoff retries.

**Validated dataset:** The same script supports downloading the validated version of the data by specifying a different dataset name:

```bash
source .env
python3 src/download/download_nl_knmi.py \
    --dataset hourly-in-situ-meteorological-observations-validated \
    --dest /projects/prjs2061/data/knmi_validated \
    --start-year 2012
```

When `--dataset` contains `validated`, the script automatically uses `KNMI_VALIDATED_API_KEY` from the environment (or you can pass `-k` explicitly). Output defaults to `/projects/prjs2061/data/knmi_validated`.

### `download_nl_holidays.py` — Dutch Holidays (OpenHolidays API)

Fetches Dutch public and school holidays from the [OpenHolidays API](https://openholidaysapi.org/) and saves them to a single CSV.

**Usage:**

```bash
python3 src/download/download_nl_holidays.py
```

Output: `data/calendar/nl_holidays.csv` — contains all public and school holidays for the Netherlands (2020–2025, the API's coverage window for NL).

### `generate_nl_public_holidays.py` — Dutch Public Holidays (Python Library)

Generates a complete list of Dutch public holidays using the `holidays` Python library, providing full coverage from 2012 to 2025.

**Usage:**

```bash
python3 src/download/generate_nl_public_holidays.py
```

Output: `data/calendar/nl_public_holidays.csv` — one row per public holiday with date, name, day of week, year, month, and day columns.

### Manual Downloads (CBS & ENTSO-E)

The CBS and ENTSO-E source files are downloaded manually from their respective platforms and placed in the expected directories:

| Source | Expected Location | Format | Notes |
|---|---|---|---|
| **Holiday Calendars** | `data/calendar/nl_holidays.csv`, `data/calendar/nl_public_holidays.csv` | CSV | Generated by download scripts above |
| **CBS Consumer Tariffs (old)** | `data/cbs/Average_energy_prices_for_consumers__2018*.csv` | Semicolon CSV, 6-line header | Old schema 2018–2023 |
| **CBS Consumer Tariffs (new)** | `data/cbs/Average_energy_prices_for_consumers_2*.csv` | Semicolon CSV, 6-line header | New schema 2021+ |
| **CBS Consumer Price Index** | `data/cbs/Consumentenprijzen__prijsindex_2015_100__*.csv` | Semicolon CSV, Dutch, 6-line header | CPI 2015=100 for energy/electricity/gas, monthly 1996–2025 |
| **CBS Gas & Electricity Prices** | `data/cbs/Prices_of_natural_gas_and_electricity_*.csv` | Semicolon CSV, 5-line header, long format | Semi-annual: 6 consumption bands × 3 price components, 2009–2025 |
| **CBS GDP Quarterly** | `data/cbs/GDP__output_and_expenditures__changes__*.csv` | Semicolon CSV, 4-line header | Wide-format: 18 indicators × 2 metrics, quarterly 1995–2025 |
| **CBS Population** | `data/cbs/Population (x million).csv` | Semicolon CSV | Annual population in millions |
| **ENTSO-E Load** | `data/entso-e/*.xlsx` | Excel workbooks | Wide format (2006–2015) + long format (2015+) |

> **Note**: File names contain datestamps from CBS's export system (e.g. `..._21042026_021720.csv`). The pipeline uses glob patterns to locate them regardless of the exact datestamp suffix.

---

## Data Processing Pipeline

The pipeline is organized in three sequential phases. Each phase reads from the previous phase's output directory and writes to the next. All phases support **idempotent re-runs** — already-processed outputs are skipped unless `--force` is passed.

```
/projects/prjs2061/data/viirs/A2/*.h5 ──┐
/projects/prjs2061/data/viirs/A1/*.h5 ──┤
/projects/prjs2061/data/cbs/*.csv ───────┼──▶ Phase 1 ──▶ data/processing_1/ ──▶ Phase 2 ──▶ data/processing_2/ ──▶ Phase 3 ──▶ data/processed/
/projects/prjs2061/data/entso-e/*.xlsx ─┘    (Extract)     (source-level             (Aggregate)  (aggregated/              (Merge)     nl_hourly_dataset.parquet
                                                             Parquet)                               combined Parquet)
```

---

### Phase 1 — Extract

**Script:** `src/pipeline/phase1_extract.py`

Extracts and cleans raw source files into standardised, source-level Parquet files. Runs **nine sub-stages** sequentially (two VIIRS products + CBS energy + CBS GDP + CBS CPI + CBS GEP + ENTSO-E + KNMI + KNMI validated):

#### 1A: VIIRS A2 and A1 Extraction

The same `extract_viirs()` function (parameterised by `product`) processes both VNP46A2 and VNP46A1 HDF5 files. Both products share the same HDF5 group structure, two-stage spatial filter, and fill value (−999.9). The key differences:

| Aspect | VNP46A2 | VNP46A1 |
|---|---|---|
| NTL dataset | `Gap_Filled_DNB_BRDF-Corrected_NTL` | `DNB_At_Sensor_Radiance` |
| Quality flag | `Mandatory_Quality_Flag` (uint8) | `QF_DNB` bits 0-1 extracted → uint8 |
| Output dir | `processing_1/viirs_a2/` | `processing_1/viirs_a1/` |

- **Parallelism**: `multiprocessing.ProcessPoolExecutor` — each worker processes one HDF5 file independently. On a 96-core node this achieves near-linear speedup.
- **Spatial masking**: Two-stage filter applied identically to both products:
  1. **Bounding-box crop** (fast): reduces the 2400×2400 h18v03 tile to a ~672×937 subgrid using coordinate bounds (`50.75°N–53.55°N`, `3.35°E–7.25°E`).
  2. **GADM polygon mask** (precise): a vectorised point-in-polygon test using `shapely.contains_xy()` against the GADM 4.1 Netherlands Level 0 boundary (`data/geo/gadm41_NLD_0.json`) retains only pixels inside Dutch land territory and islands, excluding North Sea and foreign-border pixels. The mask is computed once at startup (~0.3 s) and cached for all workers. This reduces the pixel count from 629,664 to **285,719 per day** (45.4% of the bounding box).
- **Caching**: Existing Parquet outputs are skipped (unless `--force`); partial writes are prevented via atomic `tmp → rename`.
- **Retry**: HDF5 opens retry up to 3 times with exponential backoff for transient GPFS errors.

**Output schema** — identical for both products (`viirs_a2` and `viirs_a1`):
- Path: `data/processing_1/viirs_a2/data/year=YYYY/day_YYYYDDD.parquet` (and `viirs_a1/`)

| Column | Type | Description |
|---|---|---|
| `date` | `date` | Observation date |
| `row_idx` | `int32` | VIIRS grid row index |
| `col_idx` | `int32` | VIIRS grid column index |
| `lat` | `float32` | Latitude (WGS84) |
| `lon` | `float32` | Longitude (WGS84) |
| `ntl_radiance` | `float32` | NTL radiance (nW/cm²/sr); `NaN` for fill pixels |
| `quality_flag` | `uint8` | 0 = best, 1 = good, 2+ = degraded/no retrieval |
| `is_fill` | `bool` | `True` if the pixel was a fill value (`-999.9`) in the source |

#### 1B: CBS Consumer Energy Tariffs (Harmonized)

Reads two CBS "Average energy prices for consumers" CSV files and harmonizes them into a single monthly series spanning 2018–2026.

**Harmonization logic:**

- **Old file** (2018–2023 schema) contributes months ≤2020; **new file** (2021+ schema) contributes months ≥2021.  This avoids the methodology break between the two CBS publications.
- **Variable supply rates dropped**: the old "variable delivery rate" and new "variable contract price" use incompatible methodologies and cannot be spliced.
- **ODE tax merged** from old → new for 2021–2022: the old file contains ODE tax values for these years that are absent from the new schema. For ≥2023, CBS merged ODE into the energy tax line.
- **`total_tax` computed**: `ODE + energy_tax` for ≤2022; `energy_tax` alone for ≥2023, producing a continuous comparable series.
- **Dynamic contract columns** from the new schema are preserved; `NULL` before 2025.

**Output schema** — `data/processing_1/cbs_energy/data/cbs_energy.parquet` (99 monthly rows × 15 columns):

| Column | Type | Unit | Coverage |
|---|---|---|---|
| `year` | `int64` | — | 2018–2026 |
| `month` | `int64` | — | 1–12 |
| `cbs_gas_transport_rate` | `float64` | Euro/year | 2018–2026 |
| `cbs_gas_fixed_supply_rate` | `float64` | Euro/year | 2018–2026 |
| `cbs_gas_ode_tax` | `float64` | Euro/m³ | 2018–2022 |
| `cbs_gas_energy_tax` | `float64` | Euro/m³ | 2018–2026 |
| `cbs_gas_total_tax` | `float64` | Euro/m³ | 2018–2026 (continuous) |
| `cbs_elec_transport_rate` | `float64` | Euro/year | 2018–2026 |
| `cbs_elec_fixed_supply_rate` | `float64` | Euro/year | 2018–2026 |
| `cbs_elec_fixed_supply_rate_dynamic` | `float64` | Euro/year | 2025+ only |
| `cbs_elec_variable_supply_rate_dynamic` | `float64` | Euro/kWh | 2025+ only |
| `cbs_elec_ode_tax` | `float64` | Euro/kWh | 2018–2022 |
| `cbs_elec_energy_tax` | `float64` | Euro/kWh | 2018–2026 |
| `cbs_elec_total_tax` | `float64` | Euro/kWh | 2018–2026 (continuous) |
| `cbs_elec_energy_tax_refund` | `float64` | Euro/year | 2018–2026 |

#### 1C: CBS GDP / Quarterly National Accounts + Population

Reads the wide-format CBS quarterly GDP CSV (18 economic indicators × 2 metric types) and a separate population CSV. Quarterly values are forward-filled to monthly granularity.

- **18 indicators** include GDP, imports/exports (goods & services), consumption (households & government), capital formation, inventories, and final expenditure.
- **2 metric types** per indicator: year-over-year volume change (`_yy`) and quarter-over-quarter volume change (`_qq`), yielding **35 columns** (GDP working-days-adjusted has y/y only).
- **Quarterly → monthly forward-fill**: Q1 → Jan/Feb/Mar, Q2 → Apr/May/Jun, etc.
- **Population** from a separate annual CSV is left-joined on year and replicated to all 12 months.

**Output schema** — `data/processing_1/cbs_gdp/data/cbs_gdp.parquet` (360 monthly rows × 38 columns):

| Column | Type | Description |
|---|---|---|
| `year` | `int64` | Year (1996–2025) |
| `month` | `int64` | Month (1–12) |
| `cbs_gdp_yy` | `float64` | GDP volume change, year-over-year (%) |
| `cbs_gdp_qq` | `float64` | GDP volume change, quarter-over-quarter (%) |
| `cbs_gdp_wda_yy` | `float64` | GDP (working days adjusted), y/y (%) |
| `cbs_imports_total_yy` / `_qq` | `float64` | Total imports, y/y and q/q (%) |
| `cbs_imports_goods_yy` / `_qq` | `float64` | Imports of goods, y/y and q/q (%) |
| `cbs_imports_services_yy` / `_qq` | `float64` | Imports of services, y/y and q/q (%) |
| `cbs_consumption_total_yy` / `_qq` | `float64` | Total consumption, y/y and q/q (%) |
| `cbs_consumption_hh_yy` / `_qq` | `float64` | Household consumption, y/y and q/q (%) |
| `cbs_consumption_gov_yy` / `_qq` | `float64` | Government consumption, y/y and q/q (%) |
| `cbs_capform_total_yy` / `_qq` | `float64` | Capital formation, y/y and q/q (%) |
| `cbs_capform_enterprise_yy` / `_qq` | `float64` | Enterprise capital formation, y/y and q/q (%) |
| `cbs_capform_gov_yy` / `_qq` | `float64` | Government capital formation, y/y and q/q (%) |
| `cbs_inventories_yy` / `_qq` | `float64` | Inventories (value as % of GDP) |
| `cbs_exports_total_yy` / `_qq` | `float64` | Total exports, y/y and q/q (%) |
| `cbs_exports_goods_yy` / `_qq` | `float64` | Exports of goods, y/y and q/q (%) |
| `cbs_exports_services_yy` / `_qq` | `float64` | Exports of services, y/y and q/q (%) |
| `cbs_disposable_total_yy` / `_qq` | `float64` | Total disposable, y/y and q/q (%) |
| `cbs_natl_final_exp_yy` / `_qq` | `float64` | National final expenditure, y/y and q/q (%) |
| `cbs_final_exp_total_yy` / `_qq` | `float64` | Total final expenditure, y/y and q/q (%) |
| `cbs_population_million` | `float64` | Population (millions), annual forward-filled |

#### 1D: CBS Consumer Price Index (CPI) — Energy

Reads the CBS CPI file (`Consumentenprijzen; prijsindex 2015=100`) and extracts three monthly energy price-index series.

**What a CPI value means.** The index is anchored to 2015 = 100. A value of 162 in a given month means the energy basket costs 62 % more than it did in 2015. This makes the series directly comparable across all years without any unit-conversion, unlike the absolute tariff prices (Euro/kWh, Euro/m³) which cannot be compared across years without accounting for inflation.

**How it complements the tariff files.** The tariff files (Phase 1B) cover 2018–2026 and give a detailed component breakdown (transport, supply, taxes). The CPI covers 1996–2025 and gives a single aggregate index per energy type. The two are complementary: the CPI provides the long-horizon price trend while the tariff files provide the structural decomposition. Critically, the CPI fills the 2012–2017 period where the tariff columns are null in the final dataset.

**Parsing notes.** The file uses Dutch month names and comma decimal separators. Annual summary rows (one per calendar year) are dropped; only the 12 monthly rows per year are kept.

**Output schema** — `data/processing_1/cbs_cpi/data/cbs_cpi.parquet` (360 monthly rows × 5 columns):

| Column | Type | Description |
|---|---|---|
| `year` | `int64` | Year (1996–2025) |
| `month` | `int64` | Month (1–12) |
| `cbs_cpi_energy` | `float64` | Overall energy basket CPI (2015 = 100) |
| `cbs_cpi_electricity` | `float64` | Electricity CPI (2015 = 100) |
| `cbs_cpi_gas` | `float64` | Gas CPI (2015 = 100) |

#### 1E: CBS Gas & Electricity Prices (GEP)

Reads the CBS "Prices of natural gas and electricity" CSV and extracts semi-annual prices for six consumption-band segments across three price components (total/supply/network), all including VAT and taxes.

**Six consumption bands:**

| Band tag | Segment | Unit |
|---|---|---|
| `gas_hh` | Gas household (569–5 687 m³/yr) | €/m³ |
| `gas_nnh_med` | Gas non-household medium (28 433–284 333 m³/yr) | €/m³ |
| `gas_nnh_lrg` | Gas non-household large (≥28 433 324 m³/yr) | €/m³ |
| `elec_hh` | Electricity household (2.5–5 MWh/yr) | €/kWh |
| `elec_nnh_med` | Electricity non-household medium (500–2 000 MWh/yr) | €/kWh |
| `elec_nnh_lrg` | Electricity non-household large (≥150 000 MWh/yr) | €/kWh |

**Parsing logic.** The file is in long format with 3 rows per period (Total/Supply/Network price). Each semester period is expanded to 6 monthly rows (H1→Jan–Jun, H2→Jul–Dec). Annual-average rows are dropped.

**Output schema** — `data/processing_1/cbs_gep/data/cbs_gep.parquet` (~204 monthly rows × 20 columns):

- `year`, `month` — temporal keys
- 18 columns named `cbs_gep_{band}_{component}` where `component` ∈ `{total, supply, network}` — e.g. `cbs_gep_gas_hh_total`, `cbs_gep_elec_hh_supply`

Coverage: H1 2009 – H2 2025.

#### 1F: ENTSO-E Load Extraction

Reads ENTSO-E Excel workbooks (both legacy wide format and modern long format), filters to Netherlands (`NL`), deduplicates to hourly resolution, and writes year-partitioned Parquet.

**Output schema** — `data/processing_1/entsoe/data/year=YYYY/entsoe.parquet`:

| Column | Type | Description |
|---|---|---|
| `timestamp_utc` | `datetime64[us]` | Hourly timestamp in UTC (timezone-naive) |
| `entsoe_load_mw` | `float64` | Netherlands electricity load (MW) |

#### 1G: KNMI Hourly Meteorological Observations

Reads ~25,000 NetCDF4 files from the KNMI download directory using `h5py` (NetCDF4 files are HDF5-based, so no additional dependency is needed). Each file contains one hour of observations from **61 weather stations** across the Netherlands. For each of 8 energy-relevant meteorological variables, the mean across all reporting stations is computed to produce one national-level hourly value.

Uses `ProcessPoolExecutor` for parallelism (same pattern as VIIRS — each file is fully independent).

**Output schema** — `data/processing_1/knmi/data/year=YYYY/knmi.parquet`:

| Column | Type | Unit | Description |
|---|---|---|---|
| `timestamp_utc` | `datetime64[us]` | — | Observation hour (UTC) |
| `knmi_temp_c` | `float64` | °C | Temperature at 1.5m |
| `knmi_dewpoint_c` | `float64` | °C | Dew point temperature |
| `knmi_wind_speed_ms` | `float64` | m/s | 10-min mean wind speed |
| `knmi_wind_speed_hourly_ms` | `float64` | m/s | Hourly mean wind speed |
| `knmi_wind_gust_ms` | `float64` | m/s | Maximum wind gust |
| `knmi_solar_rad_jcm2` | `float64` | J/cm² | Global solar radiation |
| `knmi_sunshine_h` | `float64` | h | Sunshine duration |
| `knmi_humidity_pct` | `float64` | % | Relative humidity |
| `knmi_station_count` | `int32` | — | Number of reporting stations |

#### 1H: KNMI Validated Hourly Meteorological Observations

Identical to 1G but reads from the **validated** dataset directory (`/projects/prjs2061/data/knmi_validated`). All output columns use the `knmi_val_` prefix instead of `knmi_`.

**Output schema** — `data/processing_1/knmi_validated/data/year=YYYY/knmi.parquet`:

| Column | Type | Unit | Description |
|---|---|---|---|
| `timestamp_utc` | `datetime64[us]` | — | Observation hour (UTC) |
| `knmi_val_temp_c` | `float64` | °C | Temperature at 1.5m |
| `knmi_val_dewpoint_c` | `float64` | °C | Dew point temperature |
| `knmi_val_wind_speed_ms` | `float64` | m/s | 10-min mean wind speed |
| `knmi_val_wind_speed_hourly_ms` | `float64` | m/s | Hourly mean wind speed |
| `knmi_val_wind_gust_ms` | `float64` | m/s | Maximum wind gust |
| `knmi_val_solar_rad_jcm2` | `float64` | J/cm² | Global solar radiation |
| `knmi_val_sunshine_h` | `float64` | h | Sunshine duration |
| `knmi_val_humidity_pct` | `float64` | % | Relative humidity |
| `knmi_val_station_count` | `int32` | — | Number of reporting stations |

#### CLI Options

```bash
python src/pipeline/phase1_extract.py [--data-root /path/to/raw/data] [--out-root /path/to/output] [--workers N] [--force] [--start-method fork|spawn]
```

| Flag | Default | Description |
|---|---|---|
| `--data-root` | `/projects/prjs2061/data` | Root directory for raw input data (VIIRS, CBS, ENTSO-E, KNMI) |
| `--out-root` | `<repo>/data` | Root directory for pipeline outputs (`processing_1/`, etc.) |
| `--workers` | All CPUs | Number of parallel worker processes |
| `--force` | `false` | Re-process files even if output already exists |
| `--start-method` | `fork` | Python multiprocessing start method |

#### Quality Reports

Each sub-stage writes a `data_quality.json` alongside its output (e.g. `data/processing_1/viirs_a2/data_quality.json`) containing row counts, column-level null statistics, date ranges, and performance metrics.

---

### Phase 2 — Aggregate

**Script:** `src/pipeline/phase2_aggregate.py`

Reads Phase 1 outputs and produces aggregated/combined tables using **PySpark** in local mode. Runs **seven sub-stages** (A2 selective aggregation, A2 non-selective aggregation, A1 aggregation, CBS combination, ENTSO-E pass-through, KNMI pass-through, KNMI validated pass-through):

#### 2A: VIIRS A2, A2-all, and A1 Daily Aggregation

The same `aggregate_viirs()` function (parameterised by `product` and `selective`) is run three times: once for A2 selective, once for A2 non-selective ("A2-all"), and once for A1. Reads the NL pixel-level Parquet (~286K pixels/day × ~5000 days) and aggregates to **one row per day** with spatial summary statistics. Uses column pruning (only reads 4 of 8 columns) to halve I/O.

**Selective vs non-selective aggregation:**

| Mode | Pixels in `ntl_mean`/`ntl_sum` | Output dir | Column prefix |
|---|---|---|---|
| **Selective** (default) | `quality_flag ≤ 1` AND not fill (high-quality, directly observed) | `viirs_a2_daily/` | `ntl_a2_` |
| **Non-selective** (A2-all) | All non-fill pixels (quality 0–3, including gap-filled/imputed) | `viirs_a2_all_daily/` | `ntl_a2_all_` |
| **A1 selective** | `quality_flag ≤ 1` AND not fill | `viirs_a1_daily/` | `ntl_a1_` |

In all three modes, the pixel-count breakdown (`ntl_valid_count`, `ntl_fill_count`, `ntl_invalid_count`) uses the same fixed definitions, so the outputs are directly comparable.

> **Why A2-all?** The selective A2 version exhibits severe survivorship bias — summer nights are short and heavily cloud-masked, so only a small fraction of pixels pass the quality ≤ 1 filter, creating a spurious "summer peak" artifact. A2-all includes gap-filled/imputed pixels (quality 2–3) from the VNP46A2 product, which eliminates this selection bias and achieves ~97.5% hourly coverage (vs ~76% for selective A2). However, the imputed values reflect historical climatological composites rather than actual emission variability, so the signal is smoother and less predictive of load.

**Output schema** — identical for all three variants:
- `data/processing_2/viirs_a2_daily/data/year=YYYY/part-*.parquet` (selective)
- `data/processing_2/viirs_a2_all_daily/data/year=YYYY/part-*.parquet` (non-selective)
- `data/processing_2/viirs_a1_daily/data/year=YYYY/part-*.parquet` (A1)

| Column | Type | Description |
|---|---|---|
| `date` | `date` | Observation date |
| `ntl_mean` | `double` | Mean NTL radiance of selected pixel set (see mode above); `null` when coverage < 10% |
| `ntl_sum` | `double` | Sum of NTL radiance across selected pixel set; `null` when coverage < 10% |
| `ntl_valid_count` | `int` | Count of valid pixels (quality ≤ 1, not fill) |
| `ntl_fill_count` | `int` | Count of fill-value pixels (sensor/retrieval gaps) |
| `ntl_invalid_count` | `int` | Count of quality-degraded/imputed pixels (quality > 1, not fill) |

> **10% minimum coverage filter**: Days where the contributing pixel count (28,571 = 10% of 285,719 NL polygon pixels) is not met have `ntl_mean` and `ntl_sum` set to `null`. This eliminates unreliable statistics from nights with extreme spatial selection bias. Pixel counts remain populated for diagnostic use.

#### 2B: CBS Combined

Outer-joins all four CBS monthly sources on `(year, month)` via a broadcast-join loop. The result spans 1996–2026 with source-appropriate nulls outside each source's coverage window.

**Output schema** — `data/processing_2/cbs_combined/data/part-*.parquet`:

| Source | Columns included | Non-null coverage |
|---|---|---|
| CBS Energy | 13 `cbs_gas_*` / `cbs_elec_*` tariff columns | 2018–2026 |
| CBS GDP | 35 `cbs_*_yy` / `cbs_*_qq` indicator columns + `cbs_population_million` | 1996–2025 |
| CBS CPI | `cbs_cpi_energy`, `cbs_cpi_electricity`, `cbs_cpi_gas` | 1996–2025 |
| CBS GEP | 18 `cbs_gep_{band}_{component}` price columns | 2009–2025 |

#### 2C: ENTSO-E Pass-through

Re-partitions ENTSO-E hourly data by year with one file per partition and generates a Phase 2 quality report (same schema as Phase 1 ENTSO-E output).

#### 2D: KNMI Pass-through

Re-partitions KNMI hourly data by year and generates a Phase 2 quality report with per-year hour coverage vs expected (8760/8784), temperature statistics (min/max/mean/stddev), and per-variable null percentages.

#### 2E: KNMI Validated Pass-through

Same as 2D but for the validated dataset. Reads from `processing_1/knmi_validated/` and writes to `processing_2/knmi_validated/`. Uses `knmi_val_` column prefix for quality statistics.

#### CLI Options

```bash
python src/pipeline/phase2_aggregate.py [--out-root /path/to/output] [--workers N] [--force] [--driver-memory 64g]
```

| Flag | Default | Description |
|---|---|---|
| `--out-root` | `<repo>/data` | Root directory for pipeline outputs (`processing_1/`, `processing_2/`) |
| `--workers` | All CPUs | Spark local-mode thread count |
| `--driver-memory` | `64g` | Spark driver memory (JVM heap) |
| `--force` | `false` | Re-run stages even if `_SUCCESS` marker exists |

#### Spark Configuration

Phase 2 (and Phase 3) use Spark in `local[N]` mode with: Adaptive Query Execution (AQE) for automatic shuffle partition coalescing and skew handling, node-local SSD (`$TMPDIR`) for shuffle spill (GPFS is catastrophically slow for Spark shuffle I/O), and Arrow-based Spark↔Pandas conversion.

#### Idempotency

Each sub-stage checks for Spark's `_SUCCESS` marker file. If present, the stage is skipped. Use `--force` to re-run.

---

### Phase 3 — Merge

**Script:** `src/pipeline/phase3_merge.py`

Joins all Phase 2 outputs and Dutch holiday calendars onto a **contiguous hourly UTC timestamp spine** (2012-01-01 00:00 → 2025-12-31 23:00) to produce the final unified dataset. Also generates cyclical (sin/cos) temporal features, public/school holiday binary flags, autoregressive and weather lag features, PCA components from CBS + KNMI validated features, and an energy crisis step variable.

**Join strategy:**

| Source | Join Key | Output Columns | Join Type |
|---|---|---|---|
| ENTSO-E (hourly) | `timestamp` | `entsoe_load_mw` | Broadcast left-join |
| VIIRS A2 selective (daily) | `date` | `ntl_a2_mean`, `ntl_a2_sum`, `ntl_a2_valid_count`, `ntl_a2_fill_count`, `ntl_a2_invalid_count` | Broadcast left-join |
| VIIRS A2-all non-selective (daily) | `date` | `ntl_a2_all_mean`, `ntl_a2_all_sum`, `ntl_a2_all_valid_count`, `ntl_a2_all_fill_count`, `ntl_a2_all_invalid_count` | Broadcast left-join |
| VIIRS A1 (daily) | `date` | `ntl_a1_mean`, `ntl_a1_sum`, `ntl_a1_valid_count`, `ntl_a1_fill_count`, `ntl_a1_invalid_count` | Broadcast left-join |
| CBS Combined (monthly) | `(year, month)` | All `cbs_*` columns | Broadcast left-join |
| KNMI (hourly) | `timestamp` | `knmi_temp_c`, `knmi_dewpoint_c`, `knmi_wind_speed_ms`, `knmi_wind_speed_hourly_ms`, `knmi_wind_gust_ms`, `knmi_solar_rad_jcm2`, `knmi_sunshine_h`, `knmi_humidity_pct`, `knmi_station_count` | Left equi-join |
| KNMI Validated (hourly) | `timestamp` | `knmi_val_temp_c`, `knmi_val_dewpoint_c`, `knmi_val_wind_speed_ms`, `knmi_val_wind_speed_hourly_ms`, `knmi_val_wind_gust_ms`, `knmi_val_solar_rad_jcm2`, `knmi_val_sunshine_h`, `knmi_val_humidity_pct`, `knmi_val_station_count` | Left equi-join |
| Holiday calendars | `date` | `is_public_holiday`, `is_school_holiday` | Set-lookup on date |

All source tables are small enough to be broadcast; no shuffles occur.

**Final output** — `data/processed/nl_hourly_dataset.parquet/year=YYYY/part-*.parquet`:

The final dataset contains **145 columns** (see [`docs/feature_dictionary.csv`](docs/feature_dictionary.csv) for the full schema reference). Column ordering is deterministic:

1. `timestamp` — hourly UTC timestamp (primary key)
2. `entsoe_load_mw` — **target variable** (electricity load in MW)
3. VIIRS A2 selective satellite aggregates (5 columns: `ntl_a2_mean`, `ntl_a2_sum`, `ntl_a2_valid_count`, `ntl_a2_fill_count`, `ntl_a2_invalid_count`)
4. VIIRS A2-all non-selective satellite aggregates (5 columns: `ntl_a2_all_mean`, `ntl_a2_all_sum`, `ntl_a2_all_valid_count`, `ntl_a2_all_fill_count`, `ntl_a2_all_invalid_count`)
5. VIIRS A1 satellite aggregates (5 columns: `ntl_a1_mean`, `ntl_a1_sum`, `ntl_a1_valid_count`, `ntl_a1_fill_count`, `ntl_a1_invalid_count`)
6. CBS gas tariff columns (5 columns)
7. CBS electricity tariff columns (8 columns)
8. CBS GDP headline indicators
9. CBS population (`cbs_population_million`)
10. All remaining `cbs_*` columns in sorted order (forward-compatible)
11. KNMI non-validated meteorological (9 columns: `knmi_temp_c`, `knmi_dewpoint_c`, `knmi_wind_speed_ms`, `knmi_wind_speed_hourly_ms`, `knmi_wind_gust_ms`, `knmi_solar_rad_jcm2`, `knmi_sunshine_h`, `knmi_humidity_pct`, `knmi_station_count`)
12. All remaining `knmi_*` columns (non-validated) in sorted order (forward-compatible)
13. KNMI validated meteorological (9 columns: `knmi_val_temp_c`, `knmi_val_dewpoint_c`, `knmi_val_wind_speed_ms`, `knmi_val_wind_speed_hourly_ms`, `knmi_val_wind_gust_ms`, `knmi_val_solar_rad_jcm2`, `knmi_val_sunshine_h`, `knmi_val_humidity_pct`, `knmi_val_station_count`)
14. All remaining `knmi_val_*` columns in sorted order (forward-compatible)
15. PCA components (15 columns: `pca_cbs_knmi_01` through `pca_cbs_knmi_15`) — 15 principal components from CBS + KNMI validated features via StandardScaler → PCA; captures ~95% of combined variance
16. Temporal features (9 columns: `year`, `month`, `day`, `hour`, `day_of_week`, `is_weekend`, `day_of_year`, `week_of_year`, `quarter`)
17. Cyclical temporal features (6 columns: `hour_sin`, `hour_cos`, `dow_sin`, `dow_cos`, `month_sin`, `month_cos`) — sin/cos encoding so periodic boundaries (e.g. hour 23 → 0, December → January) are continuous
18. Holiday flags (2 columns: `is_public_holiday`, `is_school_holiday`) — binary indicators from Dutch holiday calendars
19. Load lag features (4 columns: `load_lag_12h`, `load_lag_24h`, `load_lag_1w`, `load_lag_1y`) — autoregressive features at −12h, −24h, −1 week, −1 year
20. Weather lag features (3 columns: `solar_rad_lag_10h`, `humidity_lag_12h`, `temp_lag_107h`) — CCF-optimal lags from validated KNMI data
21. Event flags (1 column: `is_energy_crisis`) — step variable for the 2022–H1 2023 European energy crisis

**Coverage tracking**: Phase 3 logs separate coverage metrics for VIIRS A2 (selective), VIIRS A2 (all), VIIRS A1, ENTSO-E, CBS tariffs, CBS GDP, CBS CPI, CBS GEP, KNMI, and KNMI Validated, and writes them all to `data_quality.json`.

#### CLI Options

```bash
python src/pipeline/phase3_merge.py [--out-root /path/to/output] [--start 2012-01-01] [--end 2025-12-31]
                       [--workers N] [--force]
```

| Flag | Default | Description |
|---|---|---|
| `--out-root` | `<repo>/data` | Root directory for pipeline outputs (`processing_2/`, `processed/`) |
| `--start` | `2012-01-01` | Spine start date |
| `--end` | `2025-12-31` | Spine end date |
| `--workers` | `1` | Accepted for CLI compatibility; merge is single-threaded |
| `--force` | `false` | Re-run even if final output exists |

---

## Exploratory Data Analysis

**Script:** `analysis/run_analysis.py`  
**SLURM:** `analysis/run_analysis.slurm`

A batch-mode EDA script that produces all time series analysis figures and saves them to `analysis/figures/`. Designed for non-interactive execution on Snellius (uses `Agg` backend, FFT-based correlation, subsampled ACF/PACF).

**Sections:**

| # | Section | Key Figures |
|---|---------|-------------|
| 1 | Data loading | ENTSO-E, VIIRS A2 (selective), VIIRS A2 (all), VIIRS A1, CBS, KNMI, KNMI Validated |
| 2 | ENTSO-E hourly load | Time series, STL (period=168h), ACF/PACF, subseries |
| 3 | VIIRS A2 daily NTL (selective) | Time series, STL (period=365d), ACF/PACF, subseries |
| 4 | VIIRS A1 daily NTL | Same as A2 + A1/A2 comparison |
| 4b | VIIRS A2-all (non-selective) | Same as A2 + selective vs all comparison, imputed pixel fraction |
| 5 | CBS monthly indicators | Time series, STL (period=12mo), ACF/PACF, subseries |
| 6 | Multicollinearity | Correlation heatmap, VIF, cross-correlation, pair plot |
| 7 | KNMI meteorological | Time series (8 variables), STL (period=24h), ACF/PACF, subseries, weather–load scatter |
| 8 | KNMI validated meteorological | Same as section 7 for validated observations |
| 9 | Validated vs non-validated | Side-by-side temperature, difference plot, per-variable correlation |
| 10 | Temporal cyclical & holidays | Cyclical features vs load scatter, holiday load impact box plots, hourly profile overlay, correlation summary |

**Performance optimizations:**
- ACF/PACF uses FFT-accelerated ACF and Yule-Walker PACF on the full dataset (168 lags)
- VIF computed on a 50K-row random subsample
- CCF uses FFT-based `scipy.signal.correlate` — O(n log n) vs O(n²)
- `OMP_NUM_THREADS` set to match SLURM CPU allocation for NumPy BLAS parallelism

**Additional analysis scripts:**

| Script | Purpose |
|--------|---------|
| `analysis/gap_analysis.py` | KNMI data gap analysis utility; results in `analysis/gap_analysis_results.md` |
| `analysis/pca_elbow.py` | PCA elbow diagrams for CBS, KNMI validated, and combined feature groups |
| `analysis/pca_elbow_full_coverage.py` | PCA elbow for full-coverage (CBS + KNMI validated) feature subset |
| `analysis/pca_elbow_load_lags.py` | PCA elbow diagram for load lag features |
| `analysis/pca_covariance_matrix.py` | PCA covariance matrix visualisation |
| `analysis/regenerate_stl_full.py` | Regenerates STL decomposition plot for ENTSO-E over the full date range |
| `analysis/md_to_pdf.py` | Markdown → PDF conversion utility (used for `eda_report.md` → `eda_report.pdf`) |

**Run:**

```bash
# Interactive
python analysis/run_analysis.py

# Batch (Snellius)
sbatch analysis/run_analysis.slurm
```

---

## Forecasting Models

All forecasting models are located under `src/models/` and share a common evaluation protocol:

- **Target variable**: `entsoe_load_mw` (hourly Netherlands electricity load in MW)
- **Evaluation**: Rolling-Origin Cross-Validation (ROCV) with expanding training windows
  - ROCV origin starts at 2015-01-01, advances by 13 months per fold (cycles through calendar months to avoid seasonal sampling bias)
  - Minimum 2 years of training data
  - 10 folds total (fold 0–9)
- **Forecast horizons**: 60, 75, 90, 105, 120, 135, 150, 165, 180 days (baselines); 60, 120, 180 days (CMAT)
- **Metrics**: RMSE, MAE, MAPE, MASE, nMAE, nRMSE, PCC — all computed on original-scale MW values
- **Multi-seed**: Neural models (Seq2Seq, Prophet-LSTM, CMAT) use seeds {42, 123, 7} for reproducibility

### Baseline Models

**Location:** `src/models/baselines/`

Six baseline models replicated from the literature:

| # | Model | Reference | Architecture |
|---|-------|-----------|-------------|
| 1 | **van de Sande MLR** | van de Sande et al. | scikit-learn `LinearRegression` with 20 hand-picked + PCA features |
| 2 | **van de Sande Prophet-LSTM** | van de Sande et al. (Huang) | Prophet → LSTM(30, 90) ensemble |
| 3 | **Ashtar SARIMAX** | Ashtar et al. | statsmodels `SARIMAX(1,0,1)(1,0,1,168)` |
| 4 | **Ashtar SARIMAX-LSTM** | Ashtar et al. | SARIMAX residuals → PyTorch LSTM |
| 5 | **Ashtar Seq2Seq** | Ashtar et al. | Encoder-Decoder LSTM(64), teacher forcing |
| 6 | **Seasonal Naïve** | — | ŷ(t) = y(t − 365 days), no fitting required |

#### Key source files

| File | Purpose |
|------|---------|
| `config.py` | Unified configuration: paths, feature definitions, hyperparameters for all 6 models |
| `data_loader.py` | Data loading, preprocessing (IQR/Winsorization/MICE/forward-fill), feature engineering, ROCV fold generation |
| `evaluation.py` | 7-metric evaluation, crash-safe CSV checkpointing, per-horizon aggregation, metric evolution plots |
| `mlr.py` | MLR model implementation |
| `prophet_lstm.py` | Prophet-LSTM ensemble (reference-faithful Huang architecture) |
| `sarimax.py` | SARIMAX model implementation |
| `sarimax_lstm.py` | SARIMAX-LSTM residual stacking |
| `seq2seq.py` | Encoder-Decoder LSTM with iterative multi-chunk decoding |
| `train_baselines.py` | Unified training script for all models |
| `train_mlr.py` | Dedicated MLR trainer with explicit 36-feature ROCV |
| `train_prophet_lstm_v2.py` | Dedicated Prophet-LSTM trainer with explicit 36-feature ROCV |
| `train_seq2seq_v2.py` | Dedicated Seq2Seq trainer with multi-seed/multi-horizon ROCV |
| `train_seasonal_naive.py` | Seasonal naïve baseline evaluation |
| `merge_shards.py` | Merges per-(model, horizon, seed) SLURM shard CSVs into combined results |
| `aggregate_results.py` | Post-processing: unified CSV, summary tables, LaTeX comparison tables |

#### Running Baselines

```bash
# Unified runner (all models, all horizons, all folds):
python -u src/models/baselines/train_baselines.py

# Specific model and horizon:
python -u src/models/baselines/train_baselines.py --model van_de_sande_mlr --horizon 60

# Quick smoke test (1 fold, 1 horizon):
python -u src/models/baselines/train_baselines.py --model van_de_sande_mlr --quick

# Dedicated MLR (with 36-feature set):
python -u src/models/baselines/train_mlr.py --horizon 60

# SLURM batch:
sbatch src/models/baselines/run_mlr.slurm
sbatch src/models/baselines/run_seq2seq_3seeds_3horizons.slurm
sbatch src/models/baselines/run_prophet_lstm_3seeds_3horizons.slurm

# Post-processing — merge shards and aggregate:
python -m src.models.baselines.merge_shards --model-name ashtar_seq2seq
python -u src/models/baselines/aggregate_results.py
```

#### Results

Results are stored in `src/models/baselines/results/`:
- `rocv_results_{model_name}.csv` — per-fold metrics for each (model, horizon, seed)
- `rocv_results_{model_name}.json` — full results with predictions
- `run_metadata_{model_name}.json` — experiment metadata (timing, config, versions)
- `_shards/` — per-SLURM-task shard CSVs (crash-safe incremental persistence)
- `plots/` — metric evolution plots (metrics across ROCV folds)
- `seasonal_naive/` — seasonal naïve baseline results
- `drift/`, `naive_24h/` — additional naïve baseline results

---

### CMAT — Cross-Modal Attention Transformer

**Location:** `src/models/cmat/`

A novel Cross-Modal Attention Transformer that fuses tabular time-series features with 2D satellite nighttime-light imagery via dedicated cross-attention. Implements four ablation variants:

| Variant | Tag | Description |
|---------|-----|-------------|
| **CMAT-Tab** | `tab` | Tabular self-attention only (spatial branch disabled) |
| **CMAT-NTL** | `ntl` | Tabular + scalar NTL aggregate (no spatial image branch) |
| **CMAT-Early** | `early` | Both branches, early fusion via concatenation + shared self-attention |
| **CMAT-Full** | `full` | Both branches, dedicated cross-modal attention |

#### Architecture (from `model.py`)

```
Phase 1: Modality-specific feature encoding
  → Tabular: continuous features + learned categorical embeddings
  → Spatial: CNN backbone on 768×992 NTL images (CMAT-Full/Early only)

Phase 2: Temporal self-attention + cross-modal fusion
  → Windowed multi-head self-attention on tabular tokens
  → Cross-attention: tabular queries attend to spatial keys/values (CMAT-Full)
  → Early fusion: concatenation + shared SA (CMAT-Early)

Phase 3: Factorised probabilistic decoder
  → Point prediction (MSE loss)
```

#### Key source files

| File | Purpose |
|------|---------|
| `config.py` | All hyperparameters, feature definitions, search space, paths, ROCV protocol |
| `model.py` | PyTorch model implementing all 4 variants |
| `train.py` | Training pipeline: dataset creation, AdamW + cosine annealing, early stopping, crash-safe file-locked persistence |
| `ntl_images.py` | `NTLImageStore`: reads VNP46A2 HDF5 → fixed-size 2D tensors with LRU cache, NL polygon masking |
| `search.py` | Optuna hyperparameter search (per-horizon, per-variant) |
| `run_rocv.py` | Full ROCV evaluation across all fold/horizon combinations |
| `feature_importance.py` | Permutation feature importance (ΔRMSE) for trained checkpoints |
| `submit_fleet_wave.sh` | Fleet job submission for per-fold GPU ROCV on `gpu_a100` |

#### Features (36 total = 32 continuous + 4 categorical)

- **16 core continuous**: 6 cyclical temporal (sin/cos for hour, day-of-week, month), 3 ordinal temporal, 4 autoregressive load lags, 3 CCF-optimal weather lags
- **PCA components**: fitted **per-fold** on training data only (95% variance threshold) to prevent future data leakage
- **4 categorical**: embeddings for temporal/calendar features

#### Running CMAT

```bash
# ROCV evaluation (single variant + horizon):
python -m src.models.cmat.run_rocv --variant full --horizon 60 --resume

# Quick validation (1 fold):
python -m src.models.cmat.run_rocv --variant full --horizon 60 --quick

# Hyperparameter search:
python -m src.models.cmat.search --variant tab --horizon 60

# Feature importance:
python -m src.models.cmat.feature_importance --variant tab --horizon 60 --fold 0 --seed 42

# SLURM batch (GPU):
sbatch src/models/cmat/run_cmat_full_rocv.slurm

# Fleet submission (parallel per-fold GPU jobs):
bash src/models/cmat/submit_fleet_wave.sh 5      # folds 1–5
bash src/models/cmat/submit_fleet_wave.sh 9 6    # folds 6–9
```

#### Results

Results are stored in `src/models/cmat/results/`:
- `best_config_{variant}_h{horizon}d.json` — best Optuna configuration per variant/horizon
- `trial_history_{variant}_h{horizon}d.jsonl` — full Optuna search history
- `rocv_results_cmat_{variant}.csv` / `.json` — per-fold ROCV metrics
- `feature_importance_{variant}_h{horizon}d.csv` — permutation importance rankings
- `_shards/` — per-SLURM-task shard CSVs (crash-safe for multi-GPU fleet jobs)
- `checkpoints_v2/` — saved PyTorch model checkpoints (`.pt` files)

---

## Results & Report Generation

### LaTeX Table Generators

**Location:** `src/analysis/`

Scripts that read live result CSVs and generate publication-ready LaTeX tables for the thesis:

| Script | Output | Description |
|--------|--------|-------------|
| `generate_results_tables.py` | `reports/results_tables.tex` | Per-fold ROCV longtables comparing all baselines and CMAT variants across 5 metrics (RMSE, MAPE, MAE, MASE, PCC); best per row bolded, multi-seed mean±std |
| `generate_fi_tables.py` | `reports/feature_importance_tables.tex` | Top-10 permutation feature importance per CMAT variant per horizon |
| `generate_complexity_table.py` | `reports/complexity_table.tex` | Computational complexity: parameters, mean train/inference wall-clock, epochs per model |

```bash
# Regenerate all tables:
.venv/bin/python3 src/analysis/generate_results_tables.py > reports/results_tables.tex
.venv/bin/python3 src/analysis/generate_fi_tables.py > reports/feature_importance_tables.tex
.venv/bin/python3 src/analysis/generate_complexity_table.py > reports/complexity_table.tex
```

### Reports Directory

**Location:** `reports/`

Contains generated LaTeX fragments and thesis components:

| File | Description |
|------|-------------|
| `results_tables.tex` | ROCV comparison tables (generated) |
| `feature_importance_tables.tex` | Feature importance tables (generated) |
| `complexity_table.tex` | Computational complexity table (generated) |
| `forward_pass.tex` | CMAT forward-pass description |
| `hyperparams_tables.tex` | Hyperparameter summary tables |
| `pca_inputs_table.tex` | PCA input feature tables |
| `feature_summary.tex` | Feature summary for thesis |
| `methodology.tex` | Methodology chapter |
| `introduction.tex` | Introduction chapter |
| `related_work.tex` | Related work chapter |
| `methods_snippets.tex` | Reusable methodology text snippets |

---

## Running on SLURM (Snellius)

### Pipeline Jobs

Each pipeline phase has a corresponding `.slurm` batch script. You can submit them individually or chain them with dependencies.

#### Individual Submission

```bash
# Phase 1 — Extract (96 cores, 4-hour time limit)
sbatch src/pipeline/phase1.slurm

# Phase 2 — Aggregate (128 cores, exclusive node, 2-hour limit)
sbatch src/pipeline/phase2.slurm

# Phase 3 — Merge (32 cores, 64 GB explicit memory, 30-minute limit)
sbatch src/pipeline/phase3.slurm
```

#### Chained Submission (Recommended)

Submit Phase 2 after Phase 1, and Phase 3 after Phase 2, using SLURM dependency chains:

```bash
JOB1=$(sbatch --parsable src/pipeline/phase1.slurm)
JOB2=$(sbatch --parsable --dependency=afterok:$JOB1 src/pipeline/phase2.slurm)
JOB3=$(sbatch --parsable --dependency=afterok:$JOB2 src/pipeline/phase3.slurm)
echo "Phase 1: $JOB1, Phase 2: $JOB2, Phase 3: $JOB3"
```

If Phase 1 has already completed (outputs are cached), you can skip it:

```bash
JOB2=$(sbatch --parsable src/pipeline/phase2.slurm)
JOB3=$(sbatch --parsable --dependency=afterok:$JOB2 src/pipeline/phase3.slurm)
echo "Phase 2: $JOB2, Phase 3: $JOB3"
```

### Model Training Jobs

#### Baseline SLURM Scripts

| Script | Partition | Resources | Description |
|--------|-----------|-----------|-------------|
| `src/models/baselines/run_mlr.slurm` | `rome` | 16 CPUs, 64 GB | MLR with explicit 35-feature ROCV |
| `src/models/baselines/run_mlr_v3.slurm` | `rome` | 16 CPUs, 64 GB | MLR v3 variant |
| `src/models/baselines/run_seq2seq_3seeds_3horizons.slurm` | `rome` | — | Seq2Seq multi-seed/horizon sweep |
| `src/models/baselines/run_prophet_lstm_3seeds_3horizons.slurm` | `rome` | — | Prophet-LSTM multi-seed/horizon sweep |

#### CMAT SLURM Scripts

| Script | Partition | Resources | Description |
|--------|-----------|-----------|-------------|
| `run_cmat_full_rocv.slurm` | `gpu_a100` | 1 GPU, 18 CPUs, 96 GB, 24h | CMAT-Full 3-seed × 3-horizon ROCV (array tasks) |
| `run_cmat_full_fold.slurm` | `gpu_a100` | 1 GPU | Single-fold CMAT-Full training |
| `run_cmat_full_search.slurm` | `gpu_a100` | 1 GPU | Optuna hyperparameter search |
| `run_cmat_full_fi.slurm` | `gpu_a100` | 1 GPU | Feature importance for CMAT-Full |
| `run_cmat_tabntl_full_rocv.slurm` | `gpu_a100` | 1 GPU | CMAT-Tab and CMAT-NTL full ROCV |
| `run_cmat_tabntl_search50.slurm` | `gpu_a100` | 1 GPU | Tab/NTL Optuna search (50 trials) |
| `run_cmat_tabntl_fi.slurm` | `gpu_a100` | 1 GPU | Feature importance for Tab/NTL variants |

### SLURM Resource Allocation Summary

| Job | SLURM Script | Partition | CPUs | GPU | Memory | Time Limit |
|---|---|---|---|---|---|---|
| Pipeline Phase 1 | `src/pipeline/phase1.slurm` | `rome` | 96 | — | Default | 4 hours |
| Pipeline Phase 2 | `src/pipeline/phase2.slurm` | `rome` | 128 | — | Full node | 2 hours |
| Pipeline Phase 3 | `src/pipeline/phase3.slurm` | `rome` | 32 | — | 64 GB | 30 minutes |
| EDA | `analysis/run_analysis.slurm` | `rome` | 8 | — | 48 GB | 1 hour |
| Baseline MLR | `baselines/run_mlr.slurm` | `rome` | 16 | — | 64 GB | 4 hours |
| CMAT-Full ROCV | `cmat/run_cmat_full_rocv.slurm` | `gpu_a100` | 18 | 1× A100 | 96 GB | 24 hours |

---

## Observability — Logs, Jobs & Efficiency

### Log Files

Each SLURM job writes its log to the submit directory with the pattern `phase<N>_<JOBID>.log`:

```bash
# View the most recent Phase 2 log
cat logs/phase2_22050133.log

# Stream a log in real-time while a job is running
tail -f logs/phase2_$(squeue -u $USER -h -o "%i" | head -1).log
```

Download scripts log to dedicated files (at repo root):

```bash
tail -f download_A1.log    # VIIRS A1 download progress
tail -f download.log        # VIIRS A2 download progress (legacy name)
tail -f download_knmi.log   # KNMI download progress
```

### Job Monitoring

```bash
# Check your queued/running jobs
squeue -u $USER

# Get detailed job efficiency metrics AFTER a job completes
seff <JOBID>
```

`seff` reports CPU utilisation, wall-clock time, and memory usage. Example output:

```
Job ID: 22050133
State: COMPLETED (exit code 0)
CPU Utilized: 00:00:07
CPU Efficiency: 0.22% of 00:53:20 core-walltime
Job Wall-clock time: 00:00:25
Memory Utilized: 4.49 MB
```

> **Note:** `seff` only provides accurate statistics after a job has fully completed. While running, it will show `0%` efficiency.

### Data Quality Reports

Every processing stage writes a `data_quality.json` alongside its Parquet output:

```bash
# Phase 1 quality reports
cat data/processing_1/viirs_a2/data_quality.json
cat data/processing_1/viirs_a1/data_quality.json
cat data/processing_1/cbs_energy/data_quality.json
cat data/processing_1/cbs_gdp/data_quality.json
cat data/processing_1/cbs_cpi/data_quality.json
cat data/processing_1/cbs_gep/data_quality.json
cat data/processing_1/entsoe/data_quality.json

# Phase 2 quality reports
cat data/processing_2/viirs_a2_daily/data_quality.json
cat data/processing_2/viirs_a2_all_daily/data_quality.json
cat data/processing_2/viirs_a1_daily/data_quality.json
cat data/processing_2/cbs_combined/data_quality.json
cat data/processing_2/entsoe/data_quality.json
cat data/processing_2/knmi/data_quality.json
cat data/processing_2/knmi_validated/data_quality.json

# Phase 3 final quality report
cat data/processed/data_quality.json
```

Each report includes: row/column counts, per-column null percentages and dtypes, date ranges, output size in bytes/MB, and stage-specific metrics (pixel coverage fractions, year coverage percentages, load MW statistics, performance timings).

---

## Directory Layout

```
energy-demand-forecast-nl/                # Repository root (on Snellius)
├── .env                                  # API tokens (git-ignored)
├── .gitignore
├── README.md
├── requirements.in                       # Top-level Python dependencies
├── requirements.txt                      # Locked/pinned versions (pip-compile)
├── setup_env.sh                          # Venv creation/update + Jupyter kernel setup
│
├── src/
│   ├── pipeline/                         # ETL pipeline: scripts + SLURM jobs
│   │   ├── phase1_extract.py             # Phase 1: Raw → source-level Parquet
│   │   ├── phase2_aggregate.py           # Phase 2: Source-level → aggregated Parquet
│   │   ├── phase3_merge.py               # Phase 3: Aggregated → final hourly dataset
│   │   ├── phase1.slurm                  # SLURM job script for Phase 1
│   │   ├── phase2.slurm                  # SLURM job script for Phase 2
│   │   ├── phase3.slurm                  # SLURM job script for Phase 3
│   │   └── jupyter_lab.slurm             # JupyterLab compute-node server
│   │
│   ├── download/                         # Data download utilities
│   │   ├── download_nl_viirs.py          # VIIRS HDF5 download script
│   │   ├── download_nl_knmi.py           # KNMI weather data download script
│   │   ├── download_nl_holidays.py       # OpenHolidays API holiday download script
│   │   └── generate_nl_public_holidays.py # Python holidays library generator
│   │
│   ├── models/                           # Forecasting models
│   │   ├── __init__.py
│   │   │
│   │   ├── baselines/                    # Six literature baseline models
│   │   │   ├── config.py                 # Unified config (paths, features, hyperparams)
│   │   │   ├── data_loader.py            # Data loading, preprocessing, ROCV folds
│   │   │   ├── evaluation.py             # Metrics, CSV checkpointing, aggregation
│   │   │   ├── mlr.py                    # Model 1: Multiple Linear Regression
│   │   │   ├── prophet_lstm.py           # Model 2: Prophet-LSTM ensemble
│   │   │   ├── sarimax.py               # Model 3: SARIMAX
│   │   │   ├── sarimax_lstm.py          # Model 4: SARIMAX-LSTM residual stacking
│   │   │   ├── seq2seq.py               # Model 5: Encoder-Decoder LSTM
│   │   │   ├── train_baselines.py       # Unified trainer for all baselines
│   │   │   ├── train_mlr.py             # Dedicated MLR ROCV trainer
│   │   │   ├── train_prophet_lstm_v2.py # Dedicated Prophet-LSTM ROCV trainer
│   │   │   ├── train_seq2seq_v2.py      # Dedicated Seq2Seq ROCV trainer
│   │   │   ├── train_seasonal_naive.py  # Seasonal naïve baseline
│   │   │   ├── merge_shards.py          # Merge SLURM shard CSVs
│   │   │   ├── aggregate_results.py     # Post-processing & comparison tables
│   │   │   ├── run_mlr.slurm            # MLR SLURM batch script
│   │   │   ├── run_seq2seq_3seeds_3horizons.slurm
│   │   │   ├── run_prophet_lstm_3seeds_3horizons.slurm
│   │   │   └── results/                 # ROCV results, metadata, shards, plots
│   │   │
│   │   └── cmat/                         # Cross-Modal Attention Transformer
│   │       ├── config.py                 # Variants, hyperparams, search space, paths
│   │       ├── model.py                  # PyTorch model (4 ablation variants)
│   │       ├── train.py                  # Training: dataset, loss, optimiser, early stopping
│   │       ├── ntl_images.py            # NTL image loader (HDF5 → 2D tensor, LRU cache)
│   │       ├── search.py                # Optuna hyperparameter search
│   │       ├── run_rocv.py              # Full ROCV evaluation
│   │       ├── feature_importance.py    # Permutation feature importance
│   │       ├── submit_fleet_wave.sh     # Fleet GPU job submission
│   │       ├── run_cmat_full_rocv.slurm # GPU ROCV SLURM script
│   │       ├── run_cmat_full_search.slurm
│   │       ├── run_cmat_full_fi.slurm
│   │       ├── run_cmat_tabntl_*.slurm  # Tab/NTL variant SLURM scripts
│   │       └── results/                 # Best configs, trial histories, ROCV results,
│   │           ├── _shards/             #   per-fold shard CSVs
│   │           └── checkpoints_v2/      #   saved model checkpoints (.pt)
│   │
│   └── analysis/                         # Result table generators
│       ├── generate_results_tables.py    # ROCV comparison LaTeX tables
│       ├── generate_fi_tables.py         # Feature importance LaTeX tables
│       └── generate_complexity_table.py  # Computational complexity LaTeX table
│
├── analysis/                             # Exploratory data analysis
│   ├── run_analysis.py                   # Batch EDA script (10 sections, all figures)
│   ├── run_analysis.slurm                # SLURM job script for EDA
│   ├── eda_results.json                  # Machine-readable EDA results (generated)
│   ├── eda_report.md                     # Formatted EDA report (generated)
│   ├── eda_report.pdf                    # PDF version of EDA report (generated)
│   ├── gap_analysis.py                   # KNMI data gap analysis utility
│   ├── gap_analysis_results.md           # Gap analysis results
│   ├── pca_elbow.py                      # PCA elbow diagrams (CBS, KNMI, combined)
│   ├── pca_elbow_full_coverage.py        # PCA elbow for full-coverage features
│   ├── pca_elbow_load_lags.py            # PCA elbow for load lag features
│   ├── pca_covariance_matrix.py          # PCA covariance matrix visualisation
│   ├── regenerate_stl_full.py            # Full-range STL decomposition regeneration
│   ├── md_to_pdf.py                      # Markdown → PDF conversion utility
│   └── figures/                          # Generated PNG figures (~60 plots)
│
├── reports/                              # Generated LaTeX fragments for the thesis
│   ├── results_tables.tex                # ROCV comparison tables (generated)
│   ├── feature_importance_tables.tex     # Feature importance tables (generated)
│   ├── complexity_table.tex              # Computational complexity table (generated)
│   ├── forward_pass.tex                  # CMAT forward-pass description
│   ├── hyperparams_tables.tex            # Hyperparameter summary tables
│   ├── pca_inputs_table.tex              # PCA input feature tables
│   ├── feature_summary.tex               # Feature summary
│   ├── methodology.tex                   # Methodology chapter
│   ├── introduction.tex                  # Introduction chapter
│   ├── related_work.tex                  # Related work chapter
│   └── methods_snippets.tex              # Reusable methodology text snippets
│
├── docs/                                 # Reference documents
│   ├── feature_dictionary.csv            # Complete feature column definitions (145 columns)
│   ├── pipeline_architecture.md          # Pipeline design document
│   ├── baseline_config.yaml              # Baseline experiment configuration reference
│   ├── methodology.tex                   # LaTeX methodology chapter (draft)
│   ├── paper_comparison.xlsx             # Literature comparison spreadsheet
│   └── Thesis_Design_Malak_Khan-2.pdf    # Thesis design document
│
├── figures/                              # EDA figures (repo-root copy, git-ignored)
│
├── ARCHIVE/                              # Pre-leakage-fix material (moved 2026-08-27)
│   ├── README.md                         # Explanation of why this material is stale
│   ├── baselines/                        # Old baseline results (108 files)
│   ├── cmat/                             # Old CMAT results, configs, scripts (372 files)
│   ├── logs/                             # Old SLURM logs before 2026-08-25 (520 files)
│   └── misc/                             # One-off refactoring helper scripts
│
├── data/                                 # Pipeline outputs (inside repo)
│   ├── geo/                              # Geospatial reference data
│   │   └── gadm41_NLD_0.json            # GADM 4.1 Netherlands boundary (Level 0, WGS84)
│   ├── processing_1/                     # Phase 1 output (source-level Parquet)
│   │   ├── viirs_a2/data/year=YYYY/      # Per-day VNP46A2 pixel Parquet
│   │   ├── viirs_a1/data/year=YYYY/      # Per-day VNP46A1 pixel Parquet
│   │   ├── cbs_energy/data/              # Monthly consumer tariffs (harmonized)
│   │   ├── cbs_gdp/data/                 # Monthly GDP/economic indicators + population
│   │   ├── cbs_cpi/data/                 # Monthly energy CPI (2015=100)
│   │   ├── cbs_gep/data/                 # Monthly gas & electricity prices by band
│   │   ├── cbs_macro/data/               # Additional CBS macroeconomic indicators
│   │   ├── entsoe/data/year=YYYY/        # Hourly NL load
│   │   ├── knmi/data/year=YYYY/          # Hourly meteorological observations (non-validated)
│   │   └── knmi_validated/data/year=YYYY/ # Hourly meteorological observations (validated)
│   │
│   ├── processing_2/                     # Phase 2 output (aggregated Parquet)
│   │   ├── viirs_a2_daily/data/year=YYYY/      # Daily VNP46A2 aggregates (selective: quality ≤ 1)
│   │   ├── viirs_a2_all_daily/data/year=YYYY/  # Daily VNP46A2 aggregates (non-selective: all non-fill)
│   │   ├── viirs_a1_daily/data/year=YYYY/      # Daily VNP46A1 aggregates
│   │   ├── cbs_combined/data/                  # Merged CBS (~70 columns)
│   │   ├── entsoe/data/year=YYYY/              # Re-partitioned ENTSO-E
│   │   ├── knmi/data/year=YYYY/                # Re-partitioned KNMI (non-validated)
│   │   └── knmi_validated/data/year=YYYY/      # Re-partitioned KNMI (validated)
│   │
│   └── processed/                        # Phase 3 final output
│       ├── nl_hourly_dataset.parquet/year=YYYY/  # Final unified dataset (145 columns)
│       └── data_quality.json             # Comprehensive quality report
│
├── logs/                                 # SLURM job output logs (git-ignored)
│
/projects/prjs2061/data/                  # Raw input data (outside repo — too large for git)
├── viirs/
│   ├── A1/                               # Raw VNP46A1 HDF5 files
│   └── A2/                               # Raw VNP46A2 HDF5 files
├── calendar/                             # Holiday calendar CSVs
│   ├── nl_holidays.csv                   # OpenHolidays API (public + school, 2020–2025)
│   └── nl_public_holidays.csv            # holidays library (public, 2012–2025)
├── cbs/                                  # CBS CSV source files
├── entso-e/                              # ENTSO-E Excel workbooks
├── knmi/                                 # KNMI hourly NetCDF files — non-validated (~100K files)
└── knmi_validated/                        # KNMI hourly NetCDF files — validated
```
