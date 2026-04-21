# Energy Demand Forecast — Netherlands

A three-phase Apache Spark / Python ETL pipeline for building an hourly Netherlands energy-demand dataset from heterogeneous raw sources. The pipeline ingests VIIRS satellite nighttime-lights imagery, CBS consumer energy tariffs and quarterly economic statistics, and ENTSO-E electricity load data, transforming them into a single, contiguous, year-partitioned Parquet dataset suitable for downstream forecasting models.

---

## Table of Contents

1. [Setup & Prerequisites](#setup--prerequisites)
2. [Data Download Scripts](#data-download-scripts)
3. [Data Processing Pipeline](#data-processing-pipeline)
   - [Phase 1 — Extract](#phase-1--extract)
   - [Phase 2 — Aggregate](#phase-2--aggregate)
   - [Phase 3 — Merge](#phase-3--merge)
4. [Running on SLURM (Snellius)](#running-on-slurm-snellius)
5. [Observability — Logs, Jobs & Efficiency](#observability--logs-jobs--efficiency)
6. [Directory Layout](#directory-layout)

---

## Setup & Prerequisites

### Platform

This project is designed to run on **Snellius** (SURF's national supercomputer). Key assumptions about the environment:

| Aspect | Detail |
|---|---|
| **Cluster** | Snellius (SURF), `rome` partition |
| **Nodes** | Single-node jobs; Rome nodes have **128 CPU cores**, **229 GB RAM** |
| **Filesystem** | GPFS at `/projects/prjs2061/` (shared project space) |
| **Scratch** | `$TMPDIR` / `$SLURM_TMPDIR` — node-local SSD, used for Spark shuffle spill |
| **Scheduler** | SLURM (sbatch / squeue / seff) |
| **Python** | 3.9+ (system Python on Snellius; a `pip-compile`-locked `requirements.txt` is provided) |
| **Java** | OpenJDK 17+ (required by PySpark/Spark 4.x; available via `module load` on Snellius) |

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
| `pandas` | Small-file parsing (CBS, ENTSO-E, quality metrics) |
| `pyarrow` | Direct Parquet I/O in Phase 1 (bypasses JVM overhead) |
| `pyspark` (4.x) | Distributed aggregation and joins in Phases 2 & 3 |
| `openpyxl` | Excel reading for ENTSO-E `.xlsx` workbooks |
| `requests` | KNMI Open Data API downloads |

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
| `KNMI_API_KEY` | `download_nl_knmi.py` (optional) | [KNMI Data Platform](https://dataplatform.knmi.nl/) → API Keys (a public fallback key is hard-coded) |

The VIIRS download script checks `LAADS_TOKEN` first, then falls back to `EDL_TOKEN`. At least one must be set.

---

## Data Download Scripts

Raw data files are downloaded into `/projects/prjs2061/data/` subdirectories. These scripts are run **interactively** (not via SLURM) and are designed to be resumable.

### `download_nl_viirs.py` — VIIRS Nighttime Lights (VNP46A1 / VNP46A2)

Downloads VIIRS Black Marble satellite imagery from NASA's [LAADS DAAC](https://ladsweb.modaps.eosdis.nasa.gov/) archive. Each file is an HDF5 granule for the **h18v03** MODIS sinusoidal tile, which covers Western Europe including the entire Netherlands.

**Usage:**

```bash
source .env

# Download A2 (gap-filled BRDF-corrected NTL) — used in the processing pipeline
python3 -u download_nl_viirs.py -p A2 -d /projects/prjs2061/data/viirs/A2 -s 2012-01-01

# Download A1 (raw at-sensor radiance) — supplementary
python3 -u download_nl_viirs.py -p A1 -d /projects/prjs2061/data/viirs/A1 -s 2012-01-01

# Download only specific days (re-download corrupt files, etc.)
python3 -u download_nl_viirs.py -p A2 -d /projects/prjs2061/data/viirs/A2 -s 2016-10-07 -e 2016-10-07
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
  nohup python3 -u download_nl_viirs.py -p A2 -d /projects/prjs2061/data/viirs/A2 >> download_A2.log 2>&1 &
  ```

### `download_nl_knmi.py` — KNMI Meteorological Data

Downloads hourly in-situ meteorological observations from the [KNMI Open Data Platform](https://dataplatform.knmi.nl/) via their REST API.

**Usage:**

```bash
python3 download_nl_knmi.py --dest /projects/prjs2061/data/knmi --start-year 2012
```

**CLI options:**

| Flag | Default | Description |
|---|---|---|
| `-k`, `--key` | `$KNMI_API_KEY` or built-in public key | API key for the KNMI data platform |
| `-d`, `--dataset` | `uurwaarden` | KNMI dataset name |
| `-v`, `--version` | `1` | Dataset version |
| `--dest` | `/projects/prjs2061/data/knmi` | Output directory |
| `--start-year` | `2012` | Only download files for this year and later |
| `--overwrite` | `false` | Re-download even if file exists locally |

**Key behaviour:**

- Paginates through the file listing API (500 files per page), filters by year from filename.
- Uses concurrent `ThreadPoolExecutor` downloads (up to 10 threads for small files, 1 for large).
- Handles HTTP 429 rate-limiting with automatic 20-second backoff retries.

### Manual Downloads (CBS & ENTSO-E)

The CBS and ENTSO-E source files are downloaded manually from their respective platforms and placed in the expected directories:

| Source | Expected Location | Format | Notes |
|---|---|---|---|
| **CBS Consumer Tariffs (old)** | `data/cbs/Average_energy_prices_for_consumers__2018*.csv` | Semicolon CSV, 6-line header | Old schema 2018–2023 |
| **CBS Consumer Tariffs (new)** | `data/cbs/Average_energy_prices_for_consumers_2*.csv` | Semicolon CSV, 6-line header | New schema 2021+ |
| **CBS GDP Quarterly** | `data/cbs/GDP__output_and_expenditures__changes__*.csv` | Semicolon CSV, 4-line header | Wide-format: 18 indicators × 2 metrics, quarterly 1995–2025 |
| **CBS Population** | `data/cbs/Population (x million).csv` | Semicolon CSV | Annual population in millions |
| **ENTSO-E Load** | `data/entso-e/*.xlsx` | Excel workbooks | Wide format (2006–2015) + long format (2015+) |

> **Note**: File names contain datestamps from CBS's export system (e.g. `..._21042026_021720.csv`). The pipeline uses glob patterns to locate them regardless of the exact datestamp suffix.

---

## Data Processing Pipeline

The pipeline is organized in three sequential phases. Each phase reads from the previous phase's output directory and writes to the next. All phases support **idempotent re-runs** — already-processed outputs are skipped unless `--force` is passed.

```
data/viirs/A2/*.h5 ──┐
data/cbs/*.csv ──────┤──▶ Phase 1 ──▶ data/processing_1/ ──▶ Phase 2 ──▶ data/processing_2/ ──▶ Phase 3 ──▶ data/processed/
data/entso-e/*.xlsx ─┘    (Extract)     (source-level           (Aggregate)    (aggregated/            (Merge)     nl_hourly_dataset.parquet
                                         Parquet)                               combined Parquet)
```

---

### Phase 1 — Extract

**Script:** `phase1_extract.py`

Extracts and cleans raw source files into standardised, source-level Parquet files. Runs **four sub-stages** sequentially:

#### 1A: VIIRS A2 Extraction

Reads VNP46A2 HDF5 files, crops to the Netherlands bounding box (`50.75°N–53.55°N`, `3.35°E–7.25°E`), and writes one Parquet file per day.

- **Parallelism**: `multiprocessing.ProcessPoolExecutor` — each worker processes one HDF5 file independently. On a 96-core node this achieves near-linear speedup.
- **Spatial masking**: Computes NL row/col indices once from the h18v03 sinusoidal grid, then reuses for all files (the grid is identical across the tile).
- **Caching**: Existing Parquet outputs are skipped (unless `--force`); partial writes are prevented via atomic `tmp → rename`.
- **Retry**: HDF5 opens retry up to 3 times with exponential backoff for transient GPFS errors.

**Output schema** — `data/processing_1/viirs_a2/data/year=YYYY/day_YYYYDDD.parquet`:

| Column | Type | Description |
|---|---|---|
| `date` | `date` | Observation date |
| `row_idx` | `int32` | VIIRS grid row index |
| `col_idx` | `int32` | VIIRS grid column index |
| `lat` | `float32` | Latitude (WGS84) |
| `lon` | `float32` | Longitude (WGS84) |
| `ntl_radiance` | `float32` | Gap-filled BRDF-corrected NTL (nW/cm²/sr); `NaN` for fill pixels |
| `quality_flag` | `uint8` | VNP46A2 mandatory quality flag (0 = best, 1 = good, 2+ = degraded) |
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

#### 1D: ENTSO-E Load Extraction

Reads ENTSO-E Excel workbooks (both legacy wide format and modern long format), filters to Netherlands (`NL`), deduplicates to hourly resolution, and writes year-partitioned Parquet.

**Output schema** — `data/processing_1/entsoe/data/year=YYYY/entsoe.parquet`:

| Column | Type | Description |
|---|---|---|
| `timestamp_utc` | `datetime64[us]` | Hourly timestamp in UTC (timezone-naive) |
| `entsoe_load_mw` | `float64` | Netherlands electricity load (MW) |

#### CLI Options

```bash
python phase1_extract.py [--data-root /path/to/data] [--workers N] [--force] [--start-method fork|spawn]
```

| Flag | Default | Description |
|---|---|---|
| `--data-root` | `/projects/prjs2061/data` | Root data directory |
| `--workers` | All CPUs | Number of parallel worker processes |
| `--force` | `false` | Re-process files even if output already exists |
| `--start-method` | `fork` | Python multiprocessing start method |

#### Quality Reports

Each sub-stage writes a `data_quality.json` alongside its output (e.g. `data/processing_1/viirs_a2/data_quality.json`) containing row counts, column-level null statistics, date ranges, and performance metrics.

---

### Phase 2 — Aggregate

**Script:** `phase2_aggregate.py`

Reads Phase 1 outputs and produces aggregated/combined tables using **PySpark** in local mode. Runs three sub-stages:

#### 2A: VIIRS A2 Daily Aggregation

Reads the NL pixel-level Parquet (~630K pixels/day × ~5000 days) and aggregates to **one row per day** with spatial summary statistics. Uses column pruning (only reads 4 of 8 columns) to halve I/O.

**Output schema** — `data/processing_2/viirs_a2_daily/data/year=YYYY/part-*.parquet`:

| Column | Type | Description |
|---|---|---|
| `date` | `date` | Observation date |
| `ntl_mean` | `double` | Mean NTL radiance of valid pixels (quality ≤ 1, not fill) |
| `ntl_sum` | `double` | Sum of NTL radiance across valid pixels |
| `ntl_valid_count` | `int` | Count of valid pixels |
| `ntl_fill_count` | `int` | Count of fill-value pixels (sensor/retrieval gaps) |
| `ntl_invalid_count` | `int` | Count of quality-degraded pixels (cloud/atmosphere) |

#### 2B: CBS Combined

Outer-joins CBS energy tariffs (monthly, 2018–2026) and CBS GDP indicators (monthly, 1996–2025) into a single monthly-resolution table on `(year, month)`. Both sources are monthly-resolution thanks to forward-filling in Phase 1, producing a clean join with ~400 rows and ~50 `cbs_*` columns.

**Output schema** — `data/processing_2/cbs_combined/data/part-*.parquet`:

All columns from both CBS energy and CBS GDP are carried through. The combined table includes:
- The 13 `cbs_gas_*` / `cbs_elec_*` tariff columns (non-null for 2018–2026)
- The 35 `cbs_*_yy` / `cbs_*_qq` GDP indicator columns (non-null for 1996–2025)
- `cbs_population_million` (non-null for years with CBS population data)
- `year`, `month` as the join keys

#### 2C: ENTSO-E Pass-through

Re-partitions ENTSO-E hourly data by year with one file per partition and generates a Phase 2 quality report (same schema as Phase 1 ENTSO-E output).

#### CLI Options

```bash
python phase2_aggregate.py [--data-root /path/to/data] [--workers N] [--force] [--driver-memory 64g]
```

| Flag | Default | Description |
|---|---|---|
| `--data-root` | `/projects/prjs2061/data` | Root data directory |
| `--workers` | All CPUs | Spark local-mode thread count |
| `--driver-memory` | `64g` | Spark driver memory (JVM heap) |
| `--force` | `false` | Re-run stages even if `_SUCCESS` marker exists |

#### Spark Configuration

Phase 2 (and Phase 3) use Spark in `local[N]` mode with: Adaptive Query Execution (AQE) for automatic shuffle partition coalescing and skew handling, node-local SSD (`$TMPDIR`) for shuffle spill (GPFS is catastrophically slow for Spark shuffle I/O), and Arrow-based Spark↔Pandas conversion.

#### Idempotency

Each sub-stage checks for Spark's `_SUCCESS` marker file. If present, the stage is skipped. Use `--force` to re-run.

---

### Phase 3 — Merge

**Script:** `phase3_merge.py`

Joins all Phase 2 outputs onto a **contiguous hourly UTC timestamp spine** (2012-01-01 00:00 → today 23:00) to produce the final unified dataset.

**Join strategy:**

| Source | Join Key | Join Type |
|---|---|---|
| ENTSO-E (hourly) | `timestamp` | Broadcast left-join |
| VIIRS A2 (daily) | `date` | Broadcast left-join |
| CBS Combined (monthly) | `(year, month)` | Broadcast left-join |

All source tables are small enough to be broadcast; no shuffles occur.

**Final output** — `data/processed/nl_hourly_dataset.parquet/year=YYYY/part-*.parquet`:

The final dataset contains ~60 columns. Column ordering is deterministic:

1. `timestamp` — hourly UTC timestamp (primary key)
2. `entsoe_load_mw` — **target variable** (electricity load in MW)
3. VIIRS satellite aggregates (5 columns: `ntl_mean`, `ntl_sum`, `ntl_valid_count`, `ntl_fill_count`, `ntl_invalid_count`)
4. CBS gas tariff columns (5 columns: transport rate, fixed supply rate, ODE tax, energy tax, total tax)
5. CBS electricity tariff columns (8 columns: transport rate, fixed supply rate, dynamic rates, ODE tax, energy tax, total tax, tax refund)
6. CBS GDP headline indicators (`cbs_gdp_yy`, `cbs_gdp_qq`, `cbs_gdp_wda_yy`, `cbs_gdp_wda_qq`)
7. CBS population (`cbs_population_million`)
8. All remaining `cbs_*` columns (auto-appended in sorted order — forward-compatible with new indicators)
9. Temporal features (9 columns: `year`, `month`, `day`, `hour`, `day_of_week`, `is_weekend`, `day_of_year`, `week_of_year`, `quarter`)

**Coverage tracking**: Phase 3 logs separate coverage metrics for CBS tariffs (tracked via `cbs_gas_total_tax`) and CBS GDP (tracked via `cbs_gdp_yy`), alongside ENTSO-E and VIIRS coverage, and writes them to `data_quality.json`.

#### CLI Options

```bash
python phase3_merge.py [--data-root /path/to/data] [--start 2012-01-01] [--end 2026-04-20]
                       [--workers N] [--driver-memory 64g] [--force]
```

| Flag | Default | Description |
|---|---|---|
| `--start` | `2012-01-01` | Spine start date |
| `--end` | Today (UTC) | Spine end date |
| `--workers` | All CPUs | Spark local-mode thread count |
| `--driver-memory` | `64g` | Spark driver memory |
| `--force` | `false` | Re-run even if final output exists |

---

## Running on SLURM (Snellius)

Each phase has a corresponding `.slurm` batch script. You can submit them individually or chain them with dependencies.

### Individual Submission

```bash
# Phase 1 — Extract (96 cores, 4-hour time limit)
sbatch phase1.slurm

# Phase 2 — Aggregate (128 cores, exclusive node, 2-hour limit)
sbatch phase2.slurm

# Phase 3 — Merge (32 cores, 64 GB explicit memory, 30-minute limit)
sbatch phase3.slurm
```

### Chained Submission (Recommended)

Submit Phase 2 after Phase 1, and Phase 3 after Phase 2, using SLURM dependency chains:

```bash
JOB1=$(sbatch --parsable phase1.slurm)
JOB2=$(sbatch --parsable --dependency=afterok:$JOB1 phase2.slurm)
JOB3=$(sbatch --parsable --dependency=afterok:$JOB2 phase3.slurm)
echo "Phase 1: $JOB1, Phase 2: $JOB2, Phase 3: $JOB3"
```

If Phase 1 has already completed (outputs are cached), you can skip it:

```bash
JOB2=$(sbatch --parsable phase2.slurm)
JOB3=$(sbatch --parsable --dependency=afterok:$JOB2 phase3.slurm)
echo "Phase 2: $JOB2, Phase 3: $JOB3"
```

### SLURM Resource Allocation Summary

| Phase | SLURM Script | Partition | CPUs | Memory | Time Limit | Node |
|---|---|---|---|---|---|---|
| 1 | `phase1.slurm` | `rome` | 96 | Default (shared) | 4 hours | Shared |
| 2 | `phase2.slurm` | `rome` | 128 | Full node | 2 hours | **Exclusive** |
| 3 | `phase3.slurm` | `rome` | 32 | 64 GB | 30 minutes | Shared |

---

## Observability — Logs, Jobs & Efficiency

### Log Files

Each SLURM job writes its log to the submit directory with the pattern `phase<N>_<JOBID>.log`:

```bash
# View the most recent Phase 2 log
cat phase2_22050133.log

# Stream a log in real-time while a job is running
tail -f phase2_$(squeue -u $USER -h -o "%i" | head -1).log
```

Download scripts log to dedicated files:

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
cat /projects/prjs2061/data/processing_1/viirs_a2/data_quality.json
cat /projects/prjs2061/data/processing_1/cbs_energy/data_quality.json
cat /projects/prjs2061/data/processing_1/cbs_gdp/data_quality.json
cat /projects/prjs2061/data/processing_1/entsoe/data_quality.json

# Phase 2 quality reports
cat /projects/prjs2061/data/processing_2/viirs_a2_daily/data_quality.json
cat /projects/prjs2061/data/processing_2/cbs_combined/data_quality.json
cat /projects/prjs2061/data/processing_2/entsoe/data_quality.json

# Phase 3 final quality report
cat /projects/prjs2061/data/processed/data_quality.json
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
│
├── download_nl_viirs.py                  # VIIRS HDF5 download script
├── download_nl_knmi.py                   # KNMI weather data download script
│
├── phase1_extract.py                     # Phase 1: Raw → source-level Parquet
├── phase2_aggregate.py                   # Phase 2: Source-level → aggregated Parquet
├── phase3_merge.py                       # Phase 3: Aggregated → final hourly dataset
│
├── phase1.slurm                          # SLURM job script for Phase 1
├── phase2.slurm                          # SLURM job script for Phase 2
├── phase3.slurm                          # SLURM job script for Phase 3
│
├── pipeline_architecture.md              # Pipeline design document
│
├── download.log                          # VIIRS A2 download log
├── download_A1.log                       # VIIRS A1 download log
├── download_knmi.log                     # KNMI download log
└── phase*_*.log                          # SLURM job output logs

/projects/prjs2061/data/                  # Data root (outside repo)
├── viirs/
│   ├── A1/                               # Raw VNP46A1 HDF5 files
│   └── A2/                               # Raw VNP46A2 HDF5 files
├── cbs/                                  # CBS CSV source files
│   ├── Average_energy_prices_for_consumers__2018*.csv   # Old tariff schema
│   ├── Average_energy_prices_for_consumers_2*.csv       # New tariff schema
│   ├── GDP__output_and_expenditures__changes__*.csv     # Quarterly national accounts
│   └── Population (x million).csv                       # Annual population
├── entso-e/                              # ENTSO-E Excel workbooks
├── knmi/                                 # KNMI meteorological data
│
├── processing_1/                         # Phase 1 output
│   ├── viirs_a2/data/year=YYYY/          # Per-day pixel Parquet (+ data_quality.json)
│   ├── cbs_energy/data/                  # Monthly consumer tariffs (harmonized)
│   ├── cbs_gdp/data/                     # Monthly GDP/economic indicators + population
│   └── entsoe/data/year=YYYY/            # Hourly NL load
│
├── processing_2/                         # Phase 2 output
│   ├── viirs_a2_daily/data/year=YYYY/    # Daily VIIRS aggregates
│   ├── cbs_combined/data/                # Merged CBS (tariffs + GDP, ~50 columns)
│   └── entsoe/data/year=YYYY/            # Re-partitioned ENTSO-E
│
└── processed/                            # Phase 3 final output
    ├── nl_hourly_dataset.parquet/year=YYYY/  # Final unified dataset (~60 columns)
    └── data_quality.json                 # Comprehensive quality report
```
