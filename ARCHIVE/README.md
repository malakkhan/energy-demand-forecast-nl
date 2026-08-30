# ARCHIVE — pre-leakage-fix material (moved 2026-08-27)

Everything in this directory is **stale**: produced under the OLD experimental
protocol, before the data-leakage fixes of 2026-08-25. Nothing in here should
be used for the paper's results. It is kept only for provenance/audit.

The old protocol differed from the current one in ways that invalidate direct
comparison:
- ROCV origins were annual (always ~Jan 1) → seasonal sampling bias
- PCA was fitted once on the full 2012–2025 dataset → future leakage
- Optuna objective touched test-set PCC → test leakage
- Quantile (pinball) forecasting instead of MSE point forecasting
- `is_energy_crisis` ex-post feature included
- Old seed set for CMAT-Full multiseed was {42, 123, 456} (now {42, 123, 7})

## Layout

- `cmat/results/` — old ROCV CSVs (`*_OLD_BUGGY*`, `*_old*`), old-protocol
  best-config JSONs (h75–h165 and the `early` variant), old feature-importance
  CSVs, old checkpoint dir (`checkpoints_old_protocol/`, formerly
  `results/checkpoints/`), the worker-test artifacts, and the interim
  `results_archive_*` dirs from the Aug 25 cleanup.
- `cmat/scripts/` — old-protocol submission wrappers (`submit_*.sh`,
  `run_cmat_h100.slurm`, `run_cmat.slurm`, `run_feature_importance.slurm`)
  and one-off summary/LaTeX scripts.
- `baselines/results/` — pre-fix result CSVs (all_models, combined, huang,
  sarimax_lstm from June), old comparison .tex files, and the earlier
  `results_archive_buggy` / `results_archive_stale_20260825` dirs.
- `baselines/scripts/` — superseded runners (v1 scripts, single-seed v2
  SLURM wrappers replaced by the `*_3seeds_3horizons.slurm` versions).
- `logs/` — all SLURM logs last modified before 2026-08-25 (520 files).
- `misc/` — one-off refactoring helper scripts from the Aug 25 patch session.

## What is CURRENT (kept in normal locations)

- `reports/results_tables.tex` — canonical LaTeX results tables
  (regenerate with `src/analysis/generate_results_tables.py`)
- `src/models/cmat/results/` — best_config h60/h120/h180 (tab/ntl/full),
  trial_history JSONLs, rocv_results_cmat_{tab,ntl}, `_shards/`,
  `checkpoints_v2/`
- `src/models/baselines/results/` — seq2seq / prophet-lstm / mlr /
  naive_24h / drift / seasonal_naive results + `_shards/`
- SLURM runners: `run_cmat_full_search.slurm`, `run_cmat_tabntl_*.slurm`,
  `run_{seq2seq,prophet_lstm}_3seeds_3horizons.slurm`, `run_mlr.slurm`,
  `run_sarimax_lstm_v2.slurm` (note: sarimax still needs a fresh run under
  the new pipeline)
- `logs/` — new-era logs only (2026-08-25 onward)

Note: all `.py` model/library modules were deliberately left in place (even
old versions) because they cross-import each other; only wrapper scripts and
data artifacts were archived.
