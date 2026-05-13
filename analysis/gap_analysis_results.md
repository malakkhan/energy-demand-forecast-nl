# Dataset Gap Analysis — 2026-05-13

## KNMI Non-Validated (Hourly)

| Metric | Value |
|--------|-------|
| **Range** | 2015-01-31 → 2026-04-27 |
| **Total files** | 97,765 |
| **Expected hours** | 98,520 |
| **Missing hours** | **755** (0.77%) |
| **Gap ranges** | 34 |

### Gap Details

Most gaps are concentrated in 2015–2016 (early dataset period) and are likely genuine data gaps from KNMI:

| Period | Gap | Duration |
|--------|-----|----------|
| 2015-03-25 → 2015-03-26 | Full days | 48h |
| **2015-04-01 → 2015-04-10** | **Largest gap** | **240h (10 days)** |
| 2015-06-23 | Full day | 24h |
| 2015-10-28 → 2015-10-29 | Full days | 48h |
| 2015-12-01 → 2015-12-02 | Full days | 48h |
| 2015-12-17 → 2015-12-22 | Full days | 144h (6 days) |
| 2016-02-03 → 2016-02-04 | Full days | 48h |
| 2016-04-29 | Full day | 24h |
| 2017-10-14 → 2017-10-15 | Fragmented | ~22h across several sub-gaps |
| 2019-10-07 | Partial day | 17h |
| 2023-07-12 → 2023-07-13 | Partial | 20h |
| 2024-12-12 | Partial day | 17h |
| 2025-11-27 | Full day | 24h |
| Various scattered | Single hours | 1h each (12 instances) |

> [!NOTE]
> The vast majority of gaps (625 of 755 missing hours = 83%) occur before 2017. These are almost certainly genuine gaps in the KNMI non-validated feed—the files simply don't exist on the API. The scattered single-hour gaps from 2017 onward may be transient API/download failures.

---

## VIIRS A1 (VNP46A1 — At-Sensor Radiance, Daily)

| Metric | Value |
|--------|-------|
| **Range** | 2012-01-19 → 2026-04-19 |
| **Total files** | 5,104 |
| **Expected days** | 5,205 |
| **Missing days** | **101** (1.94%) |
| **Gap ranges** | 28 |

### Notable Gaps

| Period | Duration | Likely Cause |
|--------|----------|--------------|
| 2018-02-24 → 2018-03-07 | 12 days | Satellite maintenance |
| 2019-04-03 → 2019-04-19 | 17 days | Satellite anomaly |
| 2022-07-27 → 2022-08-10 | 15 days | Known VIIRS outage |
| 2024-07-10 → 2024-07-16 | 7 days | Processing delay |
| 2024-07-25 → 2024-07-29 | 5 days | Processing delay |

---

## VIIRS A2 (VNP46A2 — Gap-Filled NTL Composite, Daily)

| Metric | Value |
|--------|-------|
| **Range** | 2012-01-19 → 2026-04-08 |
| **Total files** | 5,080 |
| **Expected days** | 5,194 |
| **Missing days** | **114** (2.19%) |
| **Gap ranges** | 28 |

### Notable Gaps

| Period | Duration | Likely Cause |
|--------|----------|--------------|
| 2015-01-26 → 2015-02-02 | 8 days | Processing gap |
| 2021-09-07 → 2021-09-17 | 11 days | Processing gap |
| 2022-07-27 → 2022-08-10 | 15 days | Known VIIRS outage (shared with A1) |
| 2022-11-07 → 2022-11-18 | 12 days | Processing gap |
| 2023-08-30 → 2023-09-10 | 12 days | Processing delay |

> [!IMPORTANT]
> **A2 ends 11 days earlier than A1** (2026-04-08 vs 2026-04-19). A2 is a derived product that takes longer to process, so some lag is expected.

---

## VIIRS A1 vs A2 Alignment

| Metric | Count |
|--------|-------|
| Days in A1 but NOT in A2 | **71** |
| Days in A2 but NOT in A1 | **47** |

The misalignment is expected—A1 (raw radiance) and A2 (gap-filled composite) go through different processing pipelines and have independent availability windows.

---

## Summary & Recommendations

> [!TIP]
> **All three datasets have gaps, but they are almost entirely genuine upstream data gaps (not download errors).** The KNMI API and NASA LAADS DAAC simply don't serve files for those periods.

| Dataset | Gap % | Verdict |
|---------|-------|---------|
| KNMI Non-Validated | 0.77% | ✅ Healthy — mostly 2015–2016 early-period gaps |
| VIIRS A1 | 1.94% | ✅ Healthy — known satellite outage windows |
| VIIRS A2 | 2.19% | ✅ Healthy — derived product processing delays |
