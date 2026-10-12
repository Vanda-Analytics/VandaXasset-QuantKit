# VandaXasset Quant Starter Kit

A starter kit for researching **VandaXasset cross-asset positioning and flow data** through:

- Bulk parquet files on S3, for history and backtesting
- REST API, for live, hourly-refreshed data
- Official Python SDK ([`vanda-api`](https://pypi.org/project/vanda-api/))
- Reference data: a dictionary of every series, the dataset catalogue, and the series-label codes

VandaXasset covers **73 datasets and ~1,240 series** across equities, fixed income, FX and commodities:

| Family | What it is | Unit |
|---|---|---|
| **Positioning** (41 datasets, 235 series) | Vanda's *combined positioning* score per market, sector, tenor or currency pair | z-score vs the series' own history |
| **Flow** (32 datasets, ~1,000 series) | The investor-level building blocks: foreign vs domestic flows, CFTC trader categories, CTA and risk-parity exposure, futures aggressors, dealer gamma, short interest, fund flows | USD mn, contracts, % of AUM |

---

## Documentation

- REST API: https://docs.vanda-analytics.com/
- Python SDK (`vanda-api`): https://pypi.org/project/vanda-api/
- Snowflake share: https://docs.vanda-analytics.com/snowflake/vandaxasset
- Using VandaXasset from an AI assistant (MCP): https://docs.vanda-analytics.com/vanda-xasset-mcp

---

## Quick start

```bash
git clone https://github.com/Vanda-Analytics/VandaXasset-QuantKit.git
cd VandaXasset-QuantKit
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env        # then add your credentials
jupyter lab Notebooks/
```

The notebooks were tested on Python 3.12 with both pandas 2.2 and pandas 3.0 (see `requirements.txt`).

### Credentials

Put credentials in `.env`, which git ignores. Never paste them into a notebook.

| To use | Set | Where to get it |
|---|---|---|
| S3 bulk files | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION=us-east-1` | Issued by Vanda with your data licence |
| REST API / SDK | `VANDA_SDK_KEY` (recommended), or `VANDA_API_TOKEN`, or `VANDA_LOGIN_EMAIL` + `VANDA_PASSWORD` | Generate an SDK key at [vanda-analytics.com/tools/api-keys](https://www.vanda-analytics.com/tools/api-keys) |

### Working offline (recommended)

Notebooks read straight from S3 by default. For speed, sync the sample once (about 200 MB) and point the kit at it:

```bash
aws s3 sync s3://vanda-analytics-samples/vandaxasset/ ./data/vandaxasset/
echo "VXA_DATA_ROOT=$(pwd)/data/vandaxasset" >> .env
```

---

## Sample data (S3)

**Bucket:** `vanda-analytics-samples` · **Folder:** `vandaxasset/`

```
vandaxasset/
├── catalog/
│   ├── flow.parquet          (+ .csv)   one row per flow dataset: id, name, coverage, current version
│   └── positioning.parquet   (+ .csv)   one row per positioning dataset
├── flow/layout=v1/
│   └── asset_class=<equity|fixed_income|fx|commodity|multi_asset>/
│       └── dataset=<name>/dataset_version=<n>/
│           └── year=YYYY/month=MM/data.parquet
└── positioning/layout=v1/
    └── asset_class=<...>/dataset=<name>/dataset_version=<n>/data.parquet
```

Folders are Hive-style partitions, so `pyarrow`, `polars`, DuckDB, Spark and Athena read the tree directly. Every file shares
one **long** schema, with one row per `(ts, series_label, metric)`:

`ts · series_label · vanda_id · dataset_id · entity · entity_type · metric · unit · measure_class · frequency · value · is_pit · published_at`

### Four things to know before you start

1. **Positioning files hold two revisions.** `is_pit = False` is the *restated* value; `is_pit = True` is the
   *point-in-time* value as first published. Filter to one, or every series has two values per day. Use point-in-time
   for backtests (notebook 04).
2. **Use the catalogue's `dataset_version`.** Improved models are published alongside old ones (for example Europe, Korea
   and Taiwan positioning v1 and v2), so reading a whole dataset folder mixes them.
3. **Check each series' cadence.** Sources publish daily, weekly or monthly. Check the median gap between observations
   before choosing a rolling window measured in observations.
4. **The S3 `series_label` is the API's `legacy_series_id`.** Pass the same codes (`USEQCOMB`, `KREQSTCKFORMIXMIXMIX`, ...) to
   the API's `series_ids`.

`quantkit.load()` handles 1 and 2 for you.

---

## Included notebooks

| # | Notebook | What you learn |
|---|---|---|
| 00 | **Data Access: S3 Bulk Files** | Bucket layout, schema, catalogues, plain `pyarrow` reads, the revision trap, versions, cadence, taking a local copy |
| 01 | **Data Access: REST API & Python SDK** | Authentication, catalogue and facets, latest readings with crowding signals, pagination, addressing flow series by slug, server-side transformations, point-in-time via the API, the systematic **forecast cube**, bulk export |
| 02 | **Catalogue Tour & Series Taxonomy** | What exists and how far back it goes; how to read a series label (with a decoder that parses every label); searching the series dictionary; the metric vocabulary |
| 03 | **Cross-Asset Crowding Screen** | All 235 positioning series on one scale: snapshot with percentiles and staleness, most-crowded rankings, crowding breadth through time, level vs momentum regimes, co-movement, CSV export |
| 04 | **Restated vs Point-in-Time: Backtest Hygiene** | How far the two revisions diverge, why re-standardising does not fix it, how many crowding signals each revision fires (and how few coincide), publication lags, a backtest-safe loader |
| 05 | **Positioning Extremes: How Long Crowding Lasts** | Every point-in-time crowding episode: survival curves by asset class, the average episode's overshoot and decay, longs vs shorts, build-up speed, and the remaining life of today's extremes |
| 06 | **Systematic Strategies: CTA, Risk Parity & Vol Targeting** | Modelled AUM, CTA exposure vs history, projected flows for the next 1–10 days, a check of projections against outcomes, three kinds of systematic money in the S&P, the cross-asset CTA book through time |
| 07 | **Cross-Border Flows Monitor: Asia & EM** | 17 foreign-flow series: cadence check, monthly z-score monitor, scoreboard, EM Asia weekly flows and breadth, China Southbound by sector |
| 08 | **Who Is On The Other Side: CFTC, Aggressors & Dealer Gamma** | The Treasury basis trade (leveraged funds vs asset managers), managed money vs producers in commodities, aggressor flow split into new positions vs covering, and dealer gamma regimes in equity and credit options |
| 09 | **Sector Rotation Across Regions** | A ten-region × GICS sector grid: global consensus vs regional disagreement, 13-week rotation, and which crowded cells are building or unwinding |

Notebooks 00 and 02–09 run on the S3 sample, and notebook 01 on the live API. All ship with outputs, and every notebook
uses VandaXasset data only.

---

## The helper module: `quantkit.py`

A single small file of helpers that every notebook shares. Copy any function into your own code if you prefer.

| Function | Purpose |
|---|---|
| `load_catalog(family)`, `load_datasets()` | Dataset catalogues |
| `load(family, dataset, series=, metrics=, start=, end=, revision=, version=)` | Load one dataset in long format, with filters pushed into the parquet scan |
| `pivot(df)` | Long → wide, one column per series |
| `series_reference()`, `describe(label)`, `names()`, `label_codes()` | The reference data |
| `expanding_zscore`, `rolling_zscore`, `percentile_of_last`, `crowding_signal` | Signal maths |
| `api_client()`, `positioning_history()`, `flow_history()`, `fetch_pages()` | SDK helpers with safe pagination |
| `set_style()`, `source_note()` | Vanda chart style |

---

## Reference data

| File | Contents |
|---|---|
| `VandaXasset-Series-Reference.csv` | One row per series (1,238): name, description, dataset, taxonomy (asset, country, region, sector, investor origin, category, type, subtype), benchmark, metrics, units, cadence, coverage, publication lag and time |
| `VandaXasset-Dataset-Catalogue.csv` | One row per dataset (73): id, name, current version, coverage, series count, metrics, units |
| `VandaXasset-Label-Codes.csv` | What each segment of a series label means (`SPE` speculative, `FOR` foreign, `CTA`, ...) |

---

## Coming soon

Additional analytics will be added in future releases.

For any further questions, see the full REST API documentation or contact sales@vanda.com.

*Research material only. Not investment advice.*
