"""Shared helpers for the VandaXasset QuantKit notebooks.

Everything here is deliberately small and readable: copy any function into your
own codebase if you prefer not to import the module.
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Iterable, Optional, Sequence, Union

import numpy as np
import pandas as pd
import pyarrow.compute as pc
import pyarrow.dataset as pads
import pyarrow.fs as pafs

KIT_ROOT = Path(__file__).resolve().parent
REFERENCE_DIR = KIT_ROOT / "Reference Data"

DATA_ROOT = os.getenv("VXA_DATA_ROOT", "s3://vanda-analytics-samples/vandaxasset")
API_BASE = os.getenv("VANDA_API_BASE", "https://api.vanda-analytics.com")

FAMILIES = ("flow", "positioning")
REVISIONS = ("restated", "point_in_time")
CROWDING_THRESHOLD = 1.5

COLUMNS = [
    "ts", "series_label", "entity", "entity_type", "metric", "value", "unit",
    "frequency", "is_pit", "dataset_id", "vanda_id", "published_at",
]

DateLike = Union[str, pd.Timestamp, None]


# ---------------------------------------------------------------------------
# Bulk data (S3 samples or a local copy)
# ---------------------------------------------------------------------------

def filesystem(root: str = DATA_ROOT):
    if root.startswith("s3://"):
        region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        return pafs.S3FileSystem(region=region), root[len("s3://"):]
    return pafs.LocalFileSystem(), str(Path(root).expanduser().resolve())


@lru_cache(maxsize=None)
def load_catalog(family: str = "flow") -> pd.DataFrame:
    """One row per dataset: id, display name, coverage window and path."""
    if family not in FAMILIES:
        raise ValueError(f"family must be one of {FAMILIES}")
    fs, base = filesystem()
    with fs.open_input_file(f"{base}/catalog/{family}.parquet") as f:
        catalog = pd.read_parquet(f)
    for col in ("first_ts", "last_ts", "last_updated"):
        catalog[col] = pd.to_datetime(catalog[col], utc=True)
    return catalog.sort_values(["asset_class", "dataset"]).reset_index(drop=True)


def load_datasets() -> pd.DataFrame:
    """Both catalogs stacked, with a `family` column."""
    return pd.concat(
        [load_catalog(f).assign(family=f) for f in FAMILIES], ignore_index=True
    )


def _dataset_entry(family: str, dataset: str) -> pd.Series:
    catalog = load_catalog(family)
    match = catalog[catalog["dataset"] == dataset]
    if match.empty:
        known = ", ".join(catalog["dataset"])
        raise KeyError(f"Unknown {family} dataset '{dataset}'. Known: {known}")
    return match.iloc[0]


def _as_list(value) -> Optional[list]:
    if value is None:
        return None
    if isinstance(value, str):
        return [value]
    return list(value)


def load(
    family: str,
    dataset: str,
    series: Optional[Union[str, Sequence[str]]] = None,
    metrics: Optional[Union[str, Sequence[str]]] = None,
    frequency: Optional[Union[str, Sequence[str]]] = None,
    start: DateLike = None,
    end: DateLike = None,
    revision: str = "restated",
    version: Optional[int] = None,
) -> pd.DataFrame:
    """Load one dataset in long format: one row per (ts, series_label, metric).

    - `version` defaults to the dataset version the catalog marks as current.
      Some datasets publish v1 and v2 side by side; reading the whole folder
      would silently mix two models.
    - Positioning files carry both restated and point-in-time rows for the
      same dates. `revision` picks one of them, so a series never has two
      values per day.
    - Positioning defaults to `metrics="combined_positioning"`; a few datasets
      also publish helper metrics (e.g. `n_inputs`) on the same dates.
    - Filters are pushed down to the parquet scan, so asking for one metric of
      a large dataset reads a fraction of it.
    """
    if revision not in REVISIONS:
        raise ValueError(f"revision must be one of {REVISIONS}")

    entry = _dataset_entry(family, dataset)
    version = int(version or entry["dataset_version"])
    fs, base = filesystem()
    path = (
        f"{base}/{family}/layout=v1/asset_class={entry['asset_class']}"
        f"/dataset={dataset}/dataset_version={version}"
    )
    ds = pads.dataset(path, filesystem=fs, format="parquet", partitioning="hive")

    if family == "positioning" and metrics is None:
        metrics = "combined_positioning"

    expr = pc.scalar(True)
    if series is not None:
        expr &= pc.field("series_label").isin(_as_list(series))
    if metrics is not None:
        expr &= pc.field("metric").isin(_as_list(metrics))
    if frequency is not None:
        expr &= pc.field("frequency").isin(_as_list(frequency))
    if family == "positioning":
        expr &= pc.field("is_pit") == (revision == "point_in_time")
    if start is not None:
        start_ts = pd.Timestamp(start, tz="UTC")
        expr &= pc.field("ts") >= start_ts
        if "year" in ds.schema.names:
            expr &= pc.field("year") >= start_ts.year
    if end is not None:
        end_ts = pd.Timestamp(end, tz="UTC")
        expr &= pc.field("ts") <= end_ts
        if "year" in ds.schema.names:
            expr &= pc.field("year") <= end_ts.year

    columns = [c for c in COLUMNS if c in ds.schema.names]
    df = ds.to_table(filter=expr, columns=columns).to_pandas()
    if df.empty:
        return df
    df["ts"] = df["ts"].dt.tz_convert(None)
    return df.sort_values(["series_label", "metric", "ts"]).reset_index(drop=True)


def pivot(df: pd.DataFrame, columns: str = "series_label", values: str = "value") -> pd.DataFrame:
    """Long -> wide: one column per series, indexed by date."""
    wide = df.pivot_table(index="ts", columns=columns, values=values, aggfunc="last")
    wide.columns.name = None
    return wide.sort_index()


# ---------------------------------------------------------------------------
# Reference data
# ---------------------------------------------------------------------------

@lru_cache(maxsize=None)
def series_reference() -> pd.DataFrame:
    """Series dictionary shipped in Reference Data/ (name, taxonomy, coverage)."""
    ref = pd.read_csv(REFERENCE_DIR / "VandaXasset-Series-Reference.csv")
    for col in ("first_ts", "last_ts"):
        ref[col] = pd.to_datetime(ref[col])
    return ref


@lru_cache(maxsize=None)
def label_codes() -> pd.DataFrame:
    """What each code inside a series label means (e.g. SPE -> Speculative)."""
    return pd.read_csv(REFERENCE_DIR / "VandaXasset-Label-Codes.csv")


def names(labels: Optional[Iterable[str]] = None) -> dict:
    """series_label -> human-readable name."""
    ref = series_reference().drop_duplicates("series_label").set_index("series_label")["name"]
    if labels is None:
        return ref.to_dict()
    return {label: ref.get(label, label) for label in labels}


def describe(label: str) -> pd.Series:
    """Everything the reference file knows about one series."""
    ref = series_reference()
    match = ref[ref["series_label"] == label]
    if match.empty:
        raise KeyError(f"'{label}' is not in the series reference")
    return match.iloc[0].dropna()


# ---------------------------------------------------------------------------
# Signal maths
# ---------------------------------------------------------------------------

def expanding_zscore(s: pd.Series, min_periods: int = 252) -> pd.Series:
    """Z-score using only data available up to each date (no look-ahead)."""
    mean = s.expanding(min_periods=min_periods).mean()
    std = s.expanding(min_periods=min_periods).std()
    return (s - mean) / std


def rolling_zscore(s: pd.Series, window: int = 252, min_periods: Optional[int] = None) -> pd.Series:
    mean = s.rolling(window, min_periods=min_periods or window // 2).mean()
    std = s.rolling(window, min_periods=min_periods or window // 2).std()
    return (s - mean) / std


def percentile_of_last(s: pd.Series) -> float:
    """Where the latest value sits in the series' own history, 0-100."""
    s = s.dropna()
    if s.empty:
        return np.nan
    return 100.0 * (s < s.iloc[-1]).mean() + 50.0 * (s == s.iloc[-1]).mean()


def crowding_signal(z: float, threshold: float = CROWDING_THRESHOLD) -> str:
    if pd.isna(z):
        return "n/a"
    if z >= threshold:
        return "crowded_long"
    if z <= -threshold:
        return "crowded_short"
    return "neutral"


# ---------------------------------------------------------------------------
# REST API / Python SDK
# ---------------------------------------------------------------------------

CREDENTIAL_VARS = ("VANDA_SDK_KEY", "VANDA_API_TOKEN", "VANDA_LOGIN_EMAIL")


def has_api_credentials() -> bool:
    return any(os.getenv(v) for v in CREDENTIAL_VARS)


def api_client():
    """VandaClient configured from the environment.

    Set one of: VANDA_SDK_KEY (recommended), VANDA_API_TOKEN, or
    VANDA_LOGIN_EMAIL + VANDA_PASSWORD. See .env.example.
    """
    from vanda import VandaClient

    if not has_api_credentials():
        raise RuntimeError(
            "No Vanda credentials found. Set VANDA_SDK_KEY (generate one at "
            "https://www.vanda-analytics.com/tools/api-keys) and re-run."
        )
    return VandaClient(base_url=API_BASE)


def fetch_pages(call, max_pages: int = 50, **params) -> list:
    """Follow `pagination.has_next` and return every row.

    Stops early if a page adds nothing new, so a misbehaving paginator can
    never loop forever.
    """
    rows, seen = [], set()
    for page in range(1, max_pages + 1):
        resp = call(page_number=page, **params)
        batch = resp.get("data", [])
        keys = {(r.get("ts"), r.get("series_id"), r.get("metric")) for r in batch}
        if not batch or keys <= seen:
            break
        seen |= keys
        rows.extend(batch)
        if not (resp.get("pagination") or {}).get("has_next"):
            break
    return rows


def flow_history(client, series_ids, metrics=None, start=None, end=None, frequency=None) -> pd.DataFrame:
    """Flow timeseries via the SDK as a long DataFrame (ts, series_label, metric, value, unit)."""
    rows = fetch_pages(
        client.flow.get_timeseries,
        series_ids=series_ids, metrics=metrics, frequency=frequency,
        start_date=start, end_date=end, order="asc", records_per_page=5000,
    )
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["ts"] = pd.to_datetime(df["ts"], utc=True).dt.tz_convert(None)
    return df.sort_values(["series_label", "metric", "ts"]).reset_index(drop=True)


def positioning_history(
    client,
    series_id: str,
    start=None,
    end=None,
    revision: str = "restated",
    normalisation: str = "none",
    normalisation_period: Optional[str] = None,
) -> pd.DataFrame:
    """One positioning series via the SDK as (ts, value).

    Ask for one series per call. The service only normalises a single series
    and returns rows keyed by a numeric id, so batching several labels into
    one request makes rows easy to mis-attribute.
    """
    params = dict(series_ids=series_id, start_date=start, end_date=end, revision=revision, order="asc")
    if normalisation != "none":
        resp = client.positioning.get_timeseries(
            normalisation=normalisation, normalisation_period=normalisation_period or "All",
            records_per_page=5000, **params,
        )
        transformed = ((resp.get("transformation") or {}).get("results") or {}).get("data")
        if not transformed:
            raise RuntimeError(f"No normalised rows returned: {resp.get('transformation')}")
        field = f"combined_positioning_{'z_score' if normalisation == 'zscore' else normalisation}"
        df = pd.DataFrame(transformed)
        df["value"] = df[field] if field in df else df["value"]
    else:
        df = pd.DataFrame(fetch_pages(
            client.positioning.get_timeseries, normalisation="none", records_per_page=5000, **params,
        ))
    if df.empty:
        return df
    df["ts"] = pd.to_datetime(df["ts"], utc=True).dt.tz_convert(None)
    # normalised results cover the full history; trim to the window asked for
    if start is not None:
        df = df[df["ts"] >= pd.Timestamp(start)]
    if end is not None:
        df = df[df["ts"] <= pd.Timestamp(end)]
    return df[["ts", "value"]].assign(series_label=series_id).sort_values("ts").reset_index(drop=True)


# ---------------------------------------------------------------------------
# Charts
# ---------------------------------------------------------------------------

VANDA = {
    "purple": "#6319a1",
    "lilac": "#c278e8",
    "ink": "#1f2937",
    "slate": "#1e293b",
    "sub": "#6b7280",
    "grid": "#e5e7eb",
    "pos": "#10b981",
    "neg": "#ef4444",
}
PALETTE = ["#6319a1", "#1e293b", "#c278e8", "#10b981", "#f59e0b", "#3b82f6", "#ef4444", "#9ca3af"]


def set_style() -> None:
    import matplotlib as mpl
    from cycler import cycler

    mpl.rcParams.update({
        "figure.figsize": (11, 4.5),
        "figure.dpi": 110,
        "axes.prop_cycle": cycler(color=PALETTE),
        "axes.edgecolor": VANDA["grid"],
        "axes.labelcolor": VANDA["ink"],
        "axes.titlesize": 12,
        "axes.titleweight": "bold",
        "axes.titlelocation": "left",
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.grid": True,
        "grid.color": VANDA["grid"],
        "grid.linewidth": 0.6,
        "xtick.color": VANDA["sub"],
        "ytick.color": VANDA["sub"],
        "legend.frameon": False,
        "font.size": 10,
    })


def source_note(ax, text: str = "Source: Vanda Analytics, VandaXasset") -> None:
    ax.annotate(text, xy=(0, 0), xycoords="axes fraction", xytext=(0, -32), textcoords="offset points",
                va="top", fontsize=8, color=VANDA["sub"])
