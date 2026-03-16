"""Pipeline helpers for the soldObjects silver notebook.

Schema configuration is defined once in SOLD_FIELDS using FieldConfig.
All legacy constants (SOLD_SCHEMA_FIELDS, RENAME_DICT, etc.) are derived
from that single source of truth.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple


@dataclass(frozen=True)
class FieldConfig:
    """Single-source-of-truth for a pipeline column."""
    raw_name: str                       # column name in the source CSV
    spark_read_type: str                # Spark type for StructField: "IntegerType", etc.
    renamed: Optional[str] = None       # post-load rename (None = keep raw_name)
    cast_type: Optional[str] = None     # post-rename Spark cast (None = no cast)
    required: bool = False              # filter rows where this column is NULL?
    select: bool = True                 # include in SELECT_COLUMNS?

    @property
    def final_name(self) -> str:
        return self.renamed or self.raw_name


# ---------------------------------------------------------------------------
# Schema definition — edit THIS list when the source data changes.
# Everything else is derived automatically.
# ---------------------------------------------------------------------------
SOLD_FIELDS: List[FieldConfig] = [
    FieldConfig("booliId",                    "IntegerType", cast_type="int"),
    FieldConfig("streetAddress",              "StringType"),
    FieldConfig("constructionYear",           "IntegerType", cast_type="int",   required=True),
    FieldConfig("objectType",                 "StringType"),
    FieldConfig("descriptiveAreaName",        "StringType"),
    FieldConfig("soldPriceType",              "StringType"),
    FieldConfig("daysActive",                 "IntegerType", cast_type="int"),
    FieldConfig("soldDate",                   "StringType",  cast_type="date",  required=True),
    FieldConfig("latitude",                   "FloatType",   cast_type="float", required=True),
    FieldConfig("longitude",                  "FloatType",   cast_type="float", required=True),
    FieldConfig("url",                        "StringType",  cast_type="string", required=True),
    FieldConfig("__typename",                 "StringType",  renamed="typeName", cast_type="string"),
    FieldConfig("soldPrice.raw",              "DoubleType",  renamed="soldPrice", cast_type="int", required=True),
    FieldConfig("rent",                       "IntegerType", cast_type="int"),
    FieldConfig("floor.raw",                  "FloatType",   renamed="floor",   cast_type="float", required=True),
    FieldConfig("soldSqmPrice.raw",           "FloatType",   renamed="soldSqmPrice", cast_type="float"),
    FieldConfig("soldPriceAbsoluteDiff.raw",  "FloatType",   renamed="soldPriceAbsoluteDiff"),
    FieldConfig("soldPricePercentageDiff.raw","FloatType",   renamed="soldPricePercentageDiff"),
    FieldConfig("listPrice.raw",              "DoubleType",  renamed="listPrice", cast_type="int", required=True),
    FieldConfig("livingArea.raw",             "FloatType",   renamed="livingArea", cast_type="float"),
    FieldConfig("rooms.raw",                  "FloatType",   renamed="rooms",   cast_type="float"),
]

# ---------------------------------------------------------------------------
# Derived constants — backward-compatible with existing notebook code.
# ---------------------------------------------------------------------------

SOLD_SCHEMA_FIELDS: List[Tuple[str, str]] = [
    (f.raw_name, f.spark_read_type) for f in SOLD_FIELDS if f.select
]

RENAME_DICT: Dict[str, str] = {
    f.raw_name: f.renamed for f in SOLD_FIELDS if f.renamed
}

SELECT_COLUMNS: List[str] = [
    f"`{f.raw_name}`" if "." in f.raw_name or f.raw_name.startswith("__") else f.raw_name
    for f in SOLD_FIELDS if f.select
] + ["source_file"]

CAST_COLUMN_TYPES: Dict[str, str] = {
    f.final_name: f.cast_type for f in SOLD_FIELDS if f.cast_type
}

REQUIRED_NON_NULL_COLUMNS: List[str] = [
    f.final_name for f in SOLD_FIELDS if f.required
]


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def normalize_source_filename(path: Optional[str]) -> Optional[str]:
    """Extract the filename from a wasbs/abfss path for audit logging."""
    if not path:
        return None
    return path.rstrip("/").split("/")[-1]


def rename_columns(df, rename_map: Dict[str, str] = RENAME_DICT):
    """Rename columns on the dataframe without failing if a source column is missing."""
    for old_name, new_name in rename_map.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)
    return df


def schema_field_names(schema_fields: Iterable[Tuple[str, str]] = SOLD_SCHEMA_FIELDS) -> List[str]:
    """Return just the field names from the schema definition for logging/tests."""
    return [name for name, _ in schema_fields]


def log_step(step_name: str, detail: str = "") -> None:
    """Standardized pipeline logging."""
    ts = datetime.utcnow().isoformat()
    print(f"[{ts}] [{step_name}] {detail}")


def build_select_rename_cast_sql(
    fields: List[FieldConfig] = SOLD_FIELDS,
    source_view: str = "raw_loaded",
    include_source_file: bool = True,
) -> str:
    """Build a SQL SELECT that selects, renames, casts, and normalizes source_file in one pass.

    Example output column: CAST(`soldPrice.raw` AS int) AS soldPrice
    """
    exprs = []
    for f in fields:
        if not f.select:
            continue
        # Quote raw names that contain dots or start with __
        raw_ref = f"`{f.raw_name}`" if ("." in f.raw_name or f.raw_name.startswith("__")) else f.raw_name
        alias = f.final_name

        if f.cast_type and f.renamed:
            exprs.append(f"CAST({raw_ref} AS {f.cast_type}) AS {alias}")
        elif f.cast_type:
            exprs.append(f"CAST({raw_ref} AS {f.cast_type}) AS {alias}")
        elif f.renamed:
            exprs.append(f"{raw_ref} AS {alias}")
        else:
            exprs.append(raw_ref)

    if include_source_file:
        exprs.append("normalize_source_filename(source_file) AS sourceFileName")

    columns_sql = ",\n    ".join(exprs)
    return f"SELECT\n    {columns_sql}\nFROM {source_view}"


def build_filter_sql(
    fields: List[FieldConfig] = SOLD_FIELDS,
    source_view: str = "renamed",
    extra_filters: Optional[List[str]] = None,
) -> str:
    """Build a SQL SELECT with WHERE clauses for required non-null columns and extra filters."""
    conditions = []
    for f in fields:
        if f.required:
            conditions.append(f"{f.final_name} IS NOT NULL")
    if extra_filters:
        conditions.extend(extra_filters)

    where_clause = "\n    AND ".join(conditions)
    return f"SELECT *\nFROM {source_view}\nWHERE {where_clause}" if conditions else f"SELECT * FROM {source_view}"
