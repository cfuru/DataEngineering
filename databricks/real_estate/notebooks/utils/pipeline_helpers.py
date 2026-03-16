"""Pipeline helpers for the soldObjects silver notebook."""

from __future__ import annotations

from typing import Dict, Iterable, List, Optional, Tuple

SOLD_SCHEMA_FIELDS: List[Tuple[str, str]] = [
    ("booliId", "IntegerType"),
    ("streetAddress", "StringType"),
    ("constructionYear", "IntegerType"),
    ("objectType", "StringType"),
    ("descriptiveAreaName", "StringType"),
    ("soldPriceType", "StringType"),
    ("daysActive", "IntegerType"),
    ("soldDate", "StringType"),
    ("latitude", "FloatType"),
    ("longitude", "FloatType"),
    ("url", "StringType"),
    ("__typename", "StringType"),
    ("soldPrice.raw", "DoubleType"),
    ("rent", "IntegerType"),
    ("floor.raw", "FloatType"),
    ("soldSqmPrice.raw", "FloatType"),
    ("soldPriceAbsoluteDiff.raw", "FloatType"),
    ("soldPricePercentageDiff.raw", "FloatType"),
    ("listPrice.raw", "DoubleType"),
    ("livingArea.raw", "FloatType"),
    ("rooms.raw", "FloatType"),
]

RENAME_DICT: Dict[str, str] = {
    "soldPrice.raw": "soldPrice",
    "rent.raw": "rent",
    "floor.raw": "floor",
    "soldSqmPrice.raw": "soldSqmPrice",
    "soldPriceAbsoluteDiff.raw": "soldPriceAbsoluteDiff",
    "soldPricePercentageDiff.raw": "soldPricePercentageDiff",
    "listPrice.raw": "listPrice",
    "livingArea.raw": "livingArea",
    "rooms.raw": "rooms",
    "__typename": "typeName",
}

SELECT_COLUMNS: List[str] = [
    "booliId",
    "streetAddress",
    "constructionYear",
    "objectType",
    "descriptiveAreaName",
    "soldPriceType",
    "daysActive",
    "soldDate",
    "latitude",
    "longitude",
    "url",
    "__typename",
    "`soldPrice.raw`",
    "`floor.raw`",
    "`soldSqmPrice.raw`",
    "`soldPriceAbsoluteDiff.raw`",
    "`soldPricePercentageDiff.raw`",
    "`listPrice.raw`",
    "`livingArea.raw`",
    "`rooms.raw`",
    "rent",
    "source_file",
]

CAST_COLUMN_TYPES: Dict[str, str] = {
    "booliId": "int",
    "constructionYear": "int",
    "daysActive": "int",
    "soldDate": "date",
    "latitude": "float",
    "longitude": "float",
    "url": "string",
    "typeName": "string",
    "rent": "int",
    "floor": "float",
    "soldSqmPrice": "float",
    "livingArea": "float",
    "rooms": "float",
    "listPrice": "int",
    "soldPrice": "int",
}

REQUIRED_NON_NULL_COLUMNS: List[str] = [
    "constructionYear",
    "soldDate",
    "latitude",
    "longitude",
    "soldPrice",
    "listPrice",
    "floor",
    "url",
]


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
