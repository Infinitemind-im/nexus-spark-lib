"""Schema manipulation helpers for PySpark DataFrames."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType


def add_column_if_missing(df: DataFrame, col_name: str, default_value: str) -> DataFrame:
    """Add a column with a literal default if it does not already exist."""
    if col_name not in df.columns:
        return df.withColumn(col_name, F.lit(default_value))
    return df


def ensure_string_column(df: DataFrame, col_name: str) -> DataFrame:
    """Cast a column to StringType if it is not already a string."""
    from pyspark.sql.types import StringType as ST
    field = next((f for f in df.schema.fields if f.name == col_name), None)
    if field and not isinstance(field.dataType, ST):
        return df.withColumn(col_name, df[col_name].cast(StringType()))
    return df


def flatten_struct_column(df: DataFrame, struct_col: str) -> DataFrame:
    """Flatten all fields of a StructType column to the top level."""
    field = next((f for f in df.schema.fields if f.name == struct_col), None)
    if field is None or not isinstance(field.dataType, StructType):
        return df
    sub_fields = [
        F.col(f"{struct_col}.{sf.name}").alias(f"{struct_col}__{sf.name}")
        for sf in field.dataType.fields
    ]
    return df.withColumns({f"{struct_col}__{sf.name}": F.col(f"{struct_col}.{sf.name}")
                           for sf in field.dataType.fields})
