"""
Polars-specific utilities for high-performance operations.

This package contains:
- translator: SQL expression to Polars expression translator
- operators: Polars-based operator implementations
"""

from .translator import (
    sql_to_polars_expr,
    can_translate_to_polars,
    extract_table_alias_from_column
)

__all__ = [
    "sql_to_polars_expr",
    "can_translate_to_polars",
    "extract_table_alias_from_column",
]

