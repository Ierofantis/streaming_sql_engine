"""
SQL Expression to Polars Expression Translator

Translates SQL expressions (from sqlglot) to Polars expressions for vectorized evaluation.
This enables SIMD-accelerated filtering and projection operations.
"""

from sqlglot import expressions as exp
from typing import Optional
import polars as pl


def sql_to_polars_expr(sql_expr, table_alias: Optional[str] = None) -> Optional[pl.Expr]:
    """
    Convert a SQL expression to a Polars expression.
    
    Args:
        sql_expr: SQL expression from sqlglot
        table_alias: Optional table alias prefix for columns
        
    Returns:
        Polars expression or None if translation not supported
    """
    if sql_expr is None:
        return None
    
    expr_type = type(sql_expr)
    
    # Column reference
    if expr_type is exp.Column:
        col_name = sql_expr.name
        # Check if column has table prefix in the expression itself
        if sql_expr.table:
            # Column has table prefix: table.column (e.g., products.checked)
            full_col = f"{sql_expr.table}.{col_name}"
            return pl.col(full_col)
        elif table_alias:
            # Use provided table alias prefix: alias.column
            full_col = f"{table_alias}.{col_name}"
            return pl.col(full_col)
        else:
            # Column without prefix
            return pl.col(col_name)
    
    # Literal value
    elif expr_type is exp.Literal:
        value = sql_expr.this
        # Handle string literals that represent numbers (common in SQL parsing)
        # SQL parsers often store numeric literals as strings, so we need to convert them
        if isinstance(value, str):
            # Try to convert string to number if it looks like a number
            # This handles cases where SQL parser stores "1" as string instead of int
            try:
                # Try integer first
                if value.isdigit() or (value.startswith('-') and value[1:].isdigit()):
                    return pl.lit(int(value))
                # Try float
                float_val = float(value)
                return pl.lit(float_val)
            except (ValueError, AttributeError):
                # Not a number, keep as string
                return pl.lit(value)
        elif isinstance(value, (int, float)):
            return pl.lit(value)
        elif isinstance(value, bool):
            return pl.lit(value)
        elif value is None:
            return pl.lit(None)
        else:
            return pl.lit(str(value))
    
    # Comparison operators (vectorized with SIMD)
    elif expr_type is exp.EQ:
        left = sql_to_polars_expr(sql_expr.this, table_alias)
        right = sql_to_polars_expr(sql_expr.expression, table_alias)
        if left is not None and right is not None:
            return left == right
    
    elif expr_type is exp.NEQ:
        left = sql_to_polars_expr(sql_expr.this, table_alias)
        right = sql_to_polars_expr(sql_expr.expression, table_alias)
        if left is not None and right is not None:
            return left != right
    
    elif expr_type is exp.LT:
        left = sql_to_polars_expr(sql_expr.this, table_alias)
        right = sql_to_polars_expr(sql_expr.expression, table_alias)
        if left is not None and right is not None:
            return left < right
    
    elif expr_type is exp.GT:
        left = sql_to_polars_expr(sql_expr.this, table_alias)
        right = sql_to_polars_expr(sql_expr.expression, table_alias)
        if left is not None and right is not None:
            return left > right
    
    elif expr_type is exp.LTE:
        left = sql_to_polars_expr(sql_expr.this, table_alias)
        right = sql_to_polars_expr(sql_expr.expression, table_alias)
        if left is not None and right is not None:
            return left <= right
    
    elif expr_type is exp.GTE:
        left = sql_to_polars_expr(sql_expr.this, table_alias)
        right = sql_to_polars_expr(sql_expr.expression, table_alias)
        if left is not None and right is not None:
            return left >= right
    
    # Boolean operators
    elif expr_type is exp.And:
        left = sql_to_polars_expr(sql_expr.this, table_alias)
        right = sql_to_polars_expr(sql_expr.expression, table_alias)
        if left is not None and right is not None:
            return left & right  # Polars uses & for AND
    
    elif expr_type is exp.Or:
        left = sql_to_polars_expr(sql_expr.this, table_alias)
        right = sql_to_polars_expr(sql_expr.expression, table_alias)
        if left is not None and right is not None:
            return left | right  # Polars uses | for OR
    
    elif expr_type is exp.Not:
        inner = sql_to_polars_expr(sql_expr.this, table_alias)
        if inner is not None:
            return ~inner  # Polars uses ~ for NOT
    
    # NULL checks
    elif expr_type is exp.Is:
        inner = sql_to_polars_expr(sql_expr.this, table_alias)
        if inner is None:
            return None
        
        # IS NULL
        if sql_expr.expression is None or isinstance(sql_expr.expression, exp.Null):
            return inner.is_null()
        # IS NOT NULL
        elif isinstance(sql_expr.expression, exp.Not) and isinstance(sql_expr.expression.this, exp.Null):
            return inner.is_not_null()
    
    # IN clause
    elif expr_type is exp.In:
        left = sql_to_polars_expr(sql_expr.this, table_alias)
        if left is None:
            return None
        
        if isinstance(sql_expr.expression, exp.Tuple):
            values = []
            for e in sql_expr.expression.expressions:
                polars_expr = sql_to_polars_expr(e, table_alias)
                if polars_expr is None:
                    return None
                # Extract literal value from Polars expression
                # This is a simplification - in practice, we'd need to handle this better
                values.append(_extract_literal_value(polars_expr))
            
            if values:
                return left.is_in(values)
    
    # Parentheses - unwrap
    elif expr_type is exp.Paren:
        return sql_to_polars_expr(sql_expr.this, table_alias)
    
    # Alias - unwrap
    elif expr_type is exp.Alias:
        return sql_to_polars_expr(sql_expr.this, table_alias)
    
    # Unsupported expression
    return None


def _extract_literal_value(polars_expr: pl.Expr) -> any:
    """
    Extract literal value from a Polars expression.
    This is a helper for IN clauses.
    """
    # This is a simplified approach - Polars expressions are complex
    # In practice, we'd need to inspect the expression structure
    # For now, we'll handle this in the calling code
    return None


def can_translate_to_polars(sql_expr) -> bool:
    """
    Check if a SQL expression can be translated to Polars.
    
    Returns:
        True if translation is supported, False otherwise
    """
    return sql_to_polars_expr(sql_expr) is not None


def extract_table_alias_from_column(column_expr: exp.Column) -> Optional[str]:
    """Extract table alias from a column expression."""
    return column_expr.table if column_expr.table else None

