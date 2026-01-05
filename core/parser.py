"""
SQL parsing using sqlglot to extract query structure.
"""

import sqlglot
from sqlglot import expressions as exp


class ParseError(Exception):
    """Raised when SQL parsing fails or unsupported constructs are detected."""
    pass
    

def parse_sql(sql, dialect=None):
    """
    Parse SQL string into an AST.
    
    Args:
        sql: SQL query string
        dialect: Optional SQL dialect ('mysql', 'postgres', 'sqlite', etc.)
                 If None, tries multiple dialects automatically
        
    Returns:
        sqlglot Select expression AST
        
    Raises:
        ParseError: If parsing fails or unsupported constructs are found
    """
    if dialect:
        # Use specified dialect
        dialects_to_try = [dialect]
    else:
        # Try common dialects in order
        dialects_to_try = ["mysql", "postgres", "sqlite"]
    
    last_error = None
    for dialect_name in dialects_to_try:
        try:
            parsed = sqlglot.parse_one(sql, dialect=dialect_name)
            if isinstance(parsed, exp.Select):
                # Check for unsupported constructs
                _validate_query(parsed)
                return parsed
            else:
                raise ParseError(f"Only SELECT queries are supported, got {type(parsed).__name__}")
        except Exception as e:
            last_error = e
            continue
    
    # If all dialects failed, raise the last error
    raise ParseError(f"Failed to parse SQL with any dialect. Last error: {last_error}") from last_error


def _validate_query(select):
    """Validate that query only contains supported constructs."""
    # Check for GROUP BY
    if select.args.get("group"):
        raise ParseError("GROUP BY is not supported")
    
    # Check for ORDER BY
    if select.args.get("order"):
        raise ParseError("ORDER BY is not supported")
    
    # Check for HAVING
    if select.args.get("having"):
        raise ParseError("HAVING is not supported")
    
    # Check for UNION
    if select.args.get("union"):
        raise ParseError("UNION is not supported")
    
    # Check for LIMIT (we could support this but it's not in the spec)
    if select.args.get("limit"):
        raise ParseError("LIMIT is not supported")
    
    # Check for subqueries in FROM
    for join in select.args.get("joins", []):
        if isinstance(join.this, exp.Select):
            raise ParseError("Subqueries in JOIN are not supported")
    
    # Check for aggregations in SELECT
    for expr in select.expressions:
        if _has_aggregation(expr):
            raise ParseError("Aggregations are not supported")


def _has_aggregation(expr):
    """Check if expression contains aggregations."""
    for node in expr.walk():
        if isinstance(node, (exp.Count, exp.Sum, exp.Avg, exp.Max, exp.Min)):
            return True
    return False

