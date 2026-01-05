"""
Query optimization: Column pruning and filter pushdown.

This module analyzes queries to:
1. Determine which columns are actually needed from each table
2. Identify WHERE clause conditions that can be pushed to data sources
"""

from sqlglot import expressions as exp
from typing import Set, Dict, Optional, Tuple
from .planner import LogicalPlan, JoinInfo


def analyze_required_columns(plan: LogicalPlan) -> Dict[str, Set[str]]:
    """
    Analyze which columns are needed from each table.
    
    Args:
        plan: Logical execution plan
        
    Returns:
        Dictionary mapping table name to set of required column names
        (without table prefix, e.g., "id" not "users.id")
    """
    required_columns = {}
    
    # Track table aliases
    table_aliases = {plan.root_table: plan.root_alias or plan.root_table}
    for join in plan.joins:
        table_aliases[join.table] = join.alias or join.table
    
    # 1. Columns needed for SELECT projections
    for projection in plan.projections:
        _extract_columns_from_expr(projection, required_columns, table_aliases)
    
    # 2. Columns needed for WHERE clause
    if plan.where_expr:
        _extract_columns_from_expr(plan.where_expr, required_columns, table_aliases)
    
    # 3. Columns needed for JOIN keys
    for join in plan.joins:
        # Left key (from already processed tables)
        left_table = _extract_table_from_key(join.left_key)
        if left_table:
            left_col = _extract_column_from_key(join.left_key)
            table_name = _find_table_by_alias(left_table, table_aliases)
            if table_name:
                if table_name not in required_columns:
                    required_columns[table_name] = set()
                required_columns[table_name].add(left_col)
        
        # Right key (from joined table)
        right_col = _extract_column_from_key(join.right_key)
        if join.table not in required_columns:
            required_columns[join.table] = set()
        required_columns[join.table].add(right_col)
    
    return required_columns


def _extract_columns_from_expr(expr, required_columns: Dict[str, Set[str]], 
                               table_aliases: Dict[str, str]):
    """Recursively extract column references from an expression."""
    if isinstance(expr, exp.Column):
        table_alias = expr.table
        column_name = expr.name
        
        if table_alias:
            # Find which table this alias refers to
            table_name = _find_table_by_alias(table_alias, table_aliases)
            if table_name:
                if table_name not in required_columns:
                    required_columns[table_name] = set()
                required_columns[table_name].add(column_name)
        else:
            # Column without table - add to all tables (ambiguous, but safe)
            for table_name in table_aliases.keys():
                if table_name not in required_columns:
                    required_columns[table_name] = set()
                required_columns[table_name].add(column_name)
    
    elif isinstance(expr, exp.Alias):
        # Recurse into aliased expression
        _extract_columns_from_expr(expr.this, required_columns, table_aliases)
    
    elif isinstance(expr, exp.Paren):
        # Recurse into parenthesized expression
        _extract_columns_from_expr(expr.this, required_columns, table_aliases)
    
    elif isinstance(expr, (exp.EQ, exp.NEQ, exp.LT, exp.GT, exp.LTE, exp.GTE)):
        # Binary comparison - check both sides
        _extract_columns_from_expr(expr.this, required_columns, table_aliases)
        _extract_columns_from_expr(expr.expression, required_columns, table_aliases)
    
    elif isinstance(expr, (exp.And, exp.Or)):
        # Boolean operators - check both sides
        _extract_columns_from_expr(expr.this, required_columns, table_aliases)
        _extract_columns_from_expr(expr.expression, required_columns, table_aliases)
    
    elif isinstance(expr, exp.Not):
        # NOT operator - recurse
        _extract_columns_from_expr(expr.this, required_columns, table_aliases)
    
    elif isinstance(expr, exp.Is):
        # IS NULL / IS NOT NULL
        _extract_columns_from_expr(expr.this, required_columns, table_aliases)
    
    elif isinstance(expr, exp.In):
        # IN clause
        _extract_columns_from_expr(expr.this, required_columns, table_aliases)
        if isinstance(expr.expression, exp.Tuple):
            for e in expr.expression.expressions:
                _extract_columns_from_expr(e, required_columns, table_aliases)
    
    # Literals and other expressions don't reference columns


def _find_table_by_alias(alias: str, table_aliases: Dict[str, str]) -> Optional[str]:
    """Find table name by its alias."""
    for table_name, table_alias in table_aliases.items():
        if table_alias == alias or table_name == alias:
            return table_name
    return None


def _extract_table_from_key(key: str) -> Optional[str]:
    """Extract table alias from a key like 'alias.column'."""
    if "." in key:
        return key.split(".", 1)[0]
    return None


def _extract_column_from_key(key: str) -> str:
    """Extract column name from a key like 'alias.column'."""
    if "." in key:
        return key.split(".", 1)[1]
    return key


def analyze_filter_pushdown(plan: LogicalPlan) -> Tuple[Optional[exp.Expression], Optional[exp.Expression]]:
    """
    Analyze WHERE clause to determine what can be pushed to root table.
    
    Only conditions that reference ONLY the root table can be pushed down.
    Conditions referencing joined tables must be evaluated after joins.
    
    Args:
        plan: Logical execution plan
        
    Returns:
        Tuple of (pushable_where_expr, remaining_where_expr)
        - pushable_where_expr: WHERE conditions that only reference root table (can be pushed)
        - remaining_where_expr: WHERE conditions that reference other tables (must stay)
    """
    if not plan.where_expr:
        return None, None
    
    root_alias = plan.root_alias or plan.root_table
    
    # Get all table aliases (for joined tables)
    joined_aliases = set()
    for join in plan.joins:
        joined_aliases.add(join.alias or join.table)
    
    # Analyze expression to split pushable vs non-pushable
    pushable_parts = []
    remaining_parts = []
    
    _split_where_expression(
        plan.where_expr,
        root_alias,
        joined_aliases,
        pushable_parts,
        remaining_parts
    )
    
    # Reconstruct expressions
    pushable_expr = _combine_expressions(pushable_parts, exp.And) if pushable_parts else None
    remaining_expr = _combine_expressions(remaining_parts, exp.And) if remaining_parts else None
    
    return pushable_expr, remaining_expr


def _split_where_expression(expr, root_alias: str, joined_aliases: Set[str],
                           pushable_parts: list, remaining_parts: list):
    """
    Recursively split WHERE expression into pushable and non-pushable parts.
    """
    if isinstance(expr, exp.And):
        # AND: both sides must be pushable for whole expression to be pushable
        _split_where_expression(expr.this, root_alias, joined_aliases, pushable_parts, remaining_parts)
        _split_where_expression(expr.expression, root_alias, joined_aliases, pushable_parts, remaining_parts)
    
    elif isinstance(expr, exp.Or):
        # OR: if either side references joined tables, whole expression can't be pushed
        # For simplicity, we don't push OR expressions (could be optimized further)
        remaining_parts.append(expr)
    
    elif isinstance(expr, exp.Not):
        # NOT: check if inner expression is pushable
        inner_pushable = _is_expression_pushable(expr.this, root_alias, joined_aliases)
        if inner_pushable:
            pushable_parts.append(expr)
        else:
            remaining_parts.append(expr)
    
    else:
        # Leaf expression (EQ, NEQ, LT, GT, etc.)
        if _is_expression_pushable(expr, root_alias, joined_aliases):
            pushable_parts.append(expr)
        else:
            remaining_parts.append(expr)


def _is_expression_pushable(expr, root_alias: str, joined_aliases: Set[str]) -> bool:
    """
    Check if an expression only references the root table (can be pushed).
    """
    referenced_aliases = set()
    _extract_table_aliases_from_expr(expr, referenced_aliases)
    
    # Can push if only references root table (or no table references)
    return len(referenced_aliases - {root_alias}) == 0


def _extract_table_aliases_from_expr(expr, aliases: Set[str]):
    """Extract all table aliases referenced in an expression."""
    if isinstance(expr, exp.Column):
        if expr.table:
            aliases.add(expr.table)
    
    elif isinstance(expr, exp.Alias):
        _extract_table_aliases_from_expr(expr.this, aliases)
    
    elif isinstance(expr, exp.Paren):
        _extract_table_aliases_from_expr(expr.this, aliases)
    
    elif isinstance(expr, (exp.EQ, exp.NEQ, exp.LT, exp.GT, exp.LTE, exp.GTE)):
        _extract_table_aliases_from_expr(expr.this, aliases)
        _extract_table_aliases_from_expr(expr.expression, aliases)
    
    elif isinstance(expr, (exp.And, exp.Or)):
        _extract_table_aliases_from_expr(expr.this, aliases)
        _extract_table_aliases_from_expr(expr.expression, aliases)
    
    elif isinstance(expr, exp.Not):
        _extract_table_aliases_from_expr(expr.this, aliases)
    
    elif isinstance(expr, exp.Is):
        _extract_table_aliases_from_expr(expr.this, aliases)
    
    elif isinstance(expr, exp.In):
        _extract_table_aliases_from_expr(expr.this, aliases)
        if isinstance(expr.expression, exp.Tuple):
            for e in expr.expression.expressions:
                _extract_table_aliases_from_expr(e, aliases)


def _combine_expressions(expressions: list, operator_class) -> Optional[exp.Expression]:
    """Combine list of expressions with AND/OR operator."""
    if not expressions:
        return None
    if len(expressions) == 1:
        return expressions[0]
    
    # Combine from left to right
    result = expressions[0]
    for expr in expressions[1:]:
        result = operator_class(this=result, expression=expr)
    return result


def expression_to_sql_string(expr: exp.Expression) -> str:
    """
    Convert a SQL expression to SQL string (for WHERE clause pushdown).
    
    This is a simplified version - in production, you'd want more robust SQL generation.
    """
    if isinstance(expr, exp.Column):
        return f"{expr.table}.{expr.name}" if expr.table else expr.name
    
    elif isinstance(expr, exp.Literal):
        if isinstance(expr.this, str):
            return f"'{expr.this.replace(chr(39), chr(39)+chr(39))}'"  # Escape single quotes
        return str(expr.this)
    
    elif isinstance(expr, exp.EQ):
        left = expression_to_sql_string(expr.this)
        right = expression_to_sql_string(expr.expression)
        return f"{left} = {right}"
    
    elif isinstance(expr, exp.NEQ):
        left = expression_to_sql_string(expr.this)
        right = expression_to_sql_string(expr.expression)
        return f"{left} != {right}"
    
    elif isinstance(expr, exp.LT):
        left = expression_to_sql_string(expr.this)
        right = expression_to_sql_string(expr.expression)
        return f"{left} < {right}"
    
    elif isinstance(expr, exp.GT):
        left = expression_to_sql_string(expr.this)
        right = expression_to_sql_string(expr.expression)
        return f"{left} > {right}"
    
    elif isinstance(expr, exp.LTE):
        left = expression_to_sql_string(expr.this)
        right = expression_to_sql_string(expr.expression)
        return f"{left} <= {right}"
    
    elif isinstance(expr, exp.GTE):
        left = expression_to_sql_string(expr.this)
        right = expression_to_sql_string(expr.expression)
        return f"{left} >= {right}"
    
    elif isinstance(expr, exp.And):
        left = expression_to_sql_string(expr.this)
        right = expression_to_sql_string(expr.expression)
        return f"({left} AND {right})"
    
    elif isinstance(expr, exp.Or):
        left = expression_to_sql_string(expr.this)
        right = expression_to_sql_string(expr.expression)
        return f"({left} OR {right})"
    
    elif isinstance(expr, exp.Not):
        inner = expression_to_sql_string(expr.this)
        return f"NOT ({inner})"
    
    elif isinstance(expr, exp.Is):
        left = expression_to_sql_string(expr.this)
        if expr.expression is None or (isinstance(expr.expression, exp.Null)):
            return f"{left} IS NULL"
        elif isinstance(expr.expression, exp.Not) and isinstance(expr.expression.this, exp.Null):
            return f"{left} IS NOT NULL"
    
    elif isinstance(expr, exp.In):
        left = expression_to_sql_string(expr.this)
        if isinstance(expr.expression, exp.Tuple):
            values = [expression_to_sql_string(e) for e in expr.expression.expressions]
            return f"{left} IN ({', '.join(values)})"
    
    # Fallback: use sqlglot's SQL generation
    try:
        return expr.sql()
    except:
        return str(expr)


def extract_table_where_clauses(where_expr, table_alias: str) -> Optional[exp.Expression]:
    """
    Extract WHERE clauses that reference a specific table.
    
    Args:
        where_expr: WHERE expression (may be None)
        table_alias: Table alias to extract clauses for
        
    Returns:
        Expression containing only clauses referencing the specified table, or None
    """
    if not where_expr:
        return None
    
    # Extract clauses that reference this table
    matching_parts = []
    
    def _extract_for_table(expr):
        """Recursively extract expressions referencing the table."""
        if isinstance(expr, exp.And):
            # AND: check both sides
            left = _extract_for_table(expr.this)
            right = _extract_for_table(expr.expression)
            
            if left and right:
                return exp.And(this=left, expression=right)
            elif left:
                return left
            elif right:
                return right
            else:
                return None
        
        elif isinstance(expr, exp.Or):
            # OR: both sides must reference the table
            left = _extract_for_table(expr.this)
            right = _extract_for_table(expr.expression)
            
            if left and right:
                return exp.Or(this=left, expression=right)
            else:
                return None
        
        elif isinstance(expr, exp.Not):
            # NOT: check inner expression
            inner = _extract_for_table(expr.this)
            if inner:
                return exp.Not(this=inner)
            else:
                return None
        
        else:
            # Leaf expression - check if it references the table
            referenced_aliases = set()
            _extract_table_aliases_from_expr(expr, referenced_aliases)
            
            if table_alias in referenced_aliases and len(referenced_aliases) == 1:
                # Only references this table
                return expr
            else:
                return None
    
    result = _extract_for_table(where_expr)
    return result


def remove_expression_from_where(where_expr: Optional[exp.Expression], expr_to_remove: exp.Expression) -> Optional[exp.Expression]:
    """
    Remove a specific expression from a WHERE clause.
    
    Args:
        where_expr: Original WHERE expression
        expr_to_remove: Expression to remove
        
    Returns:
        Updated WHERE expression with the expression removed, or None if nothing remains
    """
    if not where_expr:
        return None
    
    # Simple approach: if expressions are equal, return None
    # For AND expressions, try to remove one side
    if isinstance(where_expr, exp.And):
        left = where_expr.this
        right = where_expr.expression
        
        # Check if left or right matches what we want to remove
        if (isinstance(left, type(expr_to_remove)) and 
            hasattr(left, 'sql') and hasattr(expr_to_remove, 'sql') and
            left.sql() == expr_to_remove.sql()):
            # Remove left side
            return right if right else None
        
        if (isinstance(right, type(expr_to_remove)) and 
            hasattr(right, 'sql') and hasattr(expr_to_remove, 'sql') and
            right.sql() == expr_to_remove.sql()):
            # Remove right side
            return left if left else None
        
        # Try recursively
        new_left = remove_expression_from_where(left, expr_to_remove)
        new_right = remove_expression_from_where(right, expr_to_remove)
        
        if new_left and new_right:
            return exp.And(this=new_left, expression=new_right)
        elif new_left:
            return new_left
        elif new_right:
            return new_right
        else:
            return None
    
    # For other expressions, check if they match
    if (isinstance(where_expr, type(expr_to_remove)) and 
        hasattr(where_expr, 'sql') and hasattr(expr_to_remove, 'sql') and
        where_expr.sql() == expr_to_remove.sql()):
        return None
    
    # Doesn't match, return original
    return where_expr
