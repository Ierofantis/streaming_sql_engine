"""
Expression evaluator for WHERE clauses and SELECT projections.
Optimized to reduce isinstance() checks using expression type caching.
"""

from sqlglot import expressions as exp

# Cache expression type classes for faster isinstance checks
_COLUMN_TYPE = exp.Column
_LITERAL_TYPE = exp.Literal
_EQ_TYPE = exp.EQ
_NEQ_TYPE = exp.NEQ
_LT_TYPE = exp.LT
_GT_TYPE = exp.GT
_LTE_TYPE = exp.LTE
_GTE_TYPE = exp.GTE
_AND_TYPE = exp.And
_OR_TYPE = exp.Or
_NOT_TYPE = exp.Not
_IS_TYPE = exp.Is
_ALIAS_TYPE = exp.Alias
_PAREN_TYPE = exp.Paren
_IN_TYPE = exp.In
# Arithmetic operations
_ADD_TYPE = exp.Add
_SUB_TYPE = exp.Sub
_MUL_TYPE = exp.Mul
_DIV_TYPE = exp.Div
_MOD_TYPE = exp.Mod


def _coerce_comparable_types(left, right):
    """
    Coerce two values to comparable types for comparison operations.
    
    Handles cases where one value is a number (int/float) and the other is a string
    that can be converted to a number.
    
    Args:
        left: Left operand
        right: Right operand
        
    Returns:
        Tuple of (left, right) with coerced types
    """
    # If both are same type or both None, no coercion needed
    if type(left) == type(right) or (left is None and right is None):
        return left, right
    
    # If one is None, comparisons are False (except !=)
    if left is None or right is None:
        return left, right
    
    # Try to coerce string to number if one is numeric
    if isinstance(left, (int, float)) and isinstance(right, str):
        try:
            # Try to convert string to number
            if '.' in right or 'e' in right.lower() or 'E' in right:
                right = float(right)
            else:
                right = int(right)
        except (ValueError, TypeError):
            # Can't convert, keep as string (will raise TypeError on comparison)
            pass
    
    elif isinstance(right, (int, float)) and isinstance(left, str):
        try:
            # Try to convert string to number
            if '.' in left or 'e' in left.lower() or 'E' in left:
                left = float(left)
            else:
                left = int(left)
        except (ValueError, TypeError):
            # Can't convert, keep as string (will raise TypeError on comparison)
            pass
    
    return left, right
    
    
def evaluate_expression(expr, row):
    """
    Evaluate a SQL expression against a row.
    
    Args:
        expr: SQL expression AST node
        row: Row dictionary with prefixed column names (e.g., "alias.column")
        
    Returns:
        Evaluated value
    """
    # Use cached type references for faster isinstance checks
    expr_type = type(expr)
    
    if expr_type is _COLUMN_TYPE:
        # Column reference
        col_name = f"{expr.table}.{expr.name}" if expr.table else expr.name
        if col_name in row:
            return row[col_name]
        # Try without table prefix
        if expr.name in row:
            return row[expr.name]
        raise KeyError(f"Column {col_name} not found in row")
    
    elif expr_type is _LITERAL_TYPE:
        # Literal value
        value = expr.this
        # sqlglot stores numeric literals as strings, so convert them back to numbers
        if isinstance(value, str):
            # Try to convert string to number if it looks like a number
            try:
                # Check if it's a float (has decimal point or scientific notation)
                if '.' in value or 'e' in value.lower() or 'E' in value:
                    return float(value)
                # Otherwise try integer
                return int(value)
            except (ValueError, TypeError):
                # Not a number, return as string
                pass
        return value
    
    elif expr_type is _EQ_TYPE:
        # Equality: left = right
        left = evaluate_expression(expr.this, row)
        right = evaluate_expression(expr.expression, row)
        return left == right
    
    elif expr_type is _NEQ_TYPE:
        # Inequality: left != right
        left = evaluate_expression(expr.this, row)
        right = evaluate_expression(expr.expression, row)
        return left != right
    
    elif expr_type is _LT_TYPE:
        # Less than: left < right
        left = evaluate_expression(expr.this, row)
        right = evaluate_expression(expr.expression, row)
        left, right = _coerce_comparable_types(left, right)
        return left < right
    
    elif expr_type is _GT_TYPE:
        # Greater than: left > right
        left = evaluate_expression(expr.this, row)
        right = evaluate_expression(expr.expression, row)
        left, right = _coerce_comparable_types(left, right)
        return left > right
    
    elif expr_type is _LTE_TYPE:
        # Less than or equal: left <= right
        left = evaluate_expression(expr.this, row)
        right = evaluate_expression(expr.expression, row)
        left, right = _coerce_comparable_types(left, right)
        return left <= right
    
    elif expr_type is _GTE_TYPE:
        # Greater than or equal: left >= right
        left = evaluate_expression(expr.this, row)
        right = evaluate_expression(expr.expression, row)
        left, right = _coerce_comparable_types(left, right)
        return left >= right
    
    elif expr_type is _AND_TYPE:
        # AND: left AND right
        left = evaluate_expression(expr.this, row)
        if not left:
            return False
        right = evaluate_expression(expr.expression, row)
        return bool(right)
    
    elif expr_type is _OR_TYPE:
        # OR: left OR right
        left = evaluate_expression(expr.this, row)
        if left:
            return True
        right = evaluate_expression(expr.expression, row)
        return bool(right)
    
    elif expr_type is _NOT_TYPE:
        # NOT: NOT expr
        value = evaluate_expression(expr.this, row)
        return not value
    
    elif expr_type is _IS_TYPE:
        # IS NULL / IS NOT NULL
        value = evaluate_expression(expr.this, row)
        if expr.expression is None:
            # IS NULL
            return value is None
        elif isinstance(expr.expression, exp.Null):
            # IS NULL
            return value is None
        elif isinstance(expr.expression, exp.Not) and isinstance(expr.expression.this, exp.Null):
            # IS NOT NULL
            return value is not None
        else:
            raise ValueError(f"Unsupported IS expression: {expr}")
    
    elif expr_type is _ALIAS_TYPE:
        # Alias - evaluate the inner expression
        return evaluate_expression(expr.this, row)
    
    elif expr_type is _PAREN_TYPE:
        # Parentheses - evaluate inner expression
        return evaluate_expression(expr.this, row)
    
    elif expr_type is _IN_TYPE:
        # IN clause: column IN (value1, value2, ...)
        left = evaluate_expression(expr.this, row)
        # Get the list of values from the expression
        if isinstance(expr.expression, exp.Tuple):
            values = [evaluate_expression(e, row) for e in expr.expression.expressions]
        elif isinstance(expr.expression, (list, tuple)):
            values = [evaluate_expression(e, row) for e in expr.expression]
        else:
            # Could be a subquery (not supported)
            raise ValueError("IN clause with subqueries is not supported")
        return left in values
    
    elif isinstance(expr, exp.Not) and isinstance(expr.this, exp.In):
        # NOT IN clause
        in_expr = expr.this
        left = evaluate_expression(in_expr.this, row)
        if isinstance(in_expr.expression, exp.Tuple):
            values = [evaluate_expression(e, row) for e in in_expr.expression.expressions]
        elif isinstance(in_expr.expression, (list, tuple)):
            values = [evaluate_expression(e, row) for e in in_expr.expression]
        else:
            raise ValueError("NOT IN clause with subqueries is not supported")
        return left not in values
    
    elif isinstance(expr, exp.Where):
        # Where node should be unwrapped before reaching evaluator
        # If it reaches here, unwrap it and evaluate the inner expression
        return evaluate_expression(expr.this, row)
    
    elif expr_type is _ADD_TYPE:
        # Addition: left + right
        left = evaluate_expression(expr.this, row)
        right = evaluate_expression(expr.expression, row)
        # Handle None values
        if left is None or right is None:
            return None
        return left + right
    
    elif expr_type is _SUB_TYPE:
        # Subtraction: left - right
        left = evaluate_expression(expr.this, row)
        right = evaluate_expression(expr.expression, row)
        # Handle None values
        if left is None or right is None:
            return None
        return left - right
    
    elif expr_type is _MUL_TYPE:
        # Multiplication: left * right
        left = evaluate_expression(expr.this, row)
        right = evaluate_expression(expr.expression, row)
        # Handle None values
        if left is None or right is None:
            return None
        return left * right
    
    elif expr_type is _DIV_TYPE:
        # Division: left / right
        left = evaluate_expression(expr.this, row)
        right = evaluate_expression(expr.expression, row)
        # Handle None values
        if left is None or right is None:
            return None
        # Handle division by zero
        if right == 0:
            return None  # Return None for division by zero (SQL behavior)
        return left / right
    
    elif expr_type is _MOD_TYPE:
        # Modulo: left % right
        left = evaluate_expression(expr.this, row)
        right = evaluate_expression(expr.expression, row)
        # Handle None values
        if left is None or right is None:
            return None
        # Handle modulo by zero
        if right == 0:
            return None  # Return None for modulo by zero
        return left % right
    
    else:
        raise ValueError(f"Unsupported expression type: {type(expr).__name__}")

