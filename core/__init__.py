"""
Core query processing components: parsing, planning, optimization, execution, and evaluation.
"""

from .parser import parse_sql, ParseError
from .planner import build_logical_plan, LogicalPlan, JoinInfo
from .optimizer import (
    analyze_required_columns,
    analyze_filter_pushdown,
    expression_to_sql_string,
    extract_table_where_clauses,
    remove_expression_from_where
)
from .executor import execute_plan
from .evaluator import evaluate_expression

__all__ = [
    "parse_sql",
    "ParseError",
    "build_logical_plan",
    "LogicalPlan",
    "JoinInfo",
    "analyze_required_columns",
    "analyze_filter_pushdown",
    "expression_to_sql_string",
    "extract_table_where_clauses",
    "remove_expression_from_where",
    "execute_plan",
    "evaluate_expression",
]

