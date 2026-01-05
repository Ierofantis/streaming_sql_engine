"""
Main Engine class - public API for the streaming SQL engine.
"""

from typing import Any
from .core.parser import parse_sql
from .core.planner import build_logical_plan
from .core.executor import execute_plan


class Engine:
    """
    Main interface for the streaming SQL execution engine.
    
    Usage:
        engine = Engine(debug=True)  # Enable debug mode
        engine.register("users", lambda: iter([{"id": 1, "name": "Alice"}]))
        results = engine.query("SELECT users.name FROM users WHERE users.id = 1")
        for row in results:
            print(row)
    """
    
    def __init__(self, debug=False, use_polars=False, first_match_only=False):
        """
        Initialize a new engine instance.
        
        Args:
            debug: If True, enables verbose logging of execution stages
            use_polars: If True, uses Polars for optimizations when available (default: False)
            first_match_only: If True, only returns first match per left key in joins (prevents cartesian products from duplicates)
        """
        self._sources = {}
        self._source_metadata = {}
        self.debug = debug
        self.use_polars = use_polars
        self.first_match_only = first_match_only
    
    def register(
        self,
        table_name,
        source_fn,
        ordered_by=None,
        filename=None
    ):
        """
        Register a table source.
        
        Args:
            table_name: Name of the table as used in SQL queries
            source_fn: Function that returns an iterator of row dictionaries.
                      If the function accepts `dynamic_where` and/or `dynamic_columns` parameters,
                      optimizations (filter pushdown and column pruning) will be applied automatically.
            ordered_by: Optional column name if the source is sorted by this column
                       (enables merge joins)
            filename: Optional filename if source is file-based (enables mmap-based joins
                     for 90-99% memory reduction)
        
        Example:
            # Simple source (no optimizations)
            def simple_source():
                return iter([{"id": 1, "name": "Alice"}])
            engine.register("users", simple_source)
            
            # Optimized source (with protocol)
            def optimized_source(dynamic_where=None, dynamic_columns=None):
                # Build query with optimizations
                query = build_query(dynamic_where, dynamic_columns)
                for row in execute(query):
                    yield row
            engine.register("products", optimized_source)
            # Optimizations apply automatically!
        """
        if not callable(source_fn):
            raise ValueError(f"source_fn must be callable, got {type(source_fn)}")
        
        self._sources[table_name] = source_fn
        self._source_metadata[table_name] = {
            "ordered_by": ordered_by,
            "filename": filename  # Enable mmap-based joins
        }
    
    def query(self, sql):
        """
        Execute a SQL query and return a generator of result rows.
        
        Args:
            sql: SQL query string
            
        Returns:
            Generator yielding dictionaries representing result rows
            
        Raises:
            ValueError: If query contains unsupported constructs
            KeyError: If referenced table is not registered
        """
        if self.debug:
            print("=" * 60)
            print("STREAMING SQL ENGINE - DEBUG MODE")
            print("=" * 60)
            print(f"\n[1/3] PARSING SQL QUERY...")
            print(f"Query:\n{sql}\n")
        
        # Parse SQL into AST
        ast = parse_sql(sql)
        
        if self.debug:
            print(f"✓ SQL parsed successfully")
            print(f"\n[2/3] BUILDING LOGICAL PLAN...")
        
        # Build logical plan
        logical_plan = build_logical_plan(ast, self._sources.keys())
        
        if self.debug:
            print(f"✓ Logical plan built:")
            print(f"  - Root table: {logical_plan.root_table} (alias: {logical_plan.root_alias})")
            print(f"  - Joins: {len(logical_plan.joins)}")
            for i, join in enumerate[Any](logical_plan.joins, 1):
                print(f"    {i}. {join.join_type} JOIN {join.table} ON {join.left_key} = {join.right_key}")
            print(f"  - WHERE clause: {'Yes' if logical_plan.where_expr else 'No'}")
            print(f"  - Projections: {len(logical_plan.projections)}")
            print(f"\n[3/3] EXECUTING QUERY...")
            print(f"Building execution pipeline...\n")
        
        # Execute plan
        return execute_plan(logical_plan, self._sources, self._source_metadata, debug=self.debug, use_polars=self.use_polars, first_match_only=self.first_match_only)
