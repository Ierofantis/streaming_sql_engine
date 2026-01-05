"""
Execution engine - builds iterator pipeline from logical plan.
"""

from .planner import LogicalPlan, JoinInfo
from ..operators.base import (
    ScanIterator,
    FilterIterator,
    ProjectIterator,
    LookupJoinIterator,
    MergeJoinIterator
)

# Try importing Polars operators (optional)
try:
    from ..operators.polars import (
        PolarsLookupJoinIterator,
        PolarsBatchFilterIterator,
        PolarsBatchProjectIterator,
        should_use_polars
    )
    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False
    PolarsBatchFilterIterator = None
    PolarsBatchProjectIterator = None

# Try importing mmap operators (optional)
try:
    from ..operators.mmap import MmapLookupJoinIterator
    MMAP_AVAILABLE = True
except ImportError:
    MMAP_AVAILABLE = False
    MmapLookupJoinIterator = None


def execute_plan(
    plan,
    sources,
    source_metadata,
    debug=False,
    use_polars=False,
    first_match_only=False
):
    """
    Execute a logical plan and return a generator of result row dictionaries.
    
    Args:
        plan: Logical execution plan (with optimizations: required_columns, pushable_where_expr)
        sources: Dictionary mapping table names to source functions
        source_metadata: Dictionary with metadata about sources (e.g., ordered_by)
        
    Returns:
        Generator of result row dictionaries
    """
    # Get required columns for root table (column pruning)
    root_required_columns = plan.required_columns.get(plan.root_table)
    
    # Handle pushable WHERE clause (filter pushdown)
    # For sources that support the protocol, we can push WHERE to the source
    # For other sources, we'll apply it after scanning
    root_source_fn = sources[plan.root_table]
    pushable_where_sql = None
    
    if plan.pushable_where_expr:
        # Convert pushable WHERE expression to SQL string
        from .optimizer import expression_to_sql_string
        try:
            pushable_where_sql = expression_to_sql_string(plan.pushable_where_expr)
            if debug:
                print(f"  [OPTIMIZATION] Pushing WHERE clause to source: {pushable_where_sql}")
        except Exception as e:
            if debug:
                print(f"  [OPTIMIZATION] Could not push WHERE clause: {e}")
            pushable_where_sql = None
    
    # Check if source supports optimization via protocol
    # Protocol: If function accepts dynamic_where or dynamic_columns parameters, optimizations apply
    import inspect
    
    def source_supports_optimizations(source_fn):
        """Check if source implements optimization protocol."""
        try:
            sig = inspect.signature(source_fn)
            params = list(sig.parameters.keys())
            return 'dynamic_where' in params or 'dynamic_columns' in params
        except (ValueError, TypeError):
            # Can't inspect signature, assume no protocol
            return False
    
    # Track whether we actually pushed the WHERE clause to the source
    # If source doesn't support protocol, we need to keep pushable WHERE in plan.where_expr
    where_was_pushed = False
    
    # Apply optimizations if protocol is supported
    if source_supports_optimizations(root_source_fn) and (root_required_columns or pushable_where_sql):
        if debug:
            print(f"  [OPTIMIZATION] Source supports protocol - applying column pruning and filter pushdown")
        
        original_source_fn = root_source_fn
        
        def optimized_source_fn():
            # Call source with optimization parameters
            return original_source_fn(
                dynamic_where=pushable_where_sql,
                dynamic_columns=list(root_required_columns) if root_required_columns else None
            )
        
        root_source_fn = optimized_source_fn
        where_was_pushed = bool(pushable_where_sql)  # Mark that WHERE was pushed
    
    # Start with scan of root table
    if debug:
        if root_required_columns:
            print(f"  [SCAN] Scanning table: {plan.root_table} (columns: {len(root_required_columns)})")
        else:
            print(f"  [SCAN] Scanning table: {plan.root_table}")
    
    iterator = ScanIterator(
        root_source_fn,
        plan.root_table,
        plan.root_alias,
        required_columns=root_required_columns,
        debug=debug
    )
    
    # Apply joins in order
    # Track WHERE clauses that have been pushed to sources
    # IMPORTANT: Initialize remaining_where_expr from plan.where_expr AFTER we know
    # whether pushable WHERE was pushed. If pushable WHERE wasn't pushed, we'll restore it later.
    # For now, start with plan.where_expr (which might be None if optimizer moved everything to pushable_where_expr)
    remaining_where_expr = plan.where_expr
    if debug:
        print(f"  [DEBUG] Initial remaining_where_expr: {remaining_where_expr}")
        print(f"  [DEBUG] plan.pushable_where_expr: {plan.pushable_where_expr}")
        print(f"  [DEBUG] where_was_pushed: {where_was_pushed}")
    
    for i, join_info in enumerate(plan.joins, 1):
        # Get required columns for joined table (needed for optimizations)
        join_required_columns = plan.required_columns.get(join_info.table)
        
        if debug:
            if join_required_columns:
                print(f"  [JOIN {i}/{len(plan.joins)}] {join_info.join_type} JOIN {join_info.table} (columns: {len(join_required_columns)})")
            else:
                print(f"  [JOIN {i}/{len(plan.joins)}] {join_info.join_type} JOIN {join_info.table}")
        
        # Check if we can push WHERE clauses to this joined table
        right_source_fn = sources[join_info.table]
        join_where_sql = None
        table_where_expr = None
        
        # Extract WHERE clauses that reference this joined table
        if remaining_where_expr:
            from .optimizer import extract_table_where_clauses, expression_to_sql_string, remove_expression_from_where
            if debug:
                print(f"  [DEBUG] remaining_where_expr is not None: {remaining_where_expr}")
            try:
                # Try both alias and table name (WHERE clause might use either)
                table_alias = join_info.alias or join_info.table
                table_name = join_info.table
                
                if debug:
                    print(f"  [DEBUG] Checking WHERE clauses for alias '{table_alias}' or table '{table_name}'")
                    print(f"  [DEBUG] Remaining WHERE expr: {remaining_where_expr}")
                
                # Try alias first, then table name
                table_where_expr = extract_table_where_clauses(remaining_where_expr, table_alias)
                if not table_where_expr and table_alias != table_name:
                    # If alias didn't match and it's different from table name, try table name
                    table_where_expr = extract_table_where_clauses(remaining_where_expr, table_name)
                
                if table_where_expr:
                    join_where_sql = expression_to_sql_string(table_where_expr)
                    if debug:
                        print(f"  [OPTIMIZATION] Pushing WHERE clause to {join_info.table}: {join_where_sql}")
                    # Remove pushed clauses from remaining WHERE
                    remaining_where_expr = remove_expression_from_where(remaining_where_expr, table_where_expr)
                    if debug:
                        print(f"  [DEBUG] Remaining WHERE after removal: {remaining_where_expr}")
                else:
                    if debug:
                        print(f"  [DEBUG] No WHERE clauses found for alias '{table_alias}' or table '{table_name}'")
            except Exception as e:
                if debug:
                    print(f"  [OPTIMIZATION] Could not push WHERE to {join_info.table}: {e}")
                    import traceback
                    traceback.print_exc()
                else:
                    # Even if not debug, log the error so we can see what's wrong
                    import traceback
                    print(f"  [ERROR] Could not push WHERE to {join_info.table}: {e}")
                    traceback.print_exc()
                join_where_sql = None
        
        # Apply protocol optimizations to joined table source if supported
        import inspect
        def source_supports_optimizations(source_fn):
            """Check if source implements optimization protocol."""
            try:
                sig = inspect.signature(source_fn)
                params = list(sig.parameters.keys())
                return 'dynamic_where' in params or 'dynamic_columns' in params
            except (ValueError, TypeError):
                return False
        
        optimized_right_source_fn = right_source_fn
        if source_supports_optimizations(right_source_fn) and (join_required_columns or join_where_sql):
            if debug:
                print(f"  [OPTIMIZATION] Source {join_info.table} supports protocol - applying optimizations")
            
            original_right_source_fn = right_source_fn
            
            def optimized_right_source_fn():
                # Call source with optimization parameters
                return original_right_source_fn(
                    dynamic_where=join_where_sql,
                    dynamic_columns=list(join_required_columns) if join_required_columns else None
                )
        
        iterator = _build_join_iterator(
            iterator,
            join_info,
            sources,
            source_metadata,
            plan.required_columns.get(join_info.table),  # Pass required columns
            optimized_right_source_fn=optimized_right_source_fn,  # Pass optimized source
            debug=debug,
            use_polars=use_polars,  # Pass Polars flag
            first_match_only=first_match_only  # Pass first-match-only flag
        )
    
    # Update plan.where_expr to only include clauses not pushed to sources
    # If pushable WHERE clause wasn't actually pushed (source doesn't support protocol),
    # we need to keep it in plan.where_expr so it gets applied after scanning/joining
    if not where_was_pushed and plan.pushable_where_expr:
        # Source doesn't support protocol, so pushable WHERE wasn't pushed
        # Combine pushable WHERE with remaining WHERE
        from .optimizer import _combine_expressions
        import sqlglot.expressions as exp
        if remaining_where_expr:
            plan.where_expr = _combine_expressions([plan.pushable_where_expr, remaining_where_expr], exp.And)
        else:
            plan.where_expr = plan.pushable_where_expr
        if debug:
            print(f"  [DEBUG] Restored pushable WHERE clause to plan.where_expr (wasn't pushed)")
            print(f"  [DEBUG] Final plan.where_expr: {plan.where_expr}")
    else:
        # WHERE was pushed or there was no pushable WHERE, use remaining WHERE
        plan.where_expr = remaining_where_expr
        if debug:
            print(f"  [DEBUG] Using remaining_where_expr: {plan.where_expr}")
    
    # Apply WHERE filter if present (non-pushable conditions)
    # Must be applied AFTER joins since remaining WHERE conditions may reference joined tables
    if plan.where_expr:
        if debug:
            print(f"  [FILTER] Applying WHERE clause (non-pushable conditions)")
            print(f"  [DEBUG] WHERE expression type: {type(plan.where_expr)}")
            print(f"  [DEBUG] WHERE expression: {plan.where_expr}")
            if hasattr(plan.where_expr, 'this') and hasattr(plan.where_expr, 'expression'):
                left = plan.where_expr.this
                right = plan.where_expr.expression
                print(f"  [DEBUG]   Left: {left} (type={type(left)})")
                print(f"  [DEBUG]   Right: {right} (type={type(right)})")
                if hasattr(right, 'this'):
                    print(f"  [DEBUG]   Right value: {right.this} (type={type(right.this)})")
        
        # Apply WHERE filter
        # IMPORTANT: For table-prefixed columns (like products.checked), we use Python FilterIterator
        # to ensure correctness. Polars filtering can be unreliable with table-prefixed columns
        # because the column names in the DataFrame might not match the filter expression.
        # Python FilterIterator uses evaluate_expression() which correctly handles table prefixes.
        
        # Check if WHERE expression has table-prefixed columns
        # IMPORTANT: Table-prefixed columns (like products.checked) require Python FilterIterator
        # because Polars filtering has issues with table prefixes and type mismatches
        has_table_prefix = False
        try:
            from sqlglot import expressions as exp
            
            def check_for_table_prefix(expr):
                """Recursively check if expression has table-prefixed columns."""
                if expr is None:
                    return False
                if isinstance(expr, exp.Column):
                    # Check if column has table prefix
                    if expr.table:
                        return True
                # Recursively check nested expressions
                if hasattr(expr, 'this'):
                    if check_for_table_prefix(expr.this):
                        return True
                if hasattr(expr, 'expression'):
                    if check_for_table_prefix(expr.expression):
                        return True
                return False
            
            has_table_prefix = check_for_table_prefix(plan.where_expr)
            if debug:
                print(f"  [DEBUG] Table prefix check: has_table_prefix={has_table_prefix}")
                # Also show the expression structure for debugging
                if hasattr(plan.where_expr, 'this') and isinstance(plan.where_expr.this, exp.Column):
                    col = plan.where_expr.this
                    print(f"  [DEBUG]   Column: {col.name}, table: {getattr(col, 'table', None)}")
        except Exception as e:
            # If check fails, assume it might have table prefixes (safer)
            has_table_prefix = True
            if debug:
                print(f"  [DEBUG] Table prefix check failed: {e}, assuming True (safer)")
                import traceback
                traceback.print_exc()
        
        # Use Polars filtering only if no table prefixes AND Polars is available
        # IMPORTANT: Always use Python FilterIterator for table-prefixed columns to ensure correctness
        # Polars filtering has issues with:
        # 1. Table-prefixed column names (products.checked vs checked)
        # 2. Type mismatches (string '1' vs int 1)
        # Python FilterIterator handles both correctly
        if (not has_table_prefix and use_polars and POLARS_AVAILABLE and PolarsBatchFilterIterator is not None):
            if debug:
                print(f"  [POLARS] Attempting Polars vectorized filtering (no table prefixes detected)")
            try:
                iterator = PolarsBatchFilterIterator(iterator, plan.where_expr, batch_size=10000, debug=debug)
                if debug:
                    print(f"  [POLARS] ✓ Polars filtering initialized successfully")
            except Exception as e:
                if debug:
                    print(f"  [POLARS] ✗ Filtering failed: {e}, falling back to Python FilterIterator")
                    import traceback
                    traceback.print_exc()
                # Fallback to Python filter - this MUST work
                iterator = FilterIterator(iterator, plan.where_expr, debug=debug)
        else:
            # Use Python filter iterator (more reliable for table-prefixed columns and type handling)
            if debug:
                if has_table_prefix:
                    print(f"  [FILTER] Using Python FilterIterator (table-prefixed columns detected: products.checked)")
                else:
                    print(f"  [FILTER] Using Python FilterIterator (Polars not available or not requested)")
            iterator = FilterIterator(iterator, plan.where_expr, debug=debug)
    else:
        if debug:
            print(f"  [DEBUG] No WHERE clause to apply (plan.where_expr is None)")
    
    # Apply projection
    if debug:
        print(f"  [PROJECT] Applying SELECT projection")
        print(f"\nPipeline ready. Starting row processing...\n")
        print("-" * 60)
    
    # Use Polars batch projection if available and beneficial
    if (use_polars and POLARS_AVAILABLE and PolarsBatchProjectIterator is not None):
        if debug:
            print(f"  [POLARS] Using Polars vectorized projection (SIMD-accelerated)")
        try:
            iterator = PolarsBatchProjectIterator(iterator, plan.projections, batch_size=10000, debug=debug)
        except Exception as e:
            if debug:
                print(f"  [POLARS] ✗ Projection failed: {e}, using Python")
            iterator = ProjectIterator(iterator, plan.projections, debug=debug)
    else:
        iterator = ProjectIterator(iterator, plan.projections, debug=debug)
    
    return iterator
    
    
def _build_join_iterator(
    left_iterator,
    join_info,
    sources,
    source_metadata,
    required_columns=None,
    optimized_right_source_fn=None,
    debug=False,
    use_polars=False,
    first_match_only=False
):
    """
    Build appropriate join iterator based on source capabilities.
    
    Args:
        required_columns: Set of column names needed from right table (for column pruning)
        optimized_right_source_fn: Optional optimized source function (with protocol applied)
    """
    # Use optimized source if provided, otherwise use original
    if optimized_right_source_fn is not None:
        right_source = optimized_right_source_fn
    else:
        right_source = sources[join_info.table]
    right_metadata = source_metadata.get(join_info.table, {})
    
    # Apply column pruning to right source if needed
    # For database sources, this would be handled at source creation
    # For other sources, LookupJoinIterator will handle it via ScanIterator
    
    # Check if both sides are ordered by join keys
    # Extract left table name to check its metadata
    left_table_name = _extract_table_from_key(join_info.left_key)
    left_metadata = source_metadata.get(left_table_name, {}) if left_table_name else {}
    left_ordered_by = left_metadata.get("ordered_by")
    right_ordered_by = right_metadata.get("ordered_by")
    
    # Extract column names from join keys
    left_join_column = _extract_column_from_key(join_info.left_key)
    right_join_column = _extract_column_from_key(join_info.right_key)
    
    # For merge join, we need BOTH sides sorted on their respective join keys
    # Merge join REQUIRES sorted data - if data isn't sorted, it produces incorrect results
    # Only use merge join when:
    # 1. Explicitly told both sides are sorted (via ordered_by metadata)
    # 2. NOT using Polars (Polars is faster for unsorted data, merge join only for sorted)
    use_merge_join = (
        not use_polars and  # Don't use merge join if Polars is explicitly requested
        left_ordered_by is not None and
        left_ordered_by == left_join_column and
        right_ordered_by is not None and
        right_ordered_by == right_join_column
    )
    
    if use_merge_join:
        if debug:
            iterator_type = "MERGE JOIN"
            print(f"      Using {iterator_type} (sorted data)")
            print(f"      Left ordered_by: {left_ordered_by}, Right ordered_by: {right_ordered_by}")
            print(f"      Left join column: {left_join_column}, Right join column: {right_join_column}")
        return MergeJoinIterator(
            left_iterator,
            right_source,
            join_info.left_key,
            join_info.right_key,
            join_info.join_type,
            join_info.table,
            join_info.alias,
            debug=debug
        )
    else:
        # When use_polars=True is explicitly set, prioritize Polars over mmap
        # (unless user explicitly wants mmap by providing filename)
        right_metadata = source_metadata.get(join_info.table, {})
        right_table_filename = right_metadata.get("filename")
        
        # Only use mmap if:
        # 1. Filename is explicitly provided (user wants mmap)
        # 2. Polars is NOT explicitly requested (use_polars=False)
        # This ensures that when user sets use_polars=True, Polars is used instead of mmap
        use_mmap = (
            MMAP_AVAILABLE and 
            MmapLookupJoinIterator is not None and 
            right_table_filename and
            not use_polars  # Don't use mmap if Polars is explicitly requested
        )
        
        if use_mmap:
            if debug:
                iterator_type = "MMAP LOOKUP JOIN"
                if required_columns:
                    print(f"      Using {iterator_type} (low memory, columns: {len(required_columns)})...")
                else:
                    print(f"      Using {iterator_type} (low memory, position-based index)...")
            try:
                return MmapLookupJoinIterator(
                    left_iterator,
                    right_source,
                    join_info.left_key,
                    join_info.right_key,
                    join_info.join_type,
                    join_info.table,
                    join_info.alias,
                    right_table_filename=right_table_filename,
                    required_columns=required_columns,
                    debug=debug
                )
            except Exception as e:
                if debug:
                    print(f"      ⚠️  Mmap join failed: {e}")
                    import traceback
                    traceback.print_exc()
                    print(f"      Falling back to Polars/Python")
                # Fallback to Polars or Python
                pass
        
        # Decide between Polars and Python implementation
        # When use_polars=True, Polars is prioritized (unless mmap explicitly requested via filename)
        # IMPORTANT: Call should_use_polars() only ONCE to avoid double I/O
        should_use_polars_result = False
        if use_polars and POLARS_AVAILABLE:
            if debug:
                print(f"      [POLARS] Checking if Polars should be used...")
            try:
                should_use_polars_result = should_use_polars(right_source, threshold=10000)
                if debug:
                    if not should_use_polars_result:
                        print(f"      [POLARS] Right side too small or estimation failed, using Python join instead")
            except Exception as e:
                if debug:
                    print(f"      [POLARS] Estimation failed: {e}, using Python join instead")
                should_use_polars_result = False
        elif debug:
            if not use_polars:
                print(f"      [POLARS] use_polars=False, using Python join")
            elif not POLARS_AVAILABLE:
                print(f"      [POLARS] Polars not available (not installed), falling back to Python")
        
        if (use_polars and POLARS_AVAILABLE and should_use_polars_result):
            if debug:
                iterator_type = "POLARS LOOKUP JOIN"
                print(f"      [POLARS] ✓ Using {iterator_type} (SIMD-accelerated)")
                if required_columns:
                    print(f"      [POLARS]   Columns: {len(required_columns)}")
                if first_match_only:
                    print(f"      [POLARS]   ⚡ First-match-only mode enabled")
            try:
                return PolarsLookupJoinIterator(
                    left_iterator,
                    right_source,
                    join_info.left_key,
                    join_info.right_key,
                    join_info.join_type,
                    join_info.table,
                    join_info.alias,
                    batch_size=10000,
                    required_columns=required_columns,
                    debug=debug,
                    first_match_only=first_match_only
                )
            except Exception as e:
                if debug:
                    print(f"      [POLARS] ✗ Polars join failed: {e}, falling back to Python")
                # Fallback to Python implementation
                pass
        
        if debug:
            iterator_type = "LOOKUP JOIN (Python)"
            if required_columns:
                print(f"      Using {iterator_type} (building index, columns: {len(required_columns)})...")
            else:
                print(f"      Using {iterator_type} (building index...)")
            if first_match_only:
                print(f"      ⚡ First-match-only mode enabled")
        return LookupJoinIterator(
            left_iterator,
            right_source,
            join_info.left_key,
            join_info.right_key,
            join_info.join_type,
            join_info.table,
            join_info.alias,
            required_columns=required_columns,
            debug=debug,
            first_match_only=first_match_only
        )


def _extract_table_from_key(key):
    """Extract table alias from a key like 'alias.column'."""
    if "." in key:
        return key.split(".", 1)[0]
    return None


def _extract_column_from_key(key):
    """Extract column name from a key like 'alias.column'."""
    if "." in key:
        return key.split(".", 1)[1]
    return key

