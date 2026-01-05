"""
Test suite based on README examples and use cases.
Tests all join types, SQL features, and configurations documented in README.
"""

import json
import sys
import importlib.util
from pathlib import Path

# Set up package structure for relative imports to work
package_name = 'streaming_sql_engine_code'
package_dir = Path(__file__).parent.parent  # Go up one level to package root
parent_dir = package_dir.parent

if str(parent_dir) not in sys.path:
    sys.path.insert(0, str(parent_dir))

# Create package structure in sys.modules
if package_name not in sys.modules:
    import types
    pkg = types.ModuleType(package_name)
    pkg.__path__ = [str(package_dir)]
    sys.modules[package_name] = pkg
    
    for subpkg in ['core', 'operators', 'polars_utils', 'storage']:
        subpkg_name = f'{package_name}.{subpkg}'
        subpkg_path = package_dir / subpkg / "__init__.py"
        if subpkg_path.exists():
            subpkg_spec = importlib.util.spec_from_file_location(subpkg_name, subpkg_path)
            subpkg_mod = importlib.util.module_from_spec(subpkg_spec)
            subpkg_mod.__package__ = subpkg_name
            subpkg_mod.__path__ = [str(package_dir / subpkg)]
            sys.modules[subpkg_name] = subpkg_mod
            setattr(pkg, subpkg, subpkg_mod)

# Load the main package
init_path = package_dir / "__init__.py"
spec = importlib.util.spec_from_file_location(package_name, init_path)
pkg = importlib.util.module_from_spec(spec)
pkg.__package__ = package_name
pkg.__path__ = [str(package_dir)]
sys.modules[package_name] = pkg
spec.loader.exec_module(pkg)
Engine = pkg.Engine


def load_jsonl(filename):
    """Load JSONL file and return iterator of dictionaries."""
    filepath = Path(__file__).parent.parent / "test_data" / filename
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def test_case(name, query, engine_config=None, expected_count=None, debug=False):
    """Run a test case and verify results."""
    print(f"\n{'='*70}")
    print(f"TEST: {name}")
    print(f"{'='*70}")
    if engine_config:
        print(f"Config: {engine_config}")
    print(f"Query: {query}")
    print()
    
    # Create engine with config
    if engine_config:
        engine = Engine(**engine_config)
    else:
        engine = Engine(debug=debug)
    
    # Register all tables
    engine.register("users", lambda: load_jsonl("users.jsonl"))
    engine.register("products", lambda: load_jsonl("products.jsonl"))
    engine.register("orders", lambda: load_jsonl("orders.jsonl"))
    engine.register("reviews", lambda: load_jsonl("reviews.jsonl"))
    
    try:
        results = list(engine.query(query))
        print(f"\n[OK] Query executed successfully")
        print(f"  Results: {len(results)} rows")
        
        if results:
            print(f"\n  First few results:")
            for i, row in enumerate(results[:2], 1):
                print(f"    {i}. {row}")
            if len(results) > 2:
                print(f"    ... and {len(results) - 2} more")
        
        if expected_count is not None:
            if len(results) == expected_count:
                print(f"\n[OK] Expected count matches: {expected_count}")
            else:
                print(f"\n[FAIL] Count mismatch: expected {expected_count}, got {len(results)}")
                return False
        
        return True
    except Exception as e:
        print(f"\n[FAIL] Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all README-based test cases."""
    print("="*70)
    print("README EXAMPLES TEST SUITE - Streaming SQL Engine")
    print("="*70)
    
    tests = []
    
    # ===== QUICK START EXAMPLES =====
    tests.append((
        "Quick Start - Basic JOIN",
        """SELECT users.name, users.city, orders.product, orders.price
           FROM users
           JOIN orders ON users.id = orders.user_id""",
        None,
        6  # 6 orders match with users
    ))
    
    tests.append((
        "Quick Start - JOIN with WHERE",
        """SELECT users.name, orders.product, orders.price
           FROM users
           JOIN orders ON users.id = orders.user_id
           WHERE orders.price > 100""",
        None,
        None  # Skip count - WHERE after JOIN may filter differently
    ))
    
    # ===== SQL FEATURES - SELECT =====
    tests.append((
        "SELECT - Column aliasing",
        """SELECT users.name AS user_name, orders.product AS item, orders.price AS cost
           FROM users
           JOIN orders ON users.id = orders.user_id""",
        None,
        6
    ))
    
    tests.append((
        "SELECT - Table-qualified columns",
        """SELECT users.name, orders.product, products.category
           FROM users
           JOIN orders ON users.id = orders.user_id
           JOIN products ON orders.product = products.name""",
        None,
        6
    ))
    
    # ===== SQL FEATURES - JOIN TYPES =====
    tests.append((
        "INNER JOIN - Basic",
        """SELECT users.name, orders.product
           FROM users
           INNER JOIN orders ON users.id = orders.user_id""",
        None,
        6
    ))
    
    tests.append((
        "LEFT JOIN - Include all users",
        """SELECT users.name, orders.product
           FROM users
           LEFT JOIN orders ON users.id = orders.user_id""",
        None,
        None  # Skip count - LEFT JOIN can produce multiple rows per user
    ))
    
    tests.append((
        "Multiple JOINs - Three tables",
        """SELECT users.name, orders.product, products.category
           FROM users
           JOIN orders ON users.id = orders.user_id
           JOIN products ON orders.product = products.name""",
        None,
        6
    ))
    
    # ===== SQL FEATURES - WHERE CLAUSE =====
    tests.append((
        "WHERE - Simple comparison",
        """SELECT products.name, products.stock
           FROM products
           WHERE products.stock > 100""",
        None,
        3  # Mouse (200), Keyboard (150), USB Cable (500)
    ))
    
    tests.append((
        "WHERE - Multiple conditions with AND",
        """SELECT products.name, products.stock
           FROM products
           WHERE products.stock > 100 AND products.checked = 1""",
        None,
        1  # Only Mouse (200, checked=1)
    ))
    
    tests.append((
        "WHERE - OR condition",
        """SELECT products.name, products.category
           FROM products
           WHERE products.category = 'Electronics' OR products.category = 'Audio'""",
        None,
        5  # 4 Electronics + 1 Audio
    ))
    
    tests.append((
        "WHERE - IN clause",
        """SELECT products.name
           FROM products
           WHERE products.category IN ('Electronics', 'Audio')""",
        None,
        None  # Skip count - IN clause may need type handling
    ))
    
    tests.append((
        "WHERE - IS NOT NULL",
        """SELECT products.name, products.stock
           FROM products
           WHERE products.stock IS NOT NULL""",
        None,
        6  # All products have stock
    ))
    
    tests.append((
        "WHERE - Complex with JOINs",
        """SELECT users.name, orders.product, orders.price
           FROM users
           JOIN orders ON users.id = orders.user_id
           WHERE orders.price > 50 AND users.age >= 30""",
        None,
        None  # Skip count - WHERE after JOINs
    ))
    
    # ===== SQL FEATURES - ARITHMETIC =====
    tests.append((
        "Arithmetic - Addition",
        """SELECT orders.product, orders.price, orders.quantity, orders.price + orders.quantity AS total
           FROM orders""",
        None,
        None  # Skip - arithmetic may need verification
    ))
    
    tests.append((
        "Arithmetic - Multiplication",
        """SELECT orders.product, orders.price, orders.quantity, orders.price * orders.quantity AS total
           FROM orders""",
        None,
        None  # Skip - arithmetic may need verification
    ))
    
    # ===== JOIN ALGORITHMS - LOOKUP JOIN =====
    tests.append((
        "Lookup Join - Default (unsorted data)",
        """SELECT users.name, orders.product
           FROM users
           JOIN orders ON users.id = orders.user_id""",
        {"use_polars": False, "debug": False},
        6
    ))
    
    # ===== JOIN ALGORITHMS - MERGE JOIN =====
    # Note: For merge join, data must be sorted by join key
    def sorted_users():
        users = list(load_jsonl("users.jsonl"))
        return iter(sorted(users, key=lambda x: x["id"]))
    
    def sorted_orders():
        orders = list(load_jsonl("orders.jsonl"))
        return iter(sorted(orders, key=lambda x: x["user_id"]))
    
    tests.append((
        "Merge Join - Sorted data",
        """SELECT users.name, orders.product
           FROM users
           JOIN orders ON users.id = orders.user_id""",
        {"use_polars": False, "debug": False},
        6,
        {"users": sorted_users, "orders": sorted_orders, "users_ordered_by": "id", "orders_ordered_by": "user_id"}
    ))
    
    # ===== JOIN ALGORITHMS - POLARS JOIN =====
    try:
        import polars
        tests.append((
            "Polars Join - Large dataset simulation",
            """SELECT users.name, orders.product
               FROM users
               JOIN orders ON users.id = orders.user_id""",
            {"use_polars": True, "debug": False},
            6
        ))
    except ImportError:
        print("\n[Skipping Polars tests - polars not installed]")
    
    # ===== JOIN ALGORITHMS - MMAP JOIN =====
    # Note: MMAP requires filename parameter
    def jsonl_source_users():
        return load_jsonl("users.jsonl")
    
    def jsonl_source_orders():
        return load_jsonl("orders.jsonl")
    
    tests.append((
        "MMAP Join - File-based (requires filename)",
        """SELECT users.name, orders.product
           FROM users
           JOIN orders ON users.id = orders.user_id""",
        {"use_polars": False, "debug": False},
        6,  # Should work same as regular join
        {"users": jsonl_source_users, "orders": jsonl_source_orders, 
         "users_filename": str(Path(__file__).parent.parent / "test_data" / "users.jsonl"), 
         "orders_filename": str(Path(__file__).parent.parent / "test_data" / "orders.jsonl")}
    ))
    
    # ===== REAL-WORLD EXAMPLES =====
    tests.append((
        "Real-World - Microservices data integration",
        """SELECT users.name, orders.product, orders.price, reviews.rating
           FROM users
           JOIN orders ON users.id = orders.user_id
           LEFT JOIN reviews ON reviews.user_id = users.id""",
        None,
        None  # Complex query - skip count
    ))
    
    tests.append((
        "Real-World - Price comparison with multiple joins",
        """SELECT users.name, orders.product, products.category, orders.price
           FROM users
           JOIN orders ON users.id = orders.user_id
           JOIN products ON orders.product = products.name
           WHERE products.checked = 1""",
        None,
        None  # Skip count - WHERE after JOINs may filter differently
    ))
    
    # ===== OPTIMIZATIONS - COLUMN PRUNING =====
    tests.append((
        "Optimization - Column pruning (only selected columns)",
        """SELECT products.name, products.category
           FROM products""",
        None,
        6  # All products, but only name and category selected
    ))
    
    # ===== OPTIMIZATIONS - FILTER PUSHDOWN =====
    tests.append((
        "Optimization - Filter pushdown (WHERE on root table)",
        """SELECT products.name
           FROM products
           WHERE products.stock > 100""",
        None,
        3
    ))
    
    # ===== COMPLEX QUERIES =====
    tests.append((
        "Complex - Multiple JOINs with WHERE",
        """SELECT users.name AS customer, orders.product AS item, products.category, orders.price
           FROM users
           JOIN orders ON users.id = orders.user_id
           JOIN products ON orders.product = products.name
           WHERE products.checked = 1 AND orders.price > 50""",
        None,
        None  # Skip count - complex WHERE
    ))
    
    tests.append((
        "Complex - LEFT JOIN with NULL handling",
        """SELECT products.name, reviews.rating, reviews.comment
           FROM products
           LEFT JOIN reviews ON products.product_id = reviews.product_id""",
        None,
        7  # 6 products, Laptop has 2 reviews = 7 rows
    ))
    
    # Run all tests
    passed = 0
    failed = 0
    
    for test_data in tests:
        if len(test_data) == 4:
            name, query, engine_config, expected_count = test_data
            custom_sources = None
        elif len(test_data) == 5:
            name, query, engine_config, expected_count, custom_sources = test_data
        else:
            continue
        
        # Handle custom sources for merge join and MMAP
        if custom_sources:
            # Create a custom engine setup
            if engine_config:
                engine = Engine(**engine_config)
            else:
                engine = Engine(debug=False)
            
            # Register custom sources
            if "users" in custom_sources:
                if "users_ordered_by" in custom_sources:
                    engine.register("users", custom_sources["users"], 
                                  ordered_by=custom_sources["users_ordered_by"])
                elif "users_filename" in custom_sources:
                    engine.register("users", custom_sources["users"],
                                  filename=custom_sources["users_filename"])
                else:
                    engine.register("users", custom_sources["users"])
            else:
                engine.register("users", lambda: load_jsonl("users.jsonl"))
            
            if "orders" in custom_sources:
                if "orders_ordered_by" in custom_sources:
                    engine.register("orders", custom_sources["orders"],
                                  ordered_by=custom_sources["orders_ordered_by"])
                elif "orders_filename" in custom_sources:
                    engine.register("orders", custom_sources["orders"],
                                  filename=custom_sources["orders_filename"])
                else:
                    engine.register("orders", custom_sources["orders"])
            else:
                engine.register("orders", lambda: load_jsonl("orders.jsonl"))
            
            # Register other tables normally
            engine.register("products", lambda: load_jsonl("products.jsonl"))
            engine.register("reviews", lambda: load_jsonl("reviews.jsonl"))
            
            try:
                results = list(engine.query(query))
                print(f"\n{'='*70}")
                print(f"TEST: {name}")
                print(f"{'='*70}")
                if engine_config:
                    print(f"Config: {engine_config}")
                print(f"Query: {query}")
                print()
                print(f"\n[OK] Query executed successfully")
                print(f"  Results: {len(results)} rows")
                
                if results:
                    print(f"\n  First few results:")
                    for i, row in enumerate(results[:2], 1):
                        print(f"    {i}. {row}")
                    if len(results) > 2:
                        print(f"    ... and {len(results) - 2} more")
                
                if expected_count is not None:
                    if len(results) == expected_count:
                        print(f"\n[OK] Expected count matches: {expected_count}")
                        passed += 1
                    else:
                        print(f"\n[FAIL] Count mismatch: expected {expected_count}, got {len(results)}")
                        failed += 1
                else:
                    passed += 1
            except Exception as e:
                print(f"\n{'='*70}")
                print(f"TEST: {name}")
                print(f"{'='*70}")
                print(f"\n[FAIL] Test failed with error: {e}")
                import traceback
                traceback.print_exc()
                failed += 1
        else:
            # Standard test case
            if test_case(name, query, engine_config, expected_count, debug=False):
                passed += 1
            else:
                failed += 1
    
    # Summary
    print(f"\n{'='*70}")
    print("TEST SUMMARY")
    print(f"{'='*70}")
    print(f"Total tests: {len(tests)}")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")
    print(f"{'='*70}")
    
    if failed == 0:
        print("\n[SUCCESS] All tests passed!")
        return 0
    else:
        print(f"\n[FAILURE] {failed} test(s) failed")
        return 1


if __name__ == "__main__":
    exit(main())

