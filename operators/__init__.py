"""
Execution operators for the streaming SQL engine.

This package contains different operator implementations:
- base: Basic Python-based operators
- polars: Polars-optimized operators (SIMD-accelerated)
- mmap: Memory-mapped operators (low memory)
"""

from .base import (
    ScanIterator,
    FilterIterator,
    ProjectIterator,
    LookupJoinIterator,
    MergeJoinIterator
)

__all__ = [
    "ScanIterator",
    "FilterIterator",
    "ProjectIterator",
    "LookupJoinIterator",
    "MergeJoinIterator",
]

