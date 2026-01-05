"""
Storage and indexing utilities.

This package contains:
- mmap_index: Memory-mapped file indexing for efficient lookups
"""

from .mmap_index import (
    MmapPositionIndex,
    create_mmap_index_from_source
)

__all__ = [
    "MmapPositionIndex",
    "create_mmap_index_from_source",
]

