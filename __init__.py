"""
Streaming SQL Join Engine

A lightweight SQL execution engine that processes queries row-by-row
without loading full tables into memory.
"""

from .engine import Engine

# Protocol helpers for easy source creation
try:
    from .protocol_helpers import (
        add_protocol_support,
        wrap_simple_source,
        create_protocol_source,
        register_file_source,
        register_api_source,
    )
    __all__ = [
        "Engine",
        "add_protocol_support",
        "wrap_simple_source",
        "create_protocol_source",
        "register_file_source",
        "register_api_source",
    ]
except ImportError:
    __all__ = ["Engine"]

__version__ = "0.1.27"

