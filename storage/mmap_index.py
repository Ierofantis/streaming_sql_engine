"""
Memory-mapped file index for efficient, low-memory lookups.

Instead of storing full row dictionaries in memory, stores file positions
and reads rows on-demand from disk using memory-mapped files.

Memory reduction: 90-99% (only positions stored, not full objects)
Can use Polars for faster index building (SIMD-accelerated scanning).
"""

import mmap
import json
from typing import Dict, List, Set, Optional, Any
from collections import defaultdict
from pathlib import Path

# Try importing Polars for faster index building
try:
    import polars as pl
    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False


class MmapPositionIndex:
    """
    Position-based index using memory-mapped files.
    
    Stores file positions instead of full objects, reducing memory by 90-99%.
    Reads rows on-demand from disk when needed.
    """
    
    def __init__(
        self,
        filename: str,
        key_column: str,
        debug: bool = False,
        use_polars: bool = True
    ):
        """
        Args:
            filename: Path to JSONL file
            key_column: Column name to index on (e.g., 'product_id')
            debug: Enable debug output
            use_polars: If True, use Polars for faster index building (SIMD-accelerated)
        """
        self.filename = filename
        self.key_column = key_column
        self.debug = debug
        self.use_polars = use_polars and POLARS_AVAILABLE
        
        # Index: {key_value: [file_position1, file_position2, ...]}
        # Positions are integers (8 bytes each), not full objects
        self.position_index: Dict[Any, List[int]] = defaultdict(list)
        
        # Build index by scanning file and recording positions
        if self.use_polars:
            self._build_index_polars()
        else:
            self._build_index()
    
    def _build_index_polars(self):
        """Build position index using Polars for faster grouping (SIMD-accelerated)."""
        if self.debug:
            print(f"      Building mmap position index with Polars (fast grouping) for {self.filename}...")
        
        try:
            # Scan file to collect positions and key values
            # Then use Polars for fast grouping
            with open(self.filename, 'rb') as f:
                mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
                current_pos = 0
                row_count = 0
                
                # Collect in batches for Polars processing
                batch_size = 500000
                batch_data = []  # List of (key_value, position) tuples
                
                while True:
                    line_start = current_pos
                    line_end = mm.find(b'\n', current_pos)
                    
                    if line_end == -1:
                        # Last line
                        if current_pos < len(mm):
                            line = mm[current_pos:].decode('utf-8', errors='ignore')
                            if line.strip():
                                try:
                                    obj = json.loads(line)
                                    key_value = obj.get(self.key_column)
                                    if key_value is not None:
                                        batch_data.append((key_value, line_start))
                                        row_count += 1
                                except (ValueError, KeyError):
                                    pass
                        break
                    
                    line = mm[line_start:line_end].decode('utf-8', errors='ignore')
                    
                    try:
                        obj = json.loads(line)
                        key_value = obj.get(self.key_column)
                        if key_value is not None:
                            batch_data.append((key_value, line_start))
                            row_count += 1
                    except (ValueError, KeyError):
                        pass
                    
                    current_pos = line_end + 1
                    
                    # Process batch with Polars when it reaches batch_size
                    if len(batch_data) >= batch_size:
                        self._process_batch_polars(batch_data)
                        batch_data = []
                    
                    if self.debug and row_count % 1000000 == 0:
                        print(f"      Indexed {row_count:,} rows...")
                
                # Process remaining batch
                if batch_data:
                    self._process_batch_polars(batch_data)
                
                mm.close()
            
            if self.debug:
                unique_keys = len(self.position_index)
                total_rows = sum(len(positions) for positions in self.position_index.values())
                print(f"      Mmap index built (Polars): {total_rows:,} rows, {unique_keys:,} unique keys")
        
        except Exception as e:
            if self.debug:
                print(f"      Polars index building failed: {e}, falling back to standard")
            # Fallback to standard method
            self._build_index()
    
    def _process_batch_polars(self, batch_data: List[tuple]):
        """Process a batch using Polars for fast grouping (SIMD-accelerated)."""
        try:
            # Convert to Polars DataFrame for fast grouping
            # batch_data is list of (key_value, position) tuples
            df = pl.DataFrame({
                'key': [d[0] for d in batch_data],
                'position': [d[1] for d in batch_data]
            })
            
            # Group by key (SIMD-accelerated)
            grouped = df.group_by('key', maintain_order=False)
            
            # Add positions to index
            for group_key, group_df in grouped:
                if group_key is not None:
                    # Polars group_by returns tuple (key,) for single column, extract the key
                    if isinstance(group_key, tuple) and len(group_key) == 1:
                        key_value = group_key[0]
                    else:
                        key_value = group_key
                    positions = group_df['position'].to_list()
                    self.position_index[key_value].extend(positions)
        except Exception as e:
            # If Polars fails, fall back to standard processing
            for key_value, position in batch_data:
                if key_value is not None:
                    self.position_index[key_value].append(position)
    
    def _build_index(self):
        """Build position index by scanning file (standard method)."""
        if self.debug:
            print(f"      Building mmap position index for {self.filename}...")
        
        try:
            with open(self.filename, 'rb') as f:
                mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
                current_pos = 0
                row_count = 0
                
                while True:
                    line_start = current_pos
                    line_end = mm.find(b'\n', current_pos)
                    
                    if line_end == -1:
                        # Last line (no newline)
                        if current_pos < len(mm):
                            line = mm[current_pos:].decode('utf-8', errors='ignore')
                            if line.strip():
                                try:
                                    obj = json.loads(line)
                                    key_value = obj.get(self.key_column)
                                    if key_value is not None:
                                        self.position_index[key_value].append(line_start)
                                        row_count += 1
                                except (ValueError, KeyError):
                                    pass
                        break
                    
                    # Read line
                    line = mm[line_start:line_end].decode('utf-8', errors='ignore')
                    
                    try:
                        obj = json.loads(line)
                        key_value = obj.get(self.key_column)
                        if key_value is not None:
                            self.position_index[key_value].append(line_start)
                            row_count += 1
                    except (ValueError, KeyError):
                        pass
                    
                    current_pos = line_end + 1
                    
                    if self.debug and row_count % 100000 == 0:
                        print(f"      Indexed {row_count:,} rows...")
                
                mm.close()
            
            if self.debug:
                unique_keys = len(self.position_index)
                print(f"      Mmap index built: {row_count:,} rows, {unique_keys:,} unique keys")
        
        except Exception as e:
            if self.debug:
                print(f"      Error building mmap index: {e}")
            self.position_index = {}
    
    def get_positions(self, key_value: Any) -> List[int]:
        """
        Get file positions for a key value.
        
        Args:
            key_value: Join key value
            
        Returns:
            List of file positions (integers)
        """
        return self.position_index.get(key_value, [])
    
    def get_rows(self, key_value: Any, required_columns: Optional[Set[str]] = None) -> List[Dict]:
        """
        Get rows for a key value by reading from disk.
        
        Args:
            key_value: Join key value
            required_columns: Optional set of columns to include (column pruning)
            
        Returns:
            List of row dictionaries
        """
        positions = self.get_positions(key_value)
        if not positions:
            return []
        
        rows = []
        
        try:
            with open(self.filename, 'rb') as f:
                mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
                
                for pos in positions:
                    mm.seek(pos)
                    line_end = mm.find(b'\n', pos)
                    
                    if line_end == -1:
                        line = mm[pos:].decode('utf-8', errors='ignore')
                    else:
                        line = mm[pos:line_end].decode('utf-8', errors='ignore')
                    
                    try:
                        row = json.loads(line)
                        
                        # Column pruning
                        if required_columns:
                            row = {k: v for k, v in row.items() if k in required_columns}
                        
                        rows.append(row)
                    except (ValueError, KeyError):
                        continue
                
                mm.close()
        
        except Exception as e:
            if self.debug:
                print(f"      Error reading rows from mmap: {e}")
        
        return rows
    
    def get_rows_batch_optimized(
        self,
        key_values: List[Any],
        required_columns: Optional[Set[str]] = None
    ) -> Dict[Any, List[Dict]]:
        """
        Get rows for multiple key values efficiently (sorted by position).
        
        This is more efficient than calling get_rows() multiple times
        because it sorts positions and reads sequentially.
        
        Args:
            key_values: List of join key values
            required_columns: Optional set of columns to include
            
        Returns:
            Dictionary mapping key_value to list of rows
        """
        if not key_values:
            return {}
        
        # Collect all positions with their key values
        all_positions = []
        key_to_positions = {}
        
        for key_value in key_values:
            positions = self.get_positions(key_value)
            if positions:
                key_to_positions[key_value] = positions
                all_positions.extend([(key_value, pos) for pos in positions])
        
        # Sort by position for sequential disk reads
        all_positions.sort(key=lambda x: x[1])
        
        # Read rows in position order
        results = defaultdict(list)
        
        try:
            with open(self.filename, 'rb') as f:
                mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
                
                for key_value, pos in all_positions:
                    mm.seek(pos)
                    line_end = mm.find(b'\n', pos)
                    
                    if line_end == -1:
                        line = mm[pos:].decode('utf-8', errors='ignore')
                    else:
                        line = mm[pos:line_end].decode('utf-8', errors='ignore')
                    
                    try:
                        row = json.loads(line)
                        
                        # Column pruning
                        if required_columns:
                            row = {k: v for k, v in row.items() if k in required_columns}
                        
                        results[key_value].append(row)
                    except (ValueError, KeyError):
                        continue
                
                mm.close()
        
        except Exception as e:
            if self.debug:
                print(f"      Error reading batch from mmap: {e}")
        
        return dict(results)
    
    def __len__(self):
        """Return number of unique keys in index."""
        return len(self.position_index)
    
    def get_total_rows(self) -> int:
        """Return total number of rows indexed."""
        return sum(len(positions) for positions in self.position_index.values())


def create_mmap_index_from_source(
    source_fn,
    key_column: str,
    filename: Optional[str] = None,
    debug: bool = False
) -> Optional[MmapPositionIndex]:
    """
    Create mmap index from a source function.
    
    If source is a file-based generator, we can use the file directly.
    Otherwise, we'd need to write to a temp file first.
    
    Args:
        source_fn: Source function that returns iterator of rows
        key_column: Column name to index on
        filename: Optional filename if source is file-based
        debug: Enable debug output
        
    Returns:
        MmapPositionIndex if file-based, None otherwise
    """
    # Check if source is file-based
    # This is a heuristic - in practice, you'd pass filename explicitly
    if filename and Path(filename).exists():
        return MmapPositionIndex(filename, key_column, debug=debug)
    
    # For non-file sources, we'd need to write to temp file first
    # For now, return None (fallback to in-memory index)
    return None

