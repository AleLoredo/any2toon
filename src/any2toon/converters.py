import json
import yaml
import xmltodict
import csv
import io
import fastavro
import pyarrow.parquet as pq
import pyarrow as pa
import warnings
from typing import Union, Dict, List, Any
from .exceptions import ConversionError
from .toon_serializer import dumps as toon_dumps
from . import config

# Optional Polars import
try:
    import polars as pl
    _HAS_POLARS = True
except ImportError:
    import polars as pl # For typing/mocking if needed
    _HAS_POLARS = False

# Optional Pandas import
try:
    import pandas as pd
    _HAS_PANDAS = True
except ImportError:
    import pandas as pd # For typing/mocking
    _HAS_PANDAS = False

def _polars_csv_to_toon(csv_string: str) -> str:
    """Optimized CSV conversion using Polars."""
    df = pl.read_csv(io.StringIO(csv_string))
    return _polars_df_to_toon(df)

def _pandas_csv_to_toon(csv_string: str) -> str:
    """Optimized CSV conversion using Pandas."""
    df = pd.read_csv(io.StringIO(csv_string))
    if len(df) == 0:
        return ""
        
    cols = df.columns
    # Vectorized string creation
    result_series = "- " + cols[0] + ": " + df[cols[0]].astype(str)
    
    for col in cols[1:]:
        result_series = result_series + "\n  " + col + ": " + df[col].astype(str)
        
    return "\n".join(result_series)

def _polars_parquet_to_toon(parquet_bytes: Union[bytes, io.BytesIO]) -> str:
    """Optimized Parquet conversion using Polars."""
    if isinstance(parquet_bytes, bytes):
        f = io.BytesIO(parquet_bytes)
    else:
        f = parquet_bytes
    df = pl.read_parquet(f)
    return _polars_df_to_toon(df)

def _polars_df_to_toon(df: 'pl.DataFrame') -> str:
    """Vectorized conversion of DataFrame to TOON string."""
    if df.height == 0:
        return ""
        
    first_col = df.columns[0]
    rest_cols = df.columns[1:]
    
    # Construct expressions
    first_expr = pl.lit(f"- {first_col}: ") + df[first_col].cast(pl.Utf8)
    rest_exprs = [pl.lit(f"  {col}: ") + df[col].cast(pl.Utf8) for col in rest_cols]
    
    all_exprs = [first_expr] + rest_exprs
    
    # Execute vectorized string concatenation
    final_output = df.select(
        pl.concat_str(all_exprs, separator="\n").str.join("\n")
    ).item()
    return final_output

def _warn_optimization_missing(feature: str):
    """Issue warning if enabled."""
    if config.warnings_enabled():
        warnings.warn(
            f"Optimized engines (Polars/Pandas) not found/usable. Using slower {feature} conversion path. "
            "Install 'polars' (recommended) or 'pandas' for improved performance on large datasets.",
            UserWarning
        )

def json_to_toon(data: Union[str, Dict, List]) -> str:
    """
    Converts JSON data to TOON format.
    
    Args:
        data: JSON string or Python object (dict/list) from already parsed JSON.
              If string, it will be parsed.
              
    Returns:
        str: TOON formatted string.
        
    Raises:
        ConversionError: If JSON parsing fails.
    """
    try:
        if isinstance(data, str):
            parsed_data = json.loads(data)
        else:
            parsed_data = data
        return toon_dumps(parsed_data)
    except json.JSONDecodeError as e:
        raise ConversionError(f"Invalid JSON: {e}")
    except Exception as e:
        raise ConversionError(f"JSON conversion failed: {e}")

def yaml_to_toon(data: Union[str, Dict, List]) -> str:
    """
    Converts YAML data to TOON format.
    
    Args:
        data: YAML string or Python object.
              If string, it will be parsed.
              
    Returns:
        str: TOON formatted string.
        
    Raises:
        ConversionError: If YAML parsing fails.
    """
    try:
        if isinstance(data, str):
            parsed_data = yaml.safe_load(data)
        else:
            parsed_data = data
        return toon_dumps(parsed_data)
    except yaml.YAMLError as e:
        raise ConversionError(f"Invalid YAML: {e}")
    except Exception as e:
        raise ConversionError(f"YAML conversion failed: {e}")

def xml_to_toon(data: str) -> str:
    """
    Converts XML string to TOON format.
    
    Args:
        data: XML string.
              
    Returns:
        str: TOON formatted string.
        
    Raises:
        ConversionError: If XML parsing fails.
    """
    try:
        # parsed_data is usually an OrderedDict from xmltodict
        parsed_data = xmltodict.parse(data)
        return toon_dumps(parsed_data)
    except Exception as e: # xmltodict can raise generic expat errors
        raise ConversionError(f"Invalid XML: {e}")

def csv_to_toon(data: str) -> str:
    """
    Converts CSV string to TOON format.
    Assumes the first row is the header.
    
    Args:
        data: CSV string.
              
    Returns:
        str: TOON formatted string.
        
    Raises:
        ConversionError: If CSV parsing fails.
    """
    try:
        # Check row count heuristic (lines)
        line_count = data.count('\n') 
        
        if line_count >= 100:
            if _HAS_POLARS:
                return _polars_csv_to_toon(data)
            elif _HAS_PANDAS:
                return _pandas_csv_to_toon(data)
            else:
                _warn_optimization_missing("CSV")
        
        # Fallback / Normal Path
        f = io.StringIO(data)
        reader = csv.DictReader(f)
        parsed_data = list(reader)
        return toon_dumps(parsed_data)
    except Exception as e:
        raise ConversionError(f"Invalid CSV: {e}")

def avro_to_toon(data: Union[bytes, io.BytesIO]) -> str:
    """
    Converts Avro data (OCF format) to TOON.
    
    Args:
        data: Avro data as bytes or file-like object (BytesIO).
              
    Returns:
        str: TOON formatted string.
        
    Raises:
        ConversionError: If Avro parsing fails.
    """
    try:
        if isinstance(data, bytes):
            f = io.BytesIO(data)
        else:
            f = data
            
        # fastavro.reader reads OCF files which contain the schema
        reader = fastavro.reader(f)
        parsed_data = list(reader)
        return toon_dumps(parsed_data)
    except Exception as e:
        raise ConversionError(f"Invalid Avro: {e}")

def _pandas_parquet_to_toon(parquet_bytes: Union[bytes, io.BytesIO]) -> str:
    """Optimized Parquet conversion using Pandas."""
    if isinstance(parquet_bytes, bytes):
        f = io.BytesIO(parquet_bytes)
    else:
        f = parquet_bytes
    df = pd.read_parquet(f)
    if len(df) == 0:
        return ""
    
    cols = df.columns
    # Vectorized string creation
    result_series = "- " + cols[0] + ": " + df[cols[0]].astype(str)
    
    for col in cols[1:]:
        result_series = result_series + "\n  " + col + ": " + df[col].astype(str)
        
    return "\n".join(result_series)

def parquet_to_toon(data: Union[bytes, io.BytesIO]) -> str:
    """
    Converts Parquet data to TOON.
    
    Args:
        data: Parquet data as bytes or file-like object (BytesIO).
              
    Returns:
        str: TOON formatted string.
        
    Raises:
        ConversionError: If Parquet parsing fails.
    """
    try:
        # Prepare file-like object for metadata reading
        if isinstance(data, bytes):
            f = io.BytesIO(data)
        else:
            f = data
            
        # Check row count using metadata (fast, no data read)
        # Note: Depending on pyarrow version, read_metadata might assume seekable stream
        try:
            metadata = pq.read_metadata(f)
            row_count = metadata.num_rows
        except Exception:
            # If metadata read fails (e.g. stream issue), default to 0 and likely let standard path handle or fail
            row_count = 0
            
        # Reset stream position if we read from it (BytesIO)
        if hasattr(f, 'seek'):
            f.seek(0)
            
        if row_count >= 100:
            if _HAS_POLARS:
                 return _polars_parquet_to_toon(data)
            elif _HAS_PANDAS:
                 return _pandas_parquet_to_toon(data)
            else:
                _warn_optimization_missing("Parquet")
            
        # Fallback / Normal Path
        # Note: if we consumed 'data' as BytesIO, we need to pass the reset 'f' or original 'data' 
        # but 'data' arg is unmodified if it was bytes. 
        # If it was BytesIO passed in, we already reset it.
        
        table = pq.read_table(f)
        parsed_data = table.to_pylist()
        return toon_dumps(parsed_data)
    except Exception as e:
        raise ConversionError(f"Invalid Parquet: {e}")
