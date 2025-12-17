import json
import yaml
import xmltodict
import csv
import io
import warnings
from typing import Union, Dict, List, Any
from .exceptions import ConversionError
from .toon_serializer import dumps as toon_dumps
from . import config

# Optional Optimization Imports (Polars/Pandas)
try:
    import polars as pl
    _HAS_POLARS = True
except ImportError:
    _HAS_POLARS = False

try:
    import pandas as pd
    _HAS_PANDAS = True
except ImportError:
    _HAS_PANDAS = False

def _ensure_dependency(module_name: str, extra_name: str):
    """Helper to check if a module is imported, raise error if not."""
    try:
        __import__(module_name)
    except ImportError:
        raise ImportError(
            f"Missing dependency '{module_name}'. "
            f"Install it via 'pip install any2toon[{extra_name}]' to support this format."
        )

def _polars_csv_to_toon(csv_string: str) -> str:
    """Optimized CSV conversion using Polars."""
    # Assuming caller checked _HAS_POLARS
    df = pl.read_csv(io.StringIO(csv_string))
    return _polars_df_to_toon(df)

def _pandas_csv_to_toon(csv_string: str) -> str:
    """Optimized CSV conversion using Pandas."""
    df = pd.read_csv(io.StringIO(csv_string))
    if len(df) == 0:
        return "root[0]{}:"
    
    cols = list(df.columns)
    cols_str = ",".join(cols)
    count = len(df)
    header = f"root[{count}]{{{cols_str}}}:"
    
    # Vectorized string creation: join all columns with comma
    # Ensure all are strings
    df_str = df.astype(str)
    
    # Use pandas flexible string concatenation or verify performant method
    # For simplicity and correctness with small-medium data in pure pandas:
    # We can join row by row or use apply which is slow-ish.
    # Faster: 
    #   result = df[cols[0]] + "," + df[cols[1]] ...
    
    if len(cols) > 0:
        series_res = df_str[cols[0]]
        for col in cols[1:]:
             series_res = series_res + "," + df_str[col]
        
        # Indent values with 1 space
        body = " " + series_res 
        return header + "\n" + "\n".join(body)
    else:
        return header

def _polars_parquet_to_toon(parquet_bytes: Union[bytes, io.BytesIO]) -> str:
    """Optimized Parquet conversion using Polars."""
    if isinstance(parquet_bytes, bytes):
        f = io.BytesIO(parquet_bytes)
    else:
        f = parquet_bytes
    df = pl.read_parquet(f)
    return _polars_df_to_toon(df)

def _polars_df_to_toon(df: 'pl.DataFrame') -> str: # type: ignore
    """Vectorized conversion of DataFrame to TOON string."""
    if df.height == 0:
        return "root[0]{}:"
        
    cols = df.columns
    cols_str = ",".join(cols)
    count = df.height
    header = f"root[{count}]{{{cols_str}}}:"
    
    # Construct expressions: concat_str([col1, col2...], separator=",")
    # Cast all to string first using strict cast or not? Using cast(pl.Utf8)
    exprs = [pl.col(c).cast(pl.Utf8) for c in cols]
    
    # We need to prepend space? 
    # " " + val1 + "," + val2
    # pl.concat_str with separator="," gives "val1,val2"
    # then we prepend " "
    
    final_output = df.select(
        (pl.lit(" ") + pl.concat_str(exprs, separator=",")).alias("res")
    ).select(
        pl.col("res").str.join("\n")
    ).item()
    
    return header + "\n" + final_output

def _warn_optimization_missing(feature: str):
    """Issue warning if enabled."""
    if config.warnings_enabled():
        warnings.warn(
            f"Optimized engines (Polars/Pandas) not found/usable. Using slower {feature} conversion path. "
            "Install 'polars' (recommended) or 'pandas' for improved performance on large datasets.",
            UserWarning
        )

def _optimize_list_conversion(data: List[Dict]) -> str:
    """
    Attempts to use Polars for converting large lists of dicts (>500 items).
    Benchmark shows Polars > Base > Pandas for this specific case.
    """
    if len(data) >= 500 and _HAS_POLARS:
        try:
            # pl.from_dicts is very fast
            df = pl.from_dicts(data)
            return _polars_df_to_toon(df)
        except Exception:
            # Fallback if structure is irregular (nested dicts inside might fail strict schema)
             return toon_dumps(data)
    return toon_dumps(data)

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
            
        if isinstance(parsed_data, list) and len(parsed_data) > 0 and isinstance(parsed_data[0], dict):
             return _optimize_list_conversion(parsed_data)
        return toon_dumps(parsed_data)
    except json.JSONDecodeError as e:
        raise ConversionError(f"Invalid JSON: {e}")
    except Exception as e:
        raise ConversionError(f"JSON conversion failed: {e}")
    except Exception as e:
        raise ConversionError(f"JSON conversion failed: {e}")

def ndjson_to_toon(data: Union[str, bytes]) -> str:
    """
    Converts NDJSON (Newline Delimited JSON) to TOON.
    
    Args:
        data: NDJSON string or bytes.
              
    Returns:
        str: TOON formatted string.
    """
    try:
        if isinstance(data, bytes):
            data_str = data.decode('utf-8')
        else:
            data_str = data
            
        parsed_data = []
        for line in data_str.strip().splitlines():
            line = line.strip()
            if not line:
                continue
            parsed_data.append(json.loads(line))
            
        return _optimize_list_conversion(parsed_data)
    except json.JSONDecodeError as e:
        raise ConversionError(f"Invalid NDJSON: {e}")
    except Exception as e:
        raise ConversionError(f"NDJSON conversion failed: {e}")

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
            
        if isinstance(parsed_data, list) and len(parsed_data) > 0 and isinstance(parsed_data[0], dict):
             return _optimize_list_conversion(parsed_data)
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
    Requires 'any2toon[avro]'.
    """
    try:
        _ensure_dependency("fastavro", "avro")
        import fastavro
        
        if isinstance(data, bytes):
            f = io.BytesIO(data)
        else:
            f = data
            
        # fastavro.reader reads OCF files which contain the schema
        reader = fastavro.reader(f)
        parsed_data = list(reader)
        
        if len(parsed_data) >= 500:
             return _optimize_list_conversion(parsed_data)
        return toon_dumps(parsed_data)
    except ImportError as e:
        raise e
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
        return "root[0]{}:"
    
    cols = list(df.columns)
    cols_str = ",".join(cols)
    count = len(df)
    header = f"root[{count}]{{{cols_str}}}:"
    
    df_str = df.astype(str)
    if len(cols) > 0:
        series_res = df_str[cols[0]]
        for col in cols[1:]:
             series_res = series_res + "," + df_str[col]
        body = " " + series_res
        return header + "\n" + "\n".join(body)
    else:
        return header

def parquet_to_toon(data: Union[bytes, io.BytesIO]) -> str:
    """
    Converts Parquet bytes/file to TOON.
    Requires 'pyarrow'.
    Priority: Polars > Pandas > Base (using pyarrow)
    """
    _ensure_dependency('pyarrow', 'parquet')
    import pyarrow.parquet as pq
    
    try:
        if isinstance(data, bytes):
            f = io.BytesIO(data)
        else:
            f = data
        
        # Check size quickly
        metadata = pq.read_metadata(f)
        row_count = metadata.num_rows
        
        if row_count < 100:
            f.seek(0)
            table = pq.read_table(f)
            parsed_data = table.to_pylist()
            return toon_dumps(parsed_data)
        else:
            if _HAS_POLARS:
                 return _polars_parquet_to_toon(data)
            elif _HAS_PANDAS:
                return _pandas_parquet_to_toon(data)
            else:
                _warn_optimization_missing("Parquet")
                f.seek(0)
                table = pq.read_table(f)
                parsed_data = table.to_pylist()
                return toon_dumps(parsed_data)
    except Exception as e:
        raise ConversionError(f"Invalid Parquet: {e}")

def bson_to_toon(data: Union[bytes, io.BytesIO]) -> str:
    """
    Converts BSON data to TOON.
    """
    _ensure_dependency('bson', 'bson')
    import bson # pymongo
    try:
        if isinstance(data, io.BytesIO):
            bson_bytes = data.read()
        else:
            bson_bytes = data
            
        parsed_data = bson.decode_all(bson_bytes)
        if isinstance(parsed_data, list) and len(parsed_data) > 0:
             return _optimize_list_conversion(parsed_data)
        return toon_dumps(parsed_data)
    except Exception as e:
        raise ConversionError(f"Invalid BSON: {e}")
