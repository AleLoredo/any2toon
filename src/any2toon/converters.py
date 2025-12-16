import json
import yaml
import xmltodict
import csv
import io
import fastavro
import pyarrow.parquet as pq
import pyarrow as pa
from typing import Union, Dict, List, Any
from .exceptions import ConversionError
from .toon_serializer import dumps as toon_dumps

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
        if isinstance(data, bytes):
            f = io.BytesIO(data)
        else:
            f = data
            
        table = pq.read_table(f)
        parsed_data = table.to_pylist()
        return toon_dumps(parsed_data)
    except Exception as e:
        raise ConversionError(f"Invalid Parquet: {e}")
