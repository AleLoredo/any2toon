from typing import Any
from .converters import json_to_toon, yaml_to_toon, xml_to_toon, csv_to_toon, avro_to_toon, parquet_to_toon, bson_to_toon
from .exceptions import InvalidFormatError

def convert_to_toon(data_input: Any, input_format: str) -> str:
    """
    Main conversion function to convert data from a specified format to TOON.
    
    Args:
        data_input: The input data (string or object depending on format).
        input_format: The format of the input data ('json', 'yaml', 'xml').
        
    Returns:
        str: The translated TOON string.
        
    Raises:
        InvalidFormatError: If the input format is not supported.
        ConversionError: If the conversion fails.
    """
    fmt = input_format.lower()
    
    if fmt == 'json':
        return json_to_toon(data_input)
    elif fmt == 'yaml':
        return yaml_to_toon(data_input)
    elif fmt == 'xml':
        return xml_to_toon(data_input)
    elif fmt == 'csv':
        return csv_to_toon(data_input)
    elif fmt == 'avro':
        return avro_to_toon(data_input)
    elif fmt == 'parquet':
        return parquet_to_toon(data_input)
    elif fmt == 'bson':
        return bson_to_toon(data_input)
    else:
        raise InvalidFormatError(f"Unsupported format: {input_format}. Supported formats: json, yaml, xml, csv, avro, parquet, bson")

def help():
    """
    Prints a list of all available conversion functions in the any2toon library.
    """
    msg = """
any2toon Conversion Functions:
-----------------------------
1. convert_to_toon(data, format): 
   Universal converter. Supported formats: 'json', 'yaml', 'xml', 'csv', 'avro', 'parquet', 'bson'.

2. Format-Specific Converters:
   - json_to_toon(data)
   - yaml_to_toon(data)
   - xml_to_toon(data)
   - csv_to_toon(data)     [Optimized with Polars/Pandas for large files]
   - parquet_to_toon(data) [Optimized with Polars/Pandas for large files]
   - avro_to_toon(data)    [Optimized with Polars for large lists]
   - bson_to_toon(data)    [Optimized with Polars for large lists]
   
For more details, see the documentation or inspect docstrings.
    """
    print(msg.strip())
