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
    elif fmt == 'ndjson':
        from .converters import ndjson_to_toon
        return ndjson_to_toon(data_input)
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



def convert(data_input: Any) -> str:
    """
    Auto-detects format and converts data to TOON.
    
    Args:
        data_input: Input data (string, bytes, or file-like).
        
    Returns:
        str: TOON formatted string.
    """
    from .sniffer import detect_format
    detected_format = detect_format(data_input)
    return convert_to_toon(data_input, detected_format)

def help():
    """
    Prints a list of all available conversion functions in the any2toon library.
    """
    msg = """
any2toon Conversion Functions:
-----------------------------
1. Use any2toon.convert(data) to auto-detect the format and convert it to TOON.
 Supported formats are: 'json', 'yaml', 'xml', 'csv', 'avro', 'parquet', 'bson'.

2. If for some reason you need to explicitly specify the format, you can use the format-Specific Converters:
   - json_to_toon(data)
   - yaml_to_toon(data)
   - xml_to_toon(data)
   - csv_to_toon(data)     [Optimized with Polars/Pandas for large files]
   - parquet_to_toon(data) [Optimized with Polars/Pandas for large files]
   - avro_to_toon(data)    [Optimized with Polars for large lists]
   - bson_to_toon(data)    [Optimized with Polars for large lists]
   - ndjson_to_toon(data)  [Optimized with Polars for large lists]
   
For more details, see the documentation or inspect docstrings.
    """
    print(msg.strip())
