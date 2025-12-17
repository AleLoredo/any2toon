import csv
import io
import json
from typing import Union, Optional
from .exceptions import Any2ToonError

def detect_format(data: Union[str, bytes]) -> str:
    """
    Analyzes input data to determine its format.
    
    Args:
        data: input data (str or bytes).
        
    Returns:
        str: Detected format ('json', 'yaml', 'xml', 'csv', 'parquet', 'avro', 'bson').
        
    Raises:
        Any2ToonError: If format cannot be detected.
    """
    
    # 1. Handle Binary Data
    if isinstance(data, bytes) or isinstance(data, io.BytesIO):
        if isinstance(data, io.BytesIO):
            pos = data.tell()
            data.seek(0)
            head = data.read(10)
            data.seek(pos) # Reset
        else:
            head = data[:10]
            
        # Parquet Magic Check
        if head.startswith(b'PAR1'):
            return 'parquet'
            
        # Avro Magic Check (Obj\x01)
        if head.startswith(b'Obj\x01'):
            return 'avro'
            
        # BSON Check
        # BSON doc starts with 4-byte int32 length, incl. itself.
        # It ends with \x00. 
        # Hard to be 100% sure without decoding, but we can try to decode first doc length if generic binary.
        # Default binary fallback -> BSON? Or error?
        # Let's try to assume BSON if generic binary, or maybe check for Text characters if it's actually just bytes-encoded text.
        
        # Check if it looks like UTF-8 text
        try:
            if isinstance(data, io.BytesIO):
                # We can't easily check whole stream without reading.
                # Assume binary if not PAR/AVRO -> BSON?
                # Actually, BSON is quite specific.
                # If we really want to support 'convert(file_obj)', probing is harder.
                # Let's rely on decoding a chunk.
                chunk = head
                decoded = chunk.decode('utf-8')
                # If it decodes, is it "clean" text?
                # BSON often has null bytes \x00. Text usually doesn't (except maybe legit uses but rare in JSON/CSV/XML start).
                if '\x00' in decoded:
                    return 'bson'
                # If no nulls, likely text -> continue to text logic below
                # We can't update 'data' reference easily for BytesIO without reading all.
                # For BytesIO, we read all to text if small? Or fail detection if stream?
                # Let's assume for BytesIO we just return 'bson' if it contains nulls in header.
                pass 
            else:
                decoded = data.decode('utf-8')
                # If we decoded entire data successfully, checks for nulls.
                # BSON binary WILL have nulls.
                if '\x00' in decoded:
                   return 'bson'
                data = decoded
        except UnicodeDecodeError:
            # If valid binary and not known magic, assume BSON
            return 'bson'

    # 2. Handle Text Data
    if isinstance(data, str) or (isinstance(data, io.BytesIO) and not isinstance(data, str)):
        # For BytesIO, we need to read it if we want to run text logic on it?
        # But convert() calls convert_to_toon(data, fmt). convert_to_toon expects BytesIO for some, String for others.
        # If we detect TEXT format (JSON/XML), convert_to_toon expects String or Bytes?
        # json_to_toon handles str.
        # If we have BytesIO and detect JSON, we should probably read it to string?
        # Or let json_to_toon handle BytesIO? (It currently doesn't documented to handle BytesIO, only str/dict).
        # Let's assume if we detected 'json' from BytesIO header, expectations handles it.
        
        # We need a 'sample' for text heuristics if data is BytesIO.
        if isinstance(data, io.BytesIO):
            # We already read 'head'.
            # Decode head for heuristics.
            try:
                sample = head.decode('utf-8')
            except:
                return 'bson'
        else:
            sample = data
            
        stripped = sample.strip()
        if not stripped:
            raise Any2ToonError("Empty input data")
            
        # JSON heuristics
        if (stripped.startswith('{') and stripped.endswith('}')) or \
           (stripped.startswith('[') and stripped.endswith(']')):
           
           # Distinguish JSON vs NDJSON
           # Heuristic: If it starts with '{', checks if multiple lines exist and first line is valid JSON.
           if stripped.startswith('{'):
                if '\n' in stripped:
                    lines = stripped.splitlines()
                    # Skip empty lines
                    non_empty_lines = [l.strip() for l in lines if l.strip()]
                    if len(non_empty_lines) > 1:
                        # Check first line
                        first = non_empty_lines[0]
                        try:
                            # If first line is a valid complete JSON object
                            json.loads(first)
                            
                            # To be safe, check second line too (avoid false positive on formatted JSON like {"a":\n1})
                            second = non_empty_lines[1]
                            try:
                                json.loads(second)
                                return 'ndjson'
                            except:
                                # Second line invalid -> likely standard JSON split across lines
                                return 'json'
                        except:
                            # First line invalid -> likely standard JSON
                            return 'json'
                            
           return 'json'
           
        # XML heuristics
        if stripped.startswith('<') and stripped.endswith('>'):
            return 'xml'
            
        # CSV heuristics
        # Use csv.Sniffer
        try:
            # Sniffer needs a sample. 
            sample = stripped[:1024]
            # Use strict delimiter list to avoid false positives (like assuming newline is a delimiter for 1-col csv)
            dialect = csv.Sniffer().sniff(sample, delimiters=',;\t|')
            # If it passes sniffing, it's likely CSV.
            # But plain text can also pass. 
            # Check for header row or multiple lines?
            if '\n' in sample:
                return 'csv'
        except csv.Error:
            pass # Not CSV
            
        # YAML fallback
        # YAML is very permissive (plain string is valid YAML).
        return 'yaml'
        
    # If we got here with BytesIO that decoded to text, it jumps to text block above?
    # No, we converted var `data` to str inside the block but didn't recurse.
    # We need to restructure slightly to handle re-check if bytes->text.
    
    raise Any2ToonError("Unknown data type")
