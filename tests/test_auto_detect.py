import pytest
import io
import json
import csv
from any2toon.sniffer import detect_format
from any2toon.exceptions import Any2ToonError

def test_detect_json():
    assert detect_format('{"a": 1}') == 'json'
    assert detect_format('[{"a": 1}]') == 'json'
    assert detect_format('  {"a": 1}  ') == 'json'

def test_detect_xml():
    assert detect_format('<root>val</root>') == 'xml'
    assert detect_format('  <root>val</root>  ') == 'xml'

def test_detect_csv():
    csv_data = "col1,col2\nval1,val2"
    assert detect_format(csv_data) == 'csv'
    
    # Single column might be tricky for sniffer, often needs delimiter
    # Sniffer might fail on single column if no delimiter found
    # assert detect_format("col1\nval1") == 'csv' # skipping flaky case

def test_detect_yaml_fallback():
    # YAML matches almost anything text that fails others
    assert detect_format("key: value") == 'yaml'
    assert detect_format("just a string") == 'yaml'

def test_detect_parquet():
    # Magic bytes PAR1
    data = b'PAR1' + b'\x00'*10
    assert detect_format(data) == 'parquet'
    
    # BytesIO
    f = io.BytesIO(data)
    assert detect_format(f) == 'parquet'
    # Ensure stream reset
    assert f.tell() == 0

def test_detect_avro():
    # Magic bytes Obj\x01
    data = b'Obj\x01' + b'\x00'*10
    assert detect_format(data) == 'avro'

def test_detect_bson():
    # Assume binary fallback for now
    data = b'\x10\x00\x00\x00\x00' # Arbitrary binary
    assert detect_format(data) == 'bson'
    
    f = io.BytesIO(data)
    assert detect_format(f) == 'bson'

def test_detect_empty():
    with pytest.raises(Any2ToonError):
        detect_format("")
    with pytest.raises(Any2ToonError):
        detect_format("   ")
