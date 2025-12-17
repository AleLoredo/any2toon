import pytest
from any2toon.sniffer import detect_format
from any2toon.converters import ndjson_to_toon, json_to_toon

def test_detect_ndjson_simple():
    data = '{"a": 1}\n{"b": 2}'
    assert detect_format(data) == 'ndjson'

def test_detect_ndjson_vs_json_multiline():
    # Valid JSON, just pretty printed
    data = '{\n  "a": 1\n}'
    assert detect_format(data) == 'json'

def test_detect_ndjson_single_line():
    # Technically NDJSON is defined as line-delimited. 
    # A single line '{"a":1}' matches both.
    # Our sniffer prefers 'json' (defaults to json if only 1 line or no newlines)
    # This is acceptable because json_to_toon handles single object fine.
    data = '{"a": 1}'
    assert detect_format(data) == 'json'

def test_detect_json_array():
    data = '[{"a": 1}, {"b": 2}]'
    assert detect_format(data) == 'json'

def test_convert_ndjson():
    data = '{"id": 1, "val": "A"}\n{"id": 2, "val": "B"}'
    res = ndjson_to_toon(data)
    # Should use Polars or standard list logic -> table
    # root[2]{id,val}:
    #  1,A
    #  2,B
    assert "root[2]{id,val}:" in res
    assert "1,A" in res
    assert "2,B" in res

def test_convert_ndjson_empty_lines():
    data = '{"id": 1}\n\n{"id": 2}'
    res = ndjson_to_toon(data)
    assert "root[2]{id}:" in res
