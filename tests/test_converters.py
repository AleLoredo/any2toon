import pytest
import json
import io
import fastavro
import pyarrow as pa
import pyarrow.parquet as pq
from any2toon import convert_to_toon, ConversionError, InvalidFormatError

def test_json_to_toon():
    json_data = '{"name": "Alice", "age": 30, "admin": true}'
    expected_output = """name: Alice
age: 30
admin: true"""
    assert convert_to_toon(json_data, 'json') == expected_output

def test_yaml_to_toon():
    yaml_data = """
name: Bob
age: 25
"""
    expected_output = """name: Bob
age: 25"""
    assert convert_to_toon(yaml_data, 'yaml') == expected_output

def test_xml_to_toon():
    xml_data = "<person><name>Charlie</name><age>35</age></person>"
    # expected output structure depends on xmltodict parsing which treats single elements as dicts
    expected_output = """person:
  name: Charlie
  age: 35"""
    assert convert_to_toon(xml_data, 'xml') == expected_output

def test_nested_structures():
    data = {
        "users": [
            {"name": "User1", "roles": ["admin", "editor"]},
            {"name": "User2", "roles": []}
        ]
    }
    json_data = json.dumps(data)
    expected_output = """users:
  -
    name: User1
    roles:
      - admin
      - editor
  -
    name: User2
    roles: []"""
    
    # Updated expectation for Compact Table Format
    # Logic: users is a list of dicts.
    # users[2]{name,roles}:
    #  User1,['admin', 'editor']  <-- Note: roles list converted to string representation by str()
    #  User2,[]
    
    actual = convert_to_toon(json_data, 'json')
    assert "users[2]{name,roles}:" in actual
    assert "User1" in actual
    assert "User2" in actual

def test_csv_to_toon():
    csv_data = """name,age,role
Dave,40,manager
Eve,28,developer"""
    # Expected:
    # root[2]{name,age,role}:
    #  Dave,40,manager
    #  Eve,28,developer
    actual = convert_to_toon(csv_data, 'csv')
    assert "root[2]{name,age,role}:" in actual
    assert "Dave,40,manager" in actual
    assert "Eve,28,developer" in actual

def test_avro_to_toon():
    schema = {
        "doc": "A weather reading.",
        "name": "Weather",
        "namespace": "test",
        "type": "record",
        "fields": [
            {"name": "station", "type": "string"},
            {"name": "temp", "type": "int"},
        ],
    }
    records = [
        {"station": "011990-99999", "temp": 0},
        {"station": "011990-99999", "temp": 22},
    ]
    # Create in-memory avro file
    fo = io.BytesIO()
    fastavro.writer(fo, schema, records)
    fo.seek(0)
    data = fo.read()
    
    # Expected:
    # root[2]{station,temp}:
    #  011990-99999,0
    #  011990-99999,22
    actual = convert_to_toon(data, 'avro')
    assert "root[2]{station,temp}:" in actual
    assert "011990-99999,0" in actual
    assert "011990-99999,22" in actual

def test_parquet_to_toon():
    data = [
        {'name': 'Alice', 'score': 100},
        {'name': 'Bob', 'score': 200}
    ]
    table = pa.Table.from_pylist(data)
    fo = io.BytesIO()
    pq.write_table(table, fo)
    fo.seek(0)
    parquet_data = fo.read()
    
    actual = convert_to_toon(parquet_data, 'parquet')
    assert "root[2]{name,score}:" in actual
    assert "Alice,100" in actual
    assert "Bob,200" in actual

def test_invalid_json():
    with pytest.raises(ConversionError):
        convert_to_toon("{invalid json", 'json')

def test_invalid_yaml():
    with pytest.raises(ConversionError):
        convert_to_toon(": invalid yaml", 'yaml') # Colon at start usually errors or parses weirdly

def test_invalid_xml():
    with pytest.raises(ConversionError):
        convert_to_toon("<root>unclosed", 'xml')

def test_unsupported_format():
    with pytest.raises(InvalidFormatError):
        convert_to_toon("{}", 'toml')
