import pytest
import bson
import io
from any2toon import convert_to_toon
from any2toon.exceptions import ConversionError

def test_bson_to_toon_single_doc():
    doc = {"name": "Test", "val": 123}
    data = bson.encode(doc)
    
    result = convert_to_toon(data, 'bson')
    assert "name: Test" in result
    assert "val: 123" in result

def test_bson_to_toon_multiple_docs():
    doc1 = {"id": 1, "name": "A"}
    doc2 = {"id": 2, "name": "B"}
    # BSON dump format is just concatenated BSON documents
    data = bson.encode(doc1) + bson.encode(doc2)
    
    result = convert_to_toon(data, 'bson')
    # Should result in a list of dicts
    assert "id: 1" in result
    assert "name: A" in result
    assert "id: 2" in result
    assert "name: B" in result
    assert result.count("-") >= 2

def test_invalid_bson():
    data = b"invalid_bson_bytes"
    with pytest.raises(ConversionError):
        convert_to_toon(data, 'bson')

def test_bson_from_bytesio():
    doc = {"name": "StreamTest"}
    data = bson.encode(doc)
    stream = io.BytesIO(data)
    
    result = convert_to_toon(stream, 'bson')
    assert "name: StreamTest" in result
