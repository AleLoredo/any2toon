import pytest
import bson
import io
from any2toon import convert_to_toon
from any2toon.exceptions import ConversionError

def test_bson_to_toon_single_doc():
    doc = {"name": "Test", "val": 123}
    data = bson.encode(doc)
    
    result = convert_to_toon(data, 'bson')
    # bson.decode_all returns [doc]
    # So it's a table of 1 item:
    # root[1]{name,val}:
    #  Test,123
    assert "root[1]{name,val}:" in result
    assert "Test,123" in result

def test_bson_to_toon_multiple_docs():
    doc1 = {"id": 1, "name": "A"}
    doc2 = {"id": 2, "name": "B"}
    # BSON dump format is just concatenated BSON documents -> List of Dicts
    data = bson.encode(doc1) + bson.encode(doc2)
    
    result = convert_to_toon(data, 'bson')
    # Should result in Table format
    # root[2]{id,name}:
    #  1,A
    #  2,B
    assert "root[2]{id,name}:" in result
    assert "1,A" in result
    assert "2,B" in result

def test_invalid_bson():
    data = b"invalid_bson_bytes"
    with pytest.raises(ConversionError):
        convert_to_toon(data, 'bson')

def test_bson_from_bytesio():
    doc = {"name": "StreamTest"}
    data = bson.encode(doc)
    stream = io.BytesIO(data)
    
    result = convert_to_toon(stream, 'bson')
    # root[1]{name}:
    #  StreamTest
    assert "root[1]{name}:" in result
    assert "StreamTest" in result
