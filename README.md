# any2toon

**any2toon** is a robust Python library designed to convert various data serialization formats (**JSON, YAML, XML, CSV, Avro, Parquet, BSON**) into **TOON** (Token Oriented Object Notation).

![Python Version](https://img.shields.io/badge/python-3.7%2B-blue)
![License](https://img.shields.io/badge/license-MIT-green)
![Build Status](https://img.shields.io/badge/build-passing-brightgreen)

## 📖 Introduction

**any2toon** solves the problem of preparing diverse data sources for Large Language Model (LLM) ingestion. LLMs often perform better (and cost less) when processing data that is free of excessive syntactic noise (like the braces and quotes in JSON). 

**TOON** format is designed to be:
- **Token-Efficient**: Minimizes punctuation overhead.
- **Human-Readable**: Uses meaningful indentation and clean key-value pairs.
- **Structure-Preserving**: Maintains the hierarchy and relationships of the original data.

This library acts as a universal adapter, taking standard formats (JSON, YAML, XML, CSV, Avro, Parquet) and normalizing them into this optimized notation.

---

## 🚀 Installation

## 🚀 Installation

### Minimal Installation (JSON, YAML, XML, CSV support)
```bash
pip install any2toon
```

### Full Installation (All formats + Optimizations)
```bash
pip install "any2toon[all]"
```

### Format-Specific Installation
If you only need specific formats, you can install them individually to keep your environment light:

```bash
pip install "any2toon[parquet]" # For Parquet support (if installing from PyPI)
pip install ".[parquet]"        # If installing locally from source
```

**Dependencies:**
- `PyYAML`: For parsing YAML files.
- `xmltodict`: For converting XML parsing trees into Python dictionaries.
- `fastavro`: (Optional) For reading binary Apache Avro OCF files.
- `pyarrow`: (Optional) For reading Apache Parquet optimized columnar files.
- `pymongo`: (Optional) For decoding BSON files.
- `polars` / `pandas`: (Optional) For high-performance conversion of large datasets.

---

## 🛠️ Core Functionality & Approach

- **CSV**: If `rows >= 100`:
  - **Priority 1**: Uses `polars`.
  - **Priority 2**: Uses `pandas`.
  - **Fallback**: Standard Python (with warning).
- **Parquet**: If `rows >= 100`:
  - **Priority 1**: Uses `polars`.
  - **Priority 2**: Uses `pandas`.
  - **Fallback**: Base Python (with warning).
The library follows a consistent pipeline for all conversions:
1.  **Ingestion**: Read the raw input (string, bytes, or file stream) using a format-specific specialized library.
2.  **Normalization**: Convert the input into a standard Python object structure (Lists and Dictionaries).
3.  **Serialization**: Traverse the Python object and generate the TOON string using a custom, lightweight serializer.

Below is the detailed approach for each supported format.

### 1. JSON (JavaScript Object Notation)
**Approach**: 
We utilize Python's standard `json` library. Since JSON maps 1:1 with Python dictionaries and lists, this transformation is direct and high-fidelity.

```python
from any2toon import convert_to_toon
json_data = '{"user": "alice", "roles": ["admin"]}'
print(convert_to_toon(json_data, 'json'))
# Output:
# user: alice
# roles:
#   - admin
```

### 2. YAML (YAML Ain't Markup Language)
**Approach**: 
We use `PyYAML`'s `safe_load` to parse YAML. This renders YAML's alias/anchor features and complex types into resolved Python objects before conversion, ensuring the final TOON output is a clean data representation without parser-specific artifacts.

```python
yaml_data = """
user: bob
attributes:
  active: true
"""
print(convert_to_toon(yaml_data, 'yaml'))
# Output:
# user: bob
# attributes:
#   active: true
```

### 3. XML (eXtensible Markup Language)
**Approach**: 
XML is inherently more complex due to attributes vs. text content. We leverage `xmltodict` to parse the XML tree.
- **Normalization Strategy**: Elements become keys. Nested elements become nested dictionaries.
- **Note**: Root elements are preserved, maintaining the document's semantic structure.

```python
xml_data = "<config><mode>production</mode></config>"
print(convert_to_toon(xml_data, 'xml'))
# Output:
# config:
#   mode: production
```

### 4. CSV (Comma Separated Values)
**Approach**: 
We use the standard `csv` library's `DictReader`.
- **Assumption**: The first row of the CSV **must** be a header row.
- **Transformation**: Each row becomes a dictionary where keys are column headers. The final structure is a List of Dictionaries.
- **Goal**: To turn tabular data into a record-oriented format readable by LLMs.

```python
csv_data = "id,status\n1,open\n2,closed"
print(convert_to_toon(csv_data, 'csv'))
# Output:
# - id: 1
#   status: open
# - id: 2
#   status: closed
```

### 5. Apache Avro
**Approach**: 
We use `fastavro` for high-performance reading of binary Avro data.
- **Prerequisite**: The input must be in **OCF (Object Container File)** format, which embeds the schema within the file itself. This allows `any2toon` to be stateless and schema-registry agnostic.
- **Transformation**: The binary stream is iterated over to produce standard Python dictionaries.

```python
# Assuming 'bytes_data' is loaded from a valid .avro file
print(convert_to_toon(bytes_data, 'avro'))
```

### 6. Apache Parquet
**Approach**: 
We leverage `pyarrow` to read Parquet files.
- **Pipeline**: Parquet (Columnar) $\to$ Arrow Table $\to$ Python List of Dicts (Row-based) $\to$ TOON.
- **Efficiency Note**: While Parquet is columnar, TOON is row-oriented (for reading). We incur a transformation cost here to make the data human-readable, effectively "pivoting" the data structures.

```python
# Assuming 'bytes_data' is loaded from a valid .parquet file
print(convert_to_toon(bytes_data, 'parquet'))
```

### 7. BSON (Binary JSON)
**Approach**: 
We use `pymongo` (specifically its `bson` module) to decode BSON data.
- **Support**: Handles both single BSON documents and concatenated BSON streams (mongo dumps).
- **Transformation**: BSON types are decoded into standard Python dictionaries/lists, then serialized to TOON.

```python
import bson
# Assuming 'bytes_data' is a BSON byte string
print(convert_to_toon(bytes_data, 'bson'))
```

---

## 📚 API Reference

### `convert_to_toon(data_input, input_format) -> str`
The universal entry point.
- `data_input`: `str` (for text formats), `bytes` or `BytesIO` (for binary formats like Avro/Parquet), or `dict/list` (if pre-parsed).
- `input_format`: Case-insensitive string: `'json'`, `'yaml'`, `'xml'`, `'csv'`, `'avro'`, `'parquet'`.

### Specific Converters
Found in `any2toon.converters`:
- `json_to_toon(data)`
- `yaml_to_toon(data)`
- `xml_to_toon(data)`
- `csv_to_toon(data)`
- `avro_to_toon(data)`
- `parquet_to_toon(data)`
- `bson_to_toon(data)`

### Exceptions
- `InvalidFormatError`: Raised if you request a format not supported.
- `ConversionError`: Raised if the input data is malformed or parsing fails.

---

## 🧪 Testing

The library is backed by a comprehensive `pytest` suite ensuring fidelity for all conversions.

```bash
# Activate your environment
source venv/bin/activate

# Run tests
pytest
```
