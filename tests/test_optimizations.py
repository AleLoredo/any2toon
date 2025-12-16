import pytest
import io
import warnings
import pyarrow as pa
import pyarrow.parquet as pq
from unittest.mock import patch
from any2toon import convert_to_toon, set_warnings
from any2toon import converters

def test_optimization_fallbacks_csv():
    # Setup data > 100 rows
    lines = ["id,val"] + [f"{i},data{i}" for i in range(150)]
    csv_data = "\n".join(lines)
    
    set_warnings(True)

    # 1. Test Polars (Priority 1)
    res_polars = convert_to_toon(csv_data, 'csv')
    assert "root[150]{id,val}:" in res_polars
    assert "0,data0" in res_polars
    assert "149,data149" in res_polars

    # 2. Test Pandas (Priority 2) - Mock Polars missing, Pandas present
    with patch('any2toon.converters._HAS_POLARS', False):
         with patch('any2toon.converters._HAS_PANDAS', True):
             with warnings.catch_warnings(record=True) as record:
                 warnings.simplefilter("always")
                 res_pandas = convert_to_toon(csv_data, 'csv')
                 assert "root[150]{id,val}:" in res_pandas
                 my_warnings = [w for w in record if "Optimized engines" in str(w.message)]
                 assert len(my_warnings) == 0

    # 3. Test Fallback (Priority 3) - Mock both missing
    with patch('any2toon.converters._HAS_POLARS', False):
         with patch('any2toon.converters._HAS_PANDAS', False):
             with pytest.warns(UserWarning, match="Optimized engines"):
                res_fallback = convert_to_toon(csv_data, 'csv')
                assert "root[150]{id,val}:" in res_fallback
                assert "0,data0" in res_fallback

def test_optimization_fallbacks_parquet():
    # Setup data > 100 rows
    data = [{'col': 'val'} for _ in range(150)]
    table = pa.Table.from_pylist(data)
    fo = io.BytesIO()
    pq.write_table(table, fo)
    fo.seek(0)
    parquet_data = fo.read()
    
    set_warnings(True)

    # 1. Polars Present (Priority 1)
    res_polars = convert_to_toon(parquet_data, 'parquet')
    assert "root[150]{col}:" in res_polars
    assert "val" in res_polars # " val" matches " val"
    
    # 2. Polars Missing, Pandas Present (Priority 2)
    with patch('any2toon.converters._HAS_POLARS', False):
        with patch('any2toon.converters._HAS_PANDAS', True):
            # No warning
            with warnings.catch_warnings(record=True) as record:
                warnings.simplefilter("always")
                res_pandas = convert_to_toon(parquet_data, 'parquet')
                assert "root[150]{col}:" in res_pandas
                my_warnings = [w for w in record if "Optimized engines" in str(w.message)]
                assert len(my_warnings) == 0

    # 3. Both Missing -> Fallback checks warning
    with patch('any2toon.converters._HAS_POLARS', False):
        with patch('any2toon.converters._HAS_PANDAS', False):
            with pytest.warns(UserWarning, match="Optimized engines"):
                res_fallback = convert_to_toon(parquet_data, 'parquet')
                assert "root[150]{col}:" in res_fallback

def test_parquet_small_file_no_optimization():
    # < 100 rows should NOT trigger optimization logic (verified by ensuring no warning when opt missing)
    data = [{'col': 'val'} for _ in range(50)]
    table = pa.Table.from_pylist(data)
    fo = io.BytesIO()
    pq.write_table(table, fo)
    fo.seek(0)
    parquet_data = fo.read()
    
    set_warnings(True)
    
    with patch('any2toon.converters._HAS_POLARS', False):
        with patch('any2toon.converters._HAS_PANDAS', False):
            # Should NOT warn because threshold (100) not met
             with warnings.catch_warnings(record=True) as record:
                warnings.simplefilter("always")
                convert_to_toon(parquet_data, 'parquet')
                my_warnings = [w for w in record if "Optimized engines" in str(w.message)]
                assert len(my_warnings) == 0
