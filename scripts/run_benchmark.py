import time
import sys
import os
import io
import csv
import json
import warnings
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import polars as pl
from typing import List, Dict

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '../src'))
from any2toon.converters import (
    _polars_csv_to_toon, 
    _pandas_csv_to_toon, 
    _polars_parquet_to_toon, 
    _pandas_parquet_to_toon,
    _polars_df_to_toon,
    csv_to_toon,
    parquet_to_toon,
    _HAS_POLARS,
    _HAS_PANDAS
)
from any2toon.toon_serializer import dumps as base_dumps
from unittest.mock import patch

def generate_data(rows: int) -> List[Dict]:
    return [
        {"id": i, "name": f"User_{i}", "value": i * 1.5, "active": i % 2 == 0}
        for i in range(rows)
    ]

def time_func(func, *args, **kwargs):
    start = time.perf_counter()
    func(*args, **kwargs)
    return (time.perf_counter() - start) * 1000  # ms

def benchmark_csv():
    print("\n--- CSV Benchmark ---")
    sizes = [10, 50, 100, 200, 500, 1000, 5000]
    results = []
    
    for n in sizes:
        data = generate_data(n)
        # Create CSV string
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
        csv_str = output.getvalue()
        
        # 1. Base (Force mocks)
        with patch('any2toon.converters._HAS_POLARS', False), \
             patch('any2toon.converters._HAS_PANDAS', False):
            t_base = time_func(csv_to_toon, csv_str)
            
        # 2. Pandas
        t_pandas = time_func(_pandas_csv_to_toon, csv_str)
        
        # 3. Polars
        t_polars = time_func(_polars_csv_to_toon, csv_str)
        
        best = min(t_base, t_pandas, t_polars)
        winner = "Base" if best == t_base else ("Pandas" if best == t_pandas else "Polars")
        
        print(f"Rows: {n:<5} | Base: {t_base:.2f}ms | Pandas: {t_pandas:.2f}ms | Polars: {t_polars:.2f}ms | Winner: {winner}")
        results.append((n, winner, best))

def benchmark_parquet():
    print("\n--- Parquet Benchmark ---")
    sizes = [10, 50, 100, 200, 500, 1000, 5000]
    
    for n in sizes:
        data = generate_data(n)
        table = pa.Table.from_pylist(data)
        buf = io.BytesIO()
        pq.write_table(table, buf)
        buf.seek(0)
        pq_bytes = buf.read()
        
        # 1. Base (Force mocks)
        with patch('any2toon.converters._HAS_POLARS', False), \
             patch('any2toon.converters._HAS_PANDAS', False):
             # Ensure seek 0 internal logic works or pass fresh bytes
             t_base = time_func(parquet_to_toon, pq_bytes)
             
        # 2. Pandas
        t_pandas = time_func(_pandas_parquet_to_toon, pq_bytes)
        
        # 3. Polars
        t_polars = time_func(_polars_parquet_to_toon, pq_bytes)
        
        best = min(t_base, t_pandas, t_polars)
        winner = "Base" if best == t_base else ("Pandas" if best == t_pandas else "Polars")
        print(f"Rows: {n:<5} | Base: {t_base:.2f}ms | Pandas: {t_pandas:.2f}ms | Polars: {t_polars:.2f}ms | Winner: {winner}")

def benchmark_list_of_dicts():
    print("\n--- List-of-Dicts (JSON/BSON/Avro sim) Benchmark [Pandas Focus] ---")
    # Extended range to find Pandas crossover
    sizes = [1000, 5000, 10000, 50000, 100000]
    
    for n in sizes:
        data = generate_data(n)
        
        # 1. Base Serializer
        t_base = time_func(base_dumps, data)
        
        # 2. Pandas Promotion
        def run_pandas_promo():
            df = pd.DataFrame(data)
            cols = list(df.columns)
            # Replicate the vectorized logic
            # Note: astype(str) is expensive.
            df_str = df.astype(str)
            series_res = df_str[cols[0]]
            for c in cols[1:]:
                series_res = series_res + "," + df_str[c]
            header = f"root[{len(df)}]{{{','.join(cols)}}}:"
            return header + "\n" + "\n".join(" " + series_res)
            
        t_pandas = time_func(run_pandas_promo)
            
        # 3. Polars (Reference)
        if _HAS_POLARS:
            def run_polars_promo():
                df = pl.from_dicts(data)
                return _polars_df_to_toon(df)
            t_polars = time_func(run_polars_promo)
        else:
            t_polars = 999999
        
        best = min(t_base, t_pandas)
        winner = "Base" if best == t_base else "Pandas" # Ignoring Polars for this decision
        
        print(f"Rows: {n:<6} | Base: {t_base:.2f}ms | Pandas: {t_pandas:.2f}ms | Polars (ref): {t_polars:.2f}ms | Winner (Base vs Pd): {winner}")

if __name__ == "__main__":
    benchmark_csv()
    benchmark_parquet()
    benchmark_list_of_dicts()
