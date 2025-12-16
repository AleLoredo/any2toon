import time
import io
import random
import string
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from any2toon.toon_serializer import dumps

def generate_parquet_bytes(rows):
    data = []
    for i in range(rows):
        data.append({
            "id": i,
            "name": ''.join(random.choices(string.ascii_letters, k=8)),
            "score": random.randint(0, 100),
            "active": random.choice([True, False])
        })
    table = pa.Table.from_pylist(data)
    out = io.BytesIO()
    pq.write_table(table, out)
    out.seek(0)
    return out.read()

def base_parquet_to_toon(data):
    # Mimic standard implementation: PyArrow -> List -> TOON
    f = io.BytesIO(data)
    table = pq.read_table(f)
    parsed_data = table.to_pylist()
    return dumps(parsed_data)

def pandas_parquet_to_toon(data):
    f = io.BytesIO(data)
    df = pd.read_parquet(f)
    if len(df) == 0:
        return ""
    
    cols = df.columns
    # Vectorized string creation
    result_series = "- " + cols[0] + ": " + df[cols[0]].astype(str)
    for col in cols[1:]:
        result_series = result_series + "\n  " + col + ": " + df[col].astype(str)
    return "\n".join(result_series)

def polars_parquet_to_toon(data):
    f = io.BytesIO(data)
    df = pl.read_parquet(f)
    if df.height == 0:
        return ""
    
    first_col = df.columns[0]
    rest_cols = df.columns[1:]
    
    first_expr = pl.lit(f"- {first_col}: ") + df[first_col].cast(pl.Utf8)
    rest_exprs = [pl.lit(f"  {col}: ") + df[col].cast(pl.Utf8) for col in rest_cols]
    all_exprs = [first_expr] + rest_exprs
    
    final_output = df.select(
        pl.concat_str(all_exprs, separator="\n").str.join("\n")
    ).item()
    return final_output

def benchmark(rows, iterations=20):
    data = generate_parquet_bytes(rows)
    
    # Warmup
    base_parquet_to_toon(data)
    pandas_parquet_to_toon(data)
    polars_parquet_to_toon(data)
    
    base_times = []
    pd_times = []
    pl_times = []
    
    for _ in range(iterations):
        start = time.time()
        base_parquet_to_toon(data)
        base_times.append(time.time() - start)
        
        start = time.time()
        pandas_parquet_to_toon(data)
        pd_times.append(time.time() - start)
        
        start = time.time()
        polars_parquet_to_toon(data)
        pl_times.append(time.time() - start)
        
    avg_base = sum(base_times) / len(base_times)
    avg_pd = sum(pd_times) / len(pd_times)
    avg_pl = sum(pl_times) / len(pl_times)
    
    return avg_base, avg_pd, avg_pl

def run_sweep():
    print(f"\nScanning Parquet Performance (Base vs Pandas vs Polars)")
    print(f"{'Rows':<8} | {'Base (s)':<10} | {'Pandas (s)':<10} | {'Polars (s)':<10} | {'Winner'}")
    print("-" * 75)
    
    # Sweep small counts to find lower bound threshold
    for rows in [10, 50, 60, 100, 500, 1000, 5000, 10000]:
        base, pd_t, pl_t = benchmark(rows, iterations=10)
        
        winner = "Base"
        if pd_t < base and pd_t < pl_t:
            winner = "Pandas"
        elif pl_t < base and pl_t < pd_t:
            winner = "Polars"
            
        print(f"{rows:<8} | {base:<10.5f} | {pd_t:<10.5f} | {pl_t:<10.5f} | {winner}")

if __name__ == "__main__":
    run_sweep()
