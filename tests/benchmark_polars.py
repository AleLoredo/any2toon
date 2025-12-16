import time
import csv
import io
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import random
import string
from any2toon.converters import csv_to_toon, parquet_to_toon

def generate_data(rows):
    data = []
    for i in range(rows):
        data.append({
            "id": i,
            "name": ''.join(random.choices(string.ascii_letters, k=10)),
            "score": random.randint(0, 100),
            "active": random.choice([True, False])
        })
    return data

def polars_csv_to_toon(csv_string):
    df = pl.read_csv(io.StringIO(csv_string))
    # Vectorized string construction simulation
    # This is a simplified TOON string construction for benchmarking
    # Assuming the specific format: key: val
    
    # We construct a series of strings that represents the TOON block for each row
    # In reality this would be more dynamic based on columns, but for perf testing
    # we can be explicit or dynamic. Let's try dynamic dynamic roughly.
    
    exprs = []
    for col in df.columns:
        # Create "  col: value" string series
        # Note: Polars string formatting is powerful.
        exprs.append(pl.lit(f"  {col}: ") + df[col].cast(pl.Utf8))
        
    # We need to interleave these, or concat them with newlines.
    # Concat_str with separator \n
    row_strings = pl.concat_str(exprs, separator="\n")
    
    # Prepend the dash for the first element? TOON uses - key: val for first item usually
    # But wait, my current implementation produces:
    # - key: val
    #   key2: val
    
    # So for the first column 'id', we prepend "- " + "id: " + val
    # For others we prepend "  " + "col: " + val
    
    first_col = df.columns[0]
    rest_cols = df.columns[1:]
    
    first_expr = pl.lit(f"- {first_col}: ") + df[first_col].cast(pl.Utf8)
    rest_exprs = [pl.lit(f"  {col}: ") + df[col].cast(pl.Utf8) for col in rest_cols]
    
    all_exprs = [first_expr] + rest_exprs
    # Execute the expression in a select context
    final_output = df.select(
        pl.concat_str(all_exprs, separator="\n").str.join("\n")
    ).item()
    return final_output

def polars_parquet_to_toon(parquet_bytes):
    df = pl.read_parquet(io.BytesIO(parquet_bytes))
    # Same logic as CSV
    first_col = df.columns[0]
    rest_cols = df.columns[1:]
    
    first_expr = pl.lit(f"- {first_col}: ") + df[first_col].cast(pl.Utf8)
    rest_exprs = [pl.lit(f"  {col}: ") + df[col].cast(pl.Utf8) for col in rest_cols]
    
    all_exprs = [first_expr] + rest_exprs
    final_result = df.select(
        pl.concat_str(all_exprs, separator="\n").str.join("\n")
    ).item()
    return final_result

def benchmark_csv(row_counts):
    print(f"\n--- CSV Benchmark ---")
    print(f"{'Rows':<10} | {'Current (s)':<12} | {'Polars (s)':<12} | {'Improvement':<12}")
    print("-" * 55)
    
    for rows in row_counts:
        data = generate_data(rows)
        
        # Create CSV string
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
        csv_str = output.getvalue()
        
        # Current
        start = time.time()
        csv_to_toon(csv_str)
        curr_time = time.time() - start
        
        # Polars
        start = time.time()
        polars_csv_to_toon(csv_str)
        pol_time = time.time() - start
        
        imp = (curr_time - pol_time) / curr_time * 100
        print(f"{rows:<10} | {curr_time:<12.5f} | {pol_time:<12.5f} | {imp:<11.1f}%")

def benchmark_parquet(row_counts):
    print(f"\n--- Parquet Benchmark ---")
    print(f"{'Rows':<10} | {'Current (s)':<12} | {'Polars (s)':<12} | {'Improvement':<12}")
    print("-" * 55)
    
    for rows in row_counts:
        data = generate_data(rows)
        
        # Create Parquet bytes
        table = pa.Table.from_pylist(data)
        out = io.BytesIO()
        pq.write_table(table, out)
        out.seek(0)
        pq_bytes = out.read()
        
        # Current
        start = time.time()
        parquet_to_toon(pq_bytes)
        curr_time = time.time() - start
        
        # Polars
        start = time.time()
        polars_parquet_to_toon(pq_bytes)
        pol_time = time.time() - start
        
        imp = (curr_time - pol_time) / curr_time * 100
        print(f"{rows:<10} | {curr_time:<12.5f} | {pol_time:<12.5f} | {imp:<11.1f}%")

if __name__ == "__main__":
    counts = [100, 1000, 10000, 100000]
    benchmark_csv(counts)
    benchmark_parquet(counts)
