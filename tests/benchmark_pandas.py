import time
import csv
import io
import random
import string
import pandas as pd
import polars as pl
from any2toon.converters import csv_to_toon, _polars_csv_to_toon
from any2toon.toon_serializer import dumps

def generate_csv_string(rows):
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=["id", "name", "score", "active"])
    writer.writeheader()
    for i in range(rows):
        writer.writerow({
            "id": i,
            "name": ''.join(random.choices(string.ascii_letters, k=10)),
            "score": random.randint(0, 100),
            "active": random.choice([True, False])
        })
    return output.getvalue()

def pandas_csv_to_toon(csv_data):
    df = pd.read_csv(io.StringIO(csv_data))
    if len(df) == 0:
        return ""
        
    cols = df.columns
    # Vectorized string creation
    
    # First column: "- col: val"
    # Others: "  col: val"
    
    # We construct the full string for each row series
    # Start with first col
    result_series = "- " + cols[0] + ": " + df[cols[0]].astype(str)
    
    # Add rest
    for col in cols[1:]:
        result_series = result_series + "\n  " + col + ": " + df[col].astype(str)
        
    return "\n".join(result_series)

def standard_csv_to_toon_wrapper(data):
    # Pure standard logic simulation using the library function but assuming polars is missing/ignored
    # csv_to_toon uses standard if row count < 60 OR polars missing.
    # To test standard on large data, we must bypass the check or use the raw logic.
    f = io.StringIO(data)
    reader = csv.DictReader(f)
    parsed_data = list(reader)
    return dumps(parsed_data)

def benchmark(row_counts):
    print(f"\nScanning Performance: Standard vs Pandas vs Polars")
    print(f"{'Rows':<10} | {'Std (s)':<10} | {'Pandas (s)':<10} | {'Polars (s)':<10}")
    print("-" * 60)
    
    for rows in row_counts:
        csv_str = generate_csv_string(rows)
        
        # Warmup
        pandas_csv_to_toon(csv_str)
        
        # Standard
        start = time.time()
        standard_csv_to_toon_wrapper(csv_str)
        std_time = time.time() - start
        
        # Pandas
        start = time.time()
        pandas_csv_to_toon(csv_str)
        pd_time = time.time() - start
        
        # Polars
        start = time.time()
        _polars_csv_to_toon(csv_str)
        pl_time = time.time() - start
        
        print(f"{rows:<10} | {std_time:<10.5f} | {pd_time:<10.5f} | {pl_time:<10.5f}")

if __name__ == "__main__":
    counts = [100, 1000, 10000, 100000]
    benchmark(counts)
