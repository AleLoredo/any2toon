import time
import csv
import io
import random
import string
import polars as pl
from any2toon.converters import csv_to_toon, _polars_csv_to_toon

# Mock _HAS_POLARS to ensure we can switch between implementations easily manually
# Actually we can just call _polars_csv_to_toon directly vs csv_to_toon (which uses standard if we force it or just use internal logic)
# To compare "standard" vs "polars", we should compare:
# 1. csv_to_toon (with polars disabled/bypassed)
# 2. _polars_csv_to_toon

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

def standard_csv_to_toon(data):
    # Copy of the standard logic from converters.py
    f = io.StringIO(data)
    reader = csv.DictReader(f)
    # create list of dicts
    parsed_data = list(reader)
    # We need the real dump logic to be fair, 
    # but for benchmark 'csv_to_toon' calls 'toon_dumps' internally.
    # So we can just call csv_to_toon but we need to ensure it DOES NOT use polars path.
    # The simplest way is to mock _HAS_POLARS inside the context, or just copy the logic.
    # Copying is safer to avoid import side effects during the loop.
    from any2toon.toon_serializer import dumps
    return dumps(parsed_data)

def benchmark_point(rows, iterations=5):
    csv_str = generate_csv_string(rows)
    
    # Warmup
    standard_csv_to_toon(csv_str)
    _polars_csv_to_toon(csv_str)
    
    std_times = []
    pol_times = []
    
    for _ in range(iterations):
        start = time.time()
        standard_csv_to_toon(csv_str)
        std_times.append(time.time() - start)
        
        start = time.time()
        _polars_csv_to_toon(csv_str)
        pol_times.append(time.time() - start)
        
    avg_std = sum(std_times) / len(std_times)
    avg_pol = sum(pol_times) / len(pol_times)
    
    if avg_pol >= avg_std:
        improvement_pct = -(avg_pol - avg_std) / avg_std * 100
    else:
        improvement_pct = (avg_std - avg_pol) / avg_std * 100
        
    return avg_std, avg_pol, improvement_pct

def find_threshold():
    print(f"\nScanning for 10% improvement threshold (Polars vs Standard CSV)...")
    print(f"{'Rows':<10} | {'Std (s)':<10} | {'Pol (s)':<10} | {'Improv %':<10}")
    print("-" * 50)
    
    # Scan range 10 to 150 in steps of 10
    for rows in range(10, 160, 10):
        std, pol, imp = benchmark_point(rows, iterations=20)
        print(f"{rows:<10} | {std:<10.5f} | {pol:<10.5f} | {imp:<10.1f}")
        
        if imp >= 10.0:
            print("-" * 50)
            print(f"THRESHOLD REACHED AT ~{rows} ROWS")
            return rows

if __name__ == "__main__":
    find_threshold()
