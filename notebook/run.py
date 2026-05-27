import os
import gc
import time
import sqlite3
import subprocess
from datetime import datetime
import pyarrow.parquet as pq
from nested_pandas.datasets import generate_data

compressions = [None, "snappy", "gzip", "brotli", "lz4", "zstd"]


def GENERATE_DATA(b, n, file_path, algo, is_flat):
    """Generates synthetic nested or flat data and writes it to a Parquet file."""
    nf = generate_data(b, n, seed=1)[["nested"]]
    if is_flat:
        df = nf["nested"].to_lists().to_frame("flat_data")
        df.to_parquet(file_path, compression=algo)
    else:
        nf.to_parquet(file_path, compression=algo)


def SAVE_ALGO_TO_SQLITE3(sqlite3_path, file_obj, algo_key):
    """Saves individual benchmark runs for a SINGLE algorithm to the database."""
    conn = sqlite3.connect(sqlite3_path)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS benchmark_results (
            file_type TEXT,
            base_rows INTEGER,
            nested_rows INTEGER,
            compression TEXT,
            file_size_bytes INTEGER,
            run_number INTEGER,
            run_timestamp DATETIME,
            run_time_sec REAL
        )
    """)

    file_type = "FlatFile" if file_obj.is_flat else "NestedFile"
    file_size = file_obj.file_sizes.get(algo_key, 0)

    if algo_key in file_obj.benchmarking_times:
        for run_data in file_obj.benchmarking_times[algo_key]:
            run_number, timestamp, run_time = run_data

            cursor.execute(
                """
                INSERT INTO benchmark_results 
                (file_type, base_rows, nested_rows, compression, file_size_bytes, run_number, run_timestamp, run_time_sec)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    file_type,
                    file_obj.b,
                    file_obj.n,
                    algo_key,
                    file_size,
                    run_number,
                    timestamp,
                    run_time,
                ),
            )

    conn.commit()
    conn.close()


class File:
    def __init__(self, b: int, n: int, path: str) -> None:
        self.b = b
        self.n = n
        self.order = b * n
        self.file_path = path
        self.file_exists = False

        self.file_sizes = {}
        self.benchmarking_times = {
            "None": [],
            "snappy": [],
            "gzip": [],
            "brotli": [],
            "lz4": [],
            "zstd": [],
        }

        self.current_algo = ""
        self.is_flat = False

    def generate_data(self, algo):
        GENERATE_DATA(self.b, self.n, self.file_path, algo, self.is_flat)
        self.file_exists = os.path.exists(self.file_path)

        algo_key = "None" if algo is None else algo
        self.file_sizes[algo_key] = (
            os.path.getsize(self.file_path) if self.file_exists else 0
        )

    def pre_benchmark(self):
        """Loads the file into the OS page cache using vmtouch."""
        if self.file_exists:
            subprocess.run(
                ["vmtouch", "-t", self.file_path], check=True, stdout=subprocess.DEVNULL
            )

    def run_benchmark_loop(self, db_path, num_runs=10):
        """Executes the complete granular lifecycle per algorithm sequentially."""
        for algo in compressions:
            self.current_algo = "None" if algo is None else algo
            print(f"      -> Processing Compression: {self.current_algo.upper()}")

            # 1 & 2. Choose algorithm & generate file
            self.generate_data(algo)

            # 3. Load file completely into OS page cache
            self.pre_benchmark()

            # 4. Benchmark the file for the specified number of runs
            for run_idx in range(1, num_runs + 1):
                run_timestamp = datetime.now().isoformat()

                start_time = time.perf_counter()
                table = pq.read_table(self.file_path, use_threads=True)
                run_time = time.perf_counter() - start_time

                del table
                gc.collect()

                self.benchmarking_times[self.current_algo].append(
                    (run_idx, run_timestamp, run_time)
                )

            # 5 & 6. Clear cache & delete file immediately after benchmarking
            self.post_benchmark()

            # 7. Save this specific algorithm's run data immediately to SQL
            SAVE_ALGO_TO_SQLITE3(db_path, self, self.current_algo)

    def post_benchmark(self):
        """Evicts the file from the OS page cache and deletes it from disk."""
        if self.file_exists:
            subprocess.run(
                ["vmtouch", "-e", self.file_path], check=True, stdout=subprocess.DEVNULL
            )
            os.remove(self.file_path)
            self.file_exists = False


class FlatFile(File):
    def __init__(self, b: int, n: int, path: str) -> None:
        super().__init__(b, n, path)
        self.is_flat = True


class NestedFile(File):
    def __init__(self, b: int, n: int, path: str) -> None:
        super().__init__(b, n, path)
        self.is_flat = False


# ==========================================
# Main Execution Pipeline
# ==========================================
if __name__ == "__main__":
    db_path = "benchmarks.db"

    size_configs = [
        # ==========================================
        # 10^2 (100 items) - Micro Scale
        # ==========================================
        (10, 10),
        # ==========================================
        # 10^3 (1,000 items)
        # ==========================================
        (10, 100),  # Extremely Deep
        (100, 10),  # Extremely Wide
        # ==========================================
        # 10^4 (10,000 items)
        # ==========================================
        (10, 1000),  # Extremely Deep
        (100, 100),  # Balanced
        (1000, 10),  # Extremely Wide
        # ==========================================
        # 10^5 (100,000 items)
        # ==========================================
        (10, 10000),  # Extremely Deep
        (100, 1000),  # Deep
        (1000, 100),  # Wide
        (10000, 10),  # Extremely Wide
        # ==========================================
        # 10^6 (1 Million items) ~ Approx 8 MB uncompressed
        # ==========================================
        (10, 100000),  # Extremely Deep
        (100, 10000),  # Deep
        (1000, 1000),  # Balanced
        (10000, 100),  # Wide
        (100000, 10),  # Extremely Wide
        # ==========================================
        # 10^7 (10 Million items) ~ Approx 80 MB uncompressed
        # ==========================================
        (10, 1000000),  # Extremely Deep
        (100, 100000),  # Deep
        (1000, 10000),  # Slightly Deep
        (10000, 1000),  # Slightly Wide
        (100000, 100),  # Wide
        (1000000, 10),  # Extremely Wide
        # ==========================================
        # 10^8 (100 Million items) ~ Approx 800 MB uncompressed
        # ==========================================
        (10, 10000000),  # Extremely Deep
        (100, 1000000),  # Deep
        (1000, 100000),  # Slightly Deep
        (10000, 10000),  # Balanced
        (100000, 1000),  # Slightly Wide
        (1000000, 100),  # Wide
        (10000000, 10),  # Extremely Wide
        # ==========================================
        # 10^9 (1 Billion items) ~ Approx 8 GB uncompressed
        # DANGER: Requires massive system RAM!
        # ==========================================
        # (10, 100000000),  # Extremely Deep
        # (100, 10000000),  # Deep
        # (1000, 1000000),  # Slightly Deep
        # (31622, 31622),   # Balanced (sqrt of 10^9)
        # (1000000, 1000),  # Slightly Wide
        # (10000000, 100),  # Wide
        # (100000000, 10),  # Extremely Wide
    ]

    total_configs = len(size_configs)

    for idx, (base_rows, nested_rows) in enumerate(size_configs, 1):
        print(f"\n========================================================")
        print(
            f"[{idx}/{total_configs}] Processing Dataset Configuration: {base_rows}x{nested_rows}"
        )
        print(f"========================================================")

        # --- Task 1: Pipeline loop for Flat representation ---
        print("  -> Starting FlatFile Sequence...")
        flat = FlatFile(base_rows, nested_rows, "temp_flat.parquet")
        # Adjust num_runs as needed (e.g., 5-10 runs)
        flat.run_benchmark_loop(db_path, num_runs=5)

        # --- Task 2: Pipeline loop for Native Nested representation ---
        print("  -> Starting NestedFile Sequence...")
        nested = NestedFile(base_rows, nested_rows, "temp_nested.parquet")
        nested.run_benchmark_loop(db_path, num_runs=5)

    print(
        "\nAll tasks in size matrix successfully processed. All run metrics saved directly to SQLite3."
    )
