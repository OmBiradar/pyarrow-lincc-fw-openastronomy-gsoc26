#
# Code inspired from https://github.com/apache/arrow/issues/48636
#
# References:
#   * https://www.geeksforgeeks.org/python/python-os-remove-method/
#   * https://www.digitalocean.com/community/tutorials/python-system-command-os-subprocess-call
#   * https://docs.python.org/3/library/subprocess.html
#   * https://linux.die.net/man/8/vmtouch
#   * https://linuxvox.com/blog/what-is-a-pid-file-and-what-does-it-contain/
#

# TODO: Update to have all 7 compression algorithms

# Inbuilt modules
import os
import subprocess
import timeit
import sys
import sqlite3
from string import Template

# External modules
from nested_pandas.datasets import generate_data
import pyarrow as pa  # noqa: F401

PYARROW_VERSION = ""
CONFIG = ""

# Benchmark config variables
# Defaults
FILE_ORDER_START = 1
FILE_ORDER_END = 8
NESTED = True
FLAT = True
MULTI = True
SINGLE = True
RUNS = 10
COMPRESSION = 0

COMPRESSION_ALGORITHMS = ["SNAPPY", "ZSTD", "GZIP", "BROTLI", "LZ4", "NONE"]


def read_config():
    """
    Reads the shorthand config from the global variable `PYARROW_OPENASTRONOMY_BENCHMARK`
    """

    ## ENV VAR FOR BENCHMARK CONFIG
    # export PYARROW_OPENASTRONOMY_BENCHMARK=S15N1M1C1R1000
    # index refrence                         01234567890123456789

    global \
        CONFIG, \
        FILE_ORDER_START, \
        FILE_ORDER_END, \
        NESTED, \
        FLAT, \
        MULTI, \
        SINGLE, \
        COMPRESSION, \
        RUNS

    CONFIG = os.getenv("PYARROW_OPENASTRONOMY_BENCHMARK")
    if not CONFIG:
        return

    FILE_ORDER_START = int(CONFIG[1])
    FILE_ORDER_END = int(CONFIG[2])
    NESTED = int(CONFIG[4]) == 1
    FLAT = True if NESTED == 0 else False
    MULTI = int(CONFIG[6]) == 1
    SINGLE = True if MULTI == 0 else False
    COMPRESSION = CONFIG[8]
    if COMPRESSION == "A":
        COMPRESSION = -1
    else:
        COMPRESSION = int(COMPRESSION)
    RUNS = int(CONFIG[10:])


def generate_parquet_file(
    n: int, b: int, nested=False, compression=COMPRESSION_ALGORITHMS[0]
) -> None:
    """
    Generates a nested/flat parquet file with name
    "nested.parquet"/"flat.parquet"with n rows and
    b nested rows

    Args
        n (int): Overall rows
        b (int): Nested rows
        nested (bool): Turn on nested or defaults to flat

    Returns:
        Nothing
    """

    nf = generate_data(n, b, seed=1)[["nested"]]
    if not nested:
        nf["nested"].to_lists().to_parquet(  # type: ignore
            "flat.parquet",
            compression=compression,
        )
    else:
        nf.to_parquet(
            "nested.parquet",
            compression=compression,
        )


def clean_parquet_file() -> None:
    """
    Deletes the file produced by `generate_nested_file` function
    """

    try:
        os.remove("flat.parquet")
    except OSError:
        pass

    try:
        os.remove("nested.parquet")
    except OSError:
        pass


def load_file_in_ram(file_name: str) -> None:
    """
    Loads the file into RAM to avoid I/O delay.

    Args:
        file_name (str): The file to load into RAM

    Returns:
        Nothing
    """
    subprocess.run(["vmtouch", "-l", "-d", "-w", "-P", "vmtouch.pid", f"{file_name}"])


def unload_file_in_ram() -> None:
    """
    Unloads the file loaded by the `load_file_in_ram` function.

    Args:
        None

    Returns:
        Nothing
    """
    subprocess.run("kill $(cat vmtouch.pid)", shell=True)


EVICT = False


def read_file(file_name: str, multi: bool = False) -> list[float]:
    """
    Read the given file and return the `timeit` result.

    Args:
        file_name (str): Name of the file to read

    Returns:
        Timeit object: results of reading the file
    """

    code = f"pa.parquet.read_table('{file_name}', use_threads={multi})"

    if EVICT is True:
        list_of_times = []
        for _ in range(RUNS):
            list_of_times.extend(
                timeit.repeat(
                    stmt=code, setup="import pyarrow as pa", repeat=1, number=1
                )
            )
            subprocess.run(["vmtouch", "-e", f"{file_name}"])
        return list_of_times

    list_of_times = timeit.repeat(
        stmt=code, setup="import pyarrow as pa", repeat=RUNS, number=1
    )

    return list_of_times


def save_to_db(data: list) -> None:
    """
    Saves the data to a sqtlie3 database
    """
    global CONFIG
    if os.getenv("PYARROW_RUN_ALL") == "1":
        CONFIG = "ALL"

    con = sqlite3.connect("benchmarks.db")
    cur = con.cursor()

    # To create a table
    create_table_query = """
    CREATE TABLE IF NOT EXISTS times(
        file_loc TEXT,
        branch TEXT,
        file_order INTEGER,
        n INTEGER,
        b INTEGER,
        nested INTEGER,
        multi INTEGER,
        compression TEXT,
        time REAL
    )
    """
    cur.execute(create_table_query)

    insert_table_query = Template("""INSERT INTO times VALUES
    ($file_loc, $branch, $f, $n, $b, $nest, $multi, $comp, $t)
    """)

    for d in data:
        for t in d[8]:
            nest_val = 1 if d[5] else 0
            mult_val = 1 if d[6] else 0
            cur.execute(
                insert_table_query.substitute(
                    {
                        "file_order": d[0],
                        "branch": d[1],
                        "f": d[2],
                        "n": d[3],
                        "b": d[4],
                        "nest": nest_val,
                        "multi": mult_val,
                        "comp": f"'{d[7]}'",
                        "t": t,
                    }
                )
            )

    con.commit()


def demo_workflow():
    generate_parquet_file(100, 10000, False, compression=COMPRESSION_ALGORITHMS[0])
    load_file_in_ram("flat.parquet")
    read_file("flat.parquet", multi=False)
    unload_file_in_ram()
    clean_parquet_file()


if __name__ == "__main__":
    """
    Orchestrates a complete benchmark for the matrix
    [Normal PyArrow, Updated PyArrow] x [single thread, multi thread]
    """

    # demo_workflow()
    # Just an identifier used to name the database file
    PYARROW_VERSION = sys.argv[1]
    branch = PYARROW_VERSION

    # DEBUG
    # print(PYARROW_VERSION)

    read_config()

    nested_config = []
    if NESTED is True:
        nested_config.append(True)
    else:
        nested_config.append(False)

    multi_config = []
    if MULTI is True:
        multi_config.append(True)
    else:
        multi_config.append(False)

    data = []
    compressions = []
    if COMPRESSION == -1:
        compressions = COMPRESSION_ALGORITHMS
    else:
        compressions.append(COMPRESSION_ALGORITHMS[COMPRESSION])

    file_location = []

    if os.getenv("PYARROW_RUN_ALL") == "1":
        FILE_ORDER_END = 7
        FILE_ORDER_START = 1
        compressions = COMPRESSION_ALGORITHMS
        nested_config = [True, False]
        multi_config = [True, False]
        file_location = ["RAM", "SSD"]

    for current_compression in compressions:
        for file_order in range(FILE_ORDER_START, FILE_ORDER_END + 1):
            for N in range(file_order):
                B = file_order - N
                n = 10**N
                b = 10**B
                for nested in nested_config:
                    generate_parquet_file(
                        n, b, nested=nested, compression=current_compression
                    )
                    file_name = "nested.parquet" if nested else "flat.parquet"
                    for file_loc in file_location:
                        if file_loc == "RAM":
                            EVICT = False
                            load_file_in_ram(file_name)
                        else:
                            EVICT = True
                        for multi in multi_config:
                            times = read_file(file_name, multi)
                            data.append(
                                [
                                    file_loc,
                                    branch,
                                    file_order,
                                    n,
                                    b,
                                    nested,
                                    multi,
                                    current_compression,
                                    times,
                                ]
                            )
                        if EVICT is False:
                            unload_file_in_ram()
                    clean_parquet_file()

    save_to_db(data)
