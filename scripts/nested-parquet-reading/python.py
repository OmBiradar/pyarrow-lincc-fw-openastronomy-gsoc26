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

from nested_pandas.datasets import generate_data
import os
import subprocess
import pyarrow as pa


def generate_parquet_file(n: int, b: int, nested=False) -> None:
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
        nf["nested"].to_lists().to_parquet("flat.parquet")  # type: ignore
    else:
        nf.to_parquet("nested.parquet")


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


def read_file(file_name: str, multi: bool = False) -> None:
    """
    Read the given file and return the `timeit` result.

    Args:
        file_name (str): Name of the file to read

    Returns:
        Timeit object: results of reading the file
    """

    # TODO: Measure the time to execute this function
    pa.parquet.read_table(file_name, use_threads=multi)


def demo_workflow():
    generate_parquet_file(100, 10000, False)
    load_file_in_ram("flat.parquet")
    read_file("flat.parquet", multi=False)
    unload_file_in_ram()
    clean_parquet_file()


if __name__ == "__main__":
    demo_workflow()
    # TODO: Orchestrate a complete workflow for the matrix
    # [Normal PyArrow, Updated PyArrow] x [single thread, multi thread]
