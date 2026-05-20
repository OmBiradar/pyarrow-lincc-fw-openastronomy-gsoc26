import sys

print(f"Using Python executable: {sys.executable}")

try:
    import pyarrow as pa
except ImportError as e:
    print(f"❌ Failed to import pyarrow: {e}")
    sys.exit(1)

print("\n=== PyArrow Installation Info ===")
print(f"PyArrow Version:     {pa.__version__}")
print(f"C++ Arrow Version:   {pa.cpp_version}")

print("\n=== C++ Compiler Info ===")
try:
    build_info = pa.cpp_build_info
    print(f"Compiler ID:         {build_info.compiler_id}")
    print(f"Compiler Version:    {build_info.compiler_version}")
    print(f"Compiler Flags:      {build_info.compiler_flags}")
except AttributeError:
    print("Could not retrieve C++ build info.")

print("\n=== Module Check ===")
try:
    import pyarrow.parquet
    import pyarrow.dataset
    import pyarrow.compute

    print("✅ Parquet, Dataset, and Compute modules loaded successfully!")
except ImportError as e:
    print(f"❌ Module missing: {e}")
