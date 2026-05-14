#!/usr/bin/env python3

import subprocess
import sys
import time
from pathlib import Path
import yaml


def run_command(cmd, cwd=None, env=None):
    """Executes a shell command with real-time output stream."""
    print(f"==> Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=cwd, env=env, text=True)
    if result.returncode != 0:
        print(f"Error: Command failed with exit code {result.returncode}")
        sys.exit(1)


def setup_arrow_branch(arrow_repo_dir, branch):
    """Fetches and checks out the specified branch."""
    print(f"\n==> Checking out Arrow branch: {branch}")
    subprocess.run(
        ["git", "fetch", "origin", branch], cwd=arrow_repo_dir, capture_output=True
    )
    run_command(["git", "checkout", branch], cwd=arrow_repo_dir)


def build_arrow(arrow_cpp_dir, build_dir):
    """Configures and compiles Arrow C++."""
    build_dir.mkdir(parents=True, exist_ok=True)
    cmake_cmd = [
        "cmake",
        str(arrow_cpp_dir),
        "-G",
        "Ninja",
        "-DCMAKE_BUILD_TYPE=Debug",
        "-DARROW_USE_CCACHE=ON",
        "-DARROW_BUILD_BENCHMARKS=ON",
        "-DARROW_COMPUTE=ON",
        "-DARROW_DATASET=ON",
        "-DARROW_PARQUET=ON",
    ]
    print("==> Configuring CMake")
    run_command(cmake_cmd, cwd=build_dir)
    print("==> Compiling Arrow with Ninja")
    run_command(["ninja"], cwd=build_dir)


def compile_and_run_test(
    test_dir, test_name, arrow_cpp_dir, build_dir, run_type="baseline"
):
    """Compiles and executes the test, returning the execution time in seconds."""
    test_cpp = test_dir / "test.cpp"
    test_bin = test_dir / f"{test_name}_{run_type}.out"

    include_dir_src = arrow_cpp_dir / "src"
    include_dir_build = build_dir / "src"
    lib_dir = build_dir / "debug"

    print(f"==> Compiling test executable: {test_bin.name}")
    compile_cmd = [
        "g++",
        "-std=c++17",
        "-g",
        "-O0",
        "-I",
        str(include_dir_src),
        "-I",
        str(include_dir_build),
        str(test_cpp),
        "-o",
        str(test_bin),
        "-L",
        str(lib_dir),
        "-larrow",
        "-larrow_dataset",
        "-lparquet",
        f"-Wl,-rpath,{lib_dir}",
    ]
    run_command(compile_cmd, cwd=test_dir)

    print(f"==> Executing test: {test_bin.name}")
    start_time = time.time()
    run_command([str(test_bin)], cwd=test_dir)
    end_time = time.time()

    execution_time = end_time - start_time
    print(f"==> Execution finished in {execution_time:.4f} seconds")
    return execution_time


def main():
    root_dir = Path.cwd()
    scripts_dir = root_dir / "scripts"
    arrow_src_dir = root_dir / "arrow-src"
    arrow_cpp_dir = arrow_src_dir / "cpp"
    build_dir = root_dir / "build"

    if not arrow_cpp_dir.exists():
        print(f"Error: Arrow C++ directory not found at {arrow_cpp_dir}")
        sys.exit(1)

    global_config_file = scripts_dir / "config.yml"
    with open(global_config_file, "r") as f:
        global_config = yaml.safe_load(f)
    base_branch = global_config.get("base_branch", "main")

    valid_tests = []
    for test_dir in sorted(scripts_dir.iterdir()):
        if (
            test_dir.is_dir()
            and (test_dir / "config.yml").exists()
            and (test_dir / "test.cpp").exists()
        ):
            valid_tests.append(test_dir)

    # Dictionary to hold the execution times
    results = {
        test.name: {"baseline_time": None, "feature_time": None, "branch": None}
        for test in valid_tests
    }

    # ==========================================
    # PHASE 1: Baseline Execution
    # ==========================================
    print("\n" + "=" * 50)
    print(f" PHASE 1: Building and Running Baseline ({base_branch})")
    print("=" * 50)

    setup_arrow_branch(arrow_src_dir, base_branch)
    build_arrow(arrow_cpp_dir, build_dir)

    for test_dir in valid_tests:
        print(f"\n--- Baseline Run: {test_dir.name} ---")
        duration = compile_and_run_test(
            test_dir, test_dir.name, arrow_cpp_dir, build_dir, run_type="baseline"
        )
        results[test_dir.name]["baseline_time"] = duration

    # ==========================================
    # PHASE 2: Feature Branch Execution
    # ==========================================
    print("\n" + "=" * 50)
    print(" PHASE 2: Building and Running Feature Branches")
    print("=" * 50)

    for test_dir in valid_tests:
        test_name = test_dir.name
        with open(test_dir / "config.yml", "r") as f:
            test_config = yaml.safe_load(f)

        feature_branch = test_config.get("branch")
        results[test_name]["branch"] = feature_branch

        if feature_branch == base_branch:
            results[test_name]["feature_time"] = "Skipped (Same as base)"
            continue

        print(f"\n--- Feature Run: {test_name} ---")
        setup_arrow_branch(arrow_src_dir, feature_branch)
        build_arrow(arrow_cpp_dir, build_dir)
        duration = compile_and_run_test(
            test_dir, test_name, arrow_cpp_dir, build_dir, run_type="feature"
        )
        results[test_name]["feature_time"] = duration

    # ==========================================
    # PHASE 3: Generate Markdown Report
    # ==========================================
    report_lines = [
        f"| Script | Baseline (`{base_branch}`) | Modified Branch | Branch Name |",
        "|---|---|---|---|",
    ]

    for test, data in results.items():
        # Format baseline time
        b_time = (
            f"{data['baseline_time']:.4f}s"
            if isinstance(data["baseline_time"], float)
            else "N/A"
        )

        # Format feature time
        if isinstance(data["feature_time"], float):
            f_time = f"{data['feature_time']:.4f}s"
        else:
            f_time = str(data["feature_time"])  # Handles "Skipped" text

        branch_name = data["branch"] or "N/A"

        report_lines.append(f"| `{test}` | {b_time} | {f_time} | `{branch_name}` |")

    # Write the table to a file that GitHub Actions can read
    with open(root_dir / "benchmark_results.md", "w") as f:
        f.write("\n".join(report_lines) + "\n")

    print("\n==> Benchmark complete. Results written to benchmark_results.md")


if __name__ == "__main__":
    main()
