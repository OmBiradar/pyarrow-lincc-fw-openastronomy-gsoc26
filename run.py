#!/usr/bin/env python3

import subprocess
import sys
import time
from pathlib import Path
import yaml


def run_command(cmd, cwd=None, env=None):
    """Executes a shell command. Streams stdout in real-time and captures stderr for error reporting."""
    print(f"==> Running: {' '.join(cmd)}")

    process = subprocess.Popen(
        cmd, cwd=cwd, env=env, text=True, stdout=sys.stdout, stderr=subprocess.PIPE
    )

    _, stderr_output = process.communicate()

    if process.returncode != 0:
        print("\n" + "!" * 60)
        print(f" ERROR FATAL: Command failed with exit code {process.returncode}")
        print(f" DIRECTORY:   {cwd or 'Current Directory'}")
        print(f" COMMAND:     {' '.join(cmd)}")
        print("!" * 60)

        if stderr_output and stderr_output.strip():
            print("\n" + "-" * 20 + " ERROR DETAILS (stderr) " + "-" * 20)
            print(stderr_output)
            print("-" * 64 + "\n")
        else:
            print("\n(No output found in standard error stream)\n")

        sys.exit(1)

    if stderr_output and stderr_output.strip():
        sys.stderr.write(stderr_output)
        sys.stderr.flush()


def run_benchmark_and_capture(test_bin, test_dir):
    """Runs the Google Benchmark binary and captures its stdout."""
    print(f"==> Running Benchmark: {test_bin.name}")
    process = subprocess.run(
        [str(test_bin)], cwd=test_dir, text=True, capture_output=True
    )

    if process.returncode != 0:
        print(f"ERROR running {test_bin.name}:\n{process.stderr}")
        sys.exit(1)

    return process.stdout.strip()


def setup_arrow_branch(arrow_repo_dir, branch):
    """Fetches and checks out the specified branch."""
    print(f"\n==> Checking out Arrow branch: {branch}")
    run_command(["git", "fetch", "origin", branch], cwd=arrow_repo_dir)
    run_command(["git", "checkout", branch], cwd=arrow_repo_dir)


def build_arrow(arrow_cpp_dir, build_dir):
    """Configures and compiles Arrow C++."""
    build_dir.mkdir(parents=True, exist_ok=True)
    cmake_cmd = [
        "cmake",
        str(arrow_cpp_dir),
        "-G",
        "Ninja",
        "-DCMAKE_BUILD_TYPE=Release",
        "-DARROW_USE_CCACHE=ON",
        "-DARROW_BUILD_BENCHMARKS=ON",
        "-DARROW_COMPUTE=ON",
        "-DARROW_DATASET=ON",
        "-DARROW_PARQUET=ON",
        "-DARROW_WITH_SNAPPY=ON",
        "-DARROW_SIMD_LEVEL=AVX2",
        "-DARROW_RUNTIME_SIMD_LEVEL=MAX",
        "-DARROW_ENABLE_LTO=ON",
        "-DARROW_JEMALLOC=ON",
        "-DCMAKE_CXX_FLAGS=-march=native",
    ]
    print("==> Configuring CMake")
    run_command(cmake_cmd, cwd=build_dir)
    print("==> Compiling Arrow with Ninja")
    run_command(["ninja"], cwd=build_dir)


def compile_test(test_dir, test_name, arrow_cpp_dir, build_dir, run_type="baseline"):
    """Compiles the Google Benchmark test binary using g++."""
    test_cpp = test_dir / "test.cpp"
    test_bin = test_dir / f"{test_name}_{run_type}.out"

    include_dir_src = arrow_cpp_dir / "src"
    include_dir_build = build_dir / "src"
    lib_dir = build_dir / "release"

    print(f"==> Compiling test executable: {test_bin.name}")
    compile_cmd = [
        "g++",
        "-std=c++23",
        "-O3",
        "-I",
        str(include_dir_src),
        "-I",
        str(include_dir_build),
        str(test_cpp),
        "-o",
        str(test_bin),
        "-L",
        str(lib_dir),
        "-lparquet",
        "-larrow",
        "-lbenchmark",
        "-lpthread",
        f"-Wl,-rpath,{lib_dir}",
    ]
    run_command(compile_cmd, cwd=test_dir)
    return test_bin


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

    results = {
        test.name: {
            "baseline_output": None,
            "feature_output": None,
            "branch": None,
            "skipped": False,
        }
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
        test_bin = compile_test(
            test_dir, test_dir.name, arrow_cpp_dir, build_dir, run_type="baseline"
        )
        output = run_benchmark_and_capture(test_bin, test_dir)
        results[test_dir.name]["baseline_output"] = output
        print("==> Baseline benchmark captured.")

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
            results[test_name]["skipped"] = True
            continue

        print(f"\n--- Feature Run: {test_name} ({feature_branch}) ---")
        setup_arrow_branch(arrow_src_dir, feature_branch)
        build_arrow(arrow_cpp_dir, build_dir)

        test_bin = compile_test(
            test_dir, test_name, arrow_cpp_dir, build_dir, run_type="feature"
        )
        output = run_benchmark_and_capture(test_bin, test_dir)
        results[test_name]["feature_output"] = output
        print("==> Feature benchmark captured.")

    # ==========================================
    # PHASE 3: Generate Markdown Report
    # ==========================================
    report_lines = []

    for test, data in results.items():
        report_lines.append(f"### 📊 `{test}`")

        # Baseline
        report_lines.append(f"**Baseline (`{base_branch}`)**")
        report_lines.append("```text")
        report_lines.append(
            data["baseline_output"] if data["baseline_output"] else "N/A"
        )
        report_lines.append("```")

        # Feature
        branch_name = data["branch"] or "Unknown"
        report_lines.append(f"**Feature (`{branch_name}`)**")
        if data["skipped"]:
            report_lines.append("_Skipped (Feature branch matches baseline branch)_")
        else:
            report_lines.append("```text")
            report_lines.append(
                data["feature_output"] if data["feature_output"] else "N/A"
            )
            report_lines.append("```")

        report_lines.append("---\n")

    with open(root_dir / "benchmark_results.md", "w") as f:
        f.write("\n".join(report_lines) + "\n")

    print("\n==> Benchmark complete. Results written to benchmark_results.md")


if __name__ == "__main__":
    main()
