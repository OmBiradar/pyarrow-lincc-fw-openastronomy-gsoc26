#!/usr/bin/env python3

import subprocess
import sys
import time
import statistics
from pathlib import Path
import yaml

NUM_RUNS = 100


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


def compile_test(test_dir, test_name, arrow_cpp_dir, build_dir, run_type="baseline"):
    """Compiles the test binary only (without running it)."""
    test_cpp = test_dir / "test.cpp"
    test_bin = test_dir / f"{test_name}_{run_type}.out"

    include_dir_src = arrow_cpp_dir / "src"
    include_dir_build = build_dir / "src"
    lib_dir = build_dir / "debug"

    print(f"==> Compiling test executable: {test_bin.name}")
    compile_cmd = [
        "g++",
        "-std=c++23",
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
    return test_bin


def run_test_n_times(test_bin, test_dir, n=NUM_RUNS):
    """Runs the compiled test binary n times, returning list of execution times."""
    times = []
    for i in range(1, n + 1):
        print(f"==> Run {i}/{n}: {test_bin.name}")
        start_time = time.time()
        run_command([str(test_bin)], cwd=test_dir)
        end_time = time.time()
        elapsed = end_time - start_time
        times.append(elapsed)
        print(f"    Finished in {elapsed:.4f}s")
    return times


def format_time_stat(mean_s, stddev_s):
    """Formats mean and stddev as human-readable strings.
    Mean: e.g. '4.0000s'
    Stddev: e.g. 'σ 0.0500s (1.25%)'
    """
    mean_str = f"{mean_s:.4f}s"
    pct = (stddev_s / mean_s * 100) if mean_s > 0 else 0.0
    stddev_str = f"σ {stddev_s:.4f}s ({pct:.2f}%)"
    return mean_str, stddev_str


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

    # Results: mean and stddev for baseline and feature
    results = {
        test.name: {
            "baseline_mean": None,
            "baseline_stddev": None,
            "feature_mean": None,
            "feature_stddev": None,
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
    print(f"          Each test will run {NUM_RUNS} times")
    print("=" * 50)

    setup_arrow_branch(arrow_src_dir, base_branch)
    build_arrow(arrow_cpp_dir, build_dir)

    for test_dir in valid_tests:
        print(f"\n--- Baseline Runs: {test_dir.name} ---")
        test_bin = compile_test(
            test_dir, test_dir.name, arrow_cpp_dir, build_dir, run_type="baseline"
        )
        times = run_test_n_times(test_bin, test_dir, n=NUM_RUNS)
        results[test_dir.name]["baseline_mean"] = statistics.mean(times)
        results[test_dir.name]["baseline_stddev"] = (
            statistics.stdev(times) if len(times) > 1 else 0.0
        )
        print(
            f"==> Baseline complete: mean={results[test_dir.name]['baseline_mean']:.4f}s, "
            f"σ={results[test_dir.name]['baseline_stddev']:.4f}s"
        )

    # ==========================================
    # PHASE 2: Feature Branch Execution
    # ==========================================
    print("\n" + "=" * 50)
    print(" PHASE 2: Building and Running Feature Branches")
    print(f"          Each test will run {NUM_RUNS} times")
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

        print(f"\n--- Feature Runs: {test_name} ---")
        setup_arrow_branch(arrow_src_dir, feature_branch)
        build_arrow(arrow_cpp_dir, build_dir)
        test_bin = compile_test(
            test_dir, test_name, arrow_cpp_dir, build_dir, run_type="feature"
        )
        times = run_test_n_times(test_bin, test_dir, n=NUM_RUNS)
        results[test_name]["feature_mean"] = statistics.mean(times)
        results[test_name]["feature_stddev"] = (
            statistics.stdev(times) if len(times) > 1 else 0.0
        )
        print(
            f"==> Feature complete: mean={results[test_name]['feature_mean']:.4f}s, "
            f"σ={results[test_name]['feature_stddev']:.4f}s"
        )

    # ==========================================
    # PHASE 3: Generate Markdown Report
    # ==========================================
    report_lines = [
        f"| Script | Baseline Mean (`{base_branch}`) | Baseline σ | Modified Mean | Modified σ | Branch |",
        "|---|---|---|---|---|---|",
    ]

    for test, data in results.items():
        # Baseline columns
        if data["baseline_mean"] is not None:
            b_mean_str, b_stddev_str = format_time_stat(
                data["baseline_mean"], data["baseline_stddev"]
            )
        else:
            b_mean_str, b_stddev_str = "N/A", "N/A"

        # Feature columns
        if data["skipped"]:
            f_mean_str = "Skipped (same as base)"
            f_stddev_str = "—"
        elif data["feature_mean"] is not None:
            f_mean_str, f_stddev_str = format_time_stat(
                data["feature_mean"], data["feature_stddev"]
            )
        else:
            f_mean_str, f_stddev_str = "N/A", "N/A"

        branch_name = data["branch"] or "N/A"

        report_lines.append(
            f"| `{test}` | {b_mean_str} | {b_stddev_str} | {f_mean_str} | {f_stddev_str} | `{branch_name}` |"
        )

    with open(root_dir / "benchmark_results.md", "w") as f:
        f.write("\n".join(report_lines) + "\n")

    print("\n==> Benchmark complete. Results written to benchmark_results.md")


if __name__ == "__main__":
    main()
