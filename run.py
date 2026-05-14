#!/usr/bin/env python3

import subprocess
import sys
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
    """Fetches and checks out the specified branch from the same repository."""
    print(f"\n==> Checking out Arrow branch: {branch}")
    # Fetching ensures we have the latest remote refs if the branch isn't local yet
    subprocess.run(
        ["git", "fetch", "origin", branch], cwd=arrow_repo_dir, capture_output=True
    )
    run_command(["git", "checkout", branch], cwd=arrow_repo_dir)


def build_arrow(arrow_cpp_dir, build_dir):
    """Configures and compiles Arrow C++ using CMake and Ninja."""
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
    """Compiles the test.cpp against the local Arrow build and executes it."""
    test_cpp = test_dir / "test.cpp"
    # Differentiate the binary name based on whether it's a baseline or feature run
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
    run_command([str(test_bin)], cwd=test_dir)


def main():
    root_dir = Path.cwd()
    scripts_dir = root_dir / "scripts"
    arrow_src_dir = root_dir / "arrow-src"
    arrow_cpp_dir = arrow_src_dir / "cpp"
    build_dir = root_dir / "build"

    # Pre-flight checks
    if not arrow_cpp_dir.exists():
        print(f"Error: Arrow C++ directory not found at {arrow_cpp_dir}")
        sys.exit(1)

    global_config_file = scripts_dir / "config.yml"
    if not global_config_file.exists():
        print(
            f"Error: Global config not found at {global_config_file}. Needed for baseline branch."
        )
        sys.exit(1)

    with open(global_config_file, "r") as f:
        global_config = yaml.safe_load(f)

    base_branch = global_config.get("base_branch", "main")

    # Discover valid test directories
    valid_tests = []
    for test_dir in sorted(scripts_dir.iterdir()):
        if test_dir.is_dir():
            test_config = test_dir / "config.yml"
            test_cpp = test_dir / "test.cpp"
            if test_config.exists() and test_cpp.exists():
                valid_tests.append(test_dir)

    if not valid_tests:
        print("No valid test directories found.")
        sys.exit(0)

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
        compile_and_run_test(
            test_dir, test_dir.name, arrow_cpp_dir, build_dir, run_type="baseline"
        )

    # ==========================================
    # PHASE 2: Feature Branch Execution
    # ==========================================
    print("\n" + "=" * 50)
    print(" PHASE 2: Building and Running Feature Branches")
    print("=" * 50)

    for test_dir in valid_tests:
        test_name = test_dir.name
        print(f"\n--- Feature Run: {test_name} ---")

        with open(test_dir / "config.yml", "r") as f:
            test_config = yaml.safe_load(f)

        feature_branch = test_config.get("branch")
        if not feature_branch:
            print(f"Error: 'branch' not specified in {test_dir}/config.yml. Skipping.")
            continue

        # Skip if the feature branch is exactly the base branch (redundant)
        if feature_branch == base_branch:
            print(
                f"Feature branch matches base branch ({base_branch}). Skipping redundant build."
            )
            continue

        setup_arrow_branch(arrow_src_dir, feature_branch)
        build_arrow(arrow_cpp_dir, build_dir)
        compile_and_run_test(
            test_dir, test_name, arrow_cpp_dir, build_dir, run_type="feature"
        )

    # Optional: Return repository to baseline state at the end
    print("\n==> Restoring repository to baseline branch...")
    setup_arrow_branch(arrow_src_dir, base_branch)
    print("All tests completed successfully.")


if __name__ == "__main__":
    main()
