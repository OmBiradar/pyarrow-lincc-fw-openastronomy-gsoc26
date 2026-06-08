package main

import (
	"fmt"
	"os"
)

// Global object holders
var (
	cppHandler    CppHandler
	pythonHandler PythonHandler
	repoHandler   RepoHandler
)

func runWorkflow(branch string, db string) {
	// Run Repo Setup
	if err := repoHandler.Setup("https://github.com/OmBiradar/arrow.git", "runner_files/arrow"); err != nil {
		fmt.Printf("Failed to setup the arrow repository: %v", err)
	}

	if err := repoHandler.Get(branch); err != nil {
		fmt.Printf("Failed to checkout to branch %v: %v", branch, err)
	}

	// Run C++ setup
	if err := cppHandler.Setup(); err != nil {
		fmt.Printf("Failed to setup C++ build: %v\n", err)
		return
	}

	// Run PyArrow setup
	if err := pythonHandler.Setup(); err != nil {
		fmt.Printf("Failed to setup PyArrow build: %v\n", err)
	}

	packages := []string{"nested-pandas"}
	if err := pythonHandler.InstallPackages(packages...); err != nil {
		fmt.Printf("Error installing packages %v: %v", packages, err)
	}

	// Test PyArrow
	if err := pythonHandler.Run("./test_pyarrow.py"); err != nil {
		fmt.Printf("Python Script Failed: %v\n", err)
		return
	}

	// Run PyArrow Benchmark
	if err := pythonHandler.Run("../scripts/nested-parquet-reading/python.py", db); err != nil {
		fmt.Printf("Python Script failed: %v\n", err)
	}
}

func main() {
	fmt.Println("PyArrow helper - GSoC 26")

	branch := os.Getenv("PYARROW_OPENASTRONOMY_BENCHMARK_BRANCH")

	switch branch {
	case "OPTIMIZED":
		runWorkflow("parquet-reader-multithreaded-for-list-struct", "OPTIMIZED")
	case "MAIN":
		runWorkflow("main", "main")
	}

	// notebooksDir := "../notebook"
	// if err := os.MkdirAll(notebooksDir, 0o755); err != nil {
	// 	fmt.Printf("Could not create the notebook directory: %v", err)
	// 	return
	// }

	// Start Jupyter
	// if err := pythonHandler.StartJupyter(notebooksDir); err != nil {
	// 	fmt.Printf("Jupyter Server Error: %v\n", err)
	// 	return
	// }
}
