package main

import (
	"fmt"
)

// Global object holders
var (
	cppHandler    CppHandler
	pythonHandler PythonHandler
	repoHandler   RepoHandler
)

func main() {
	fmt.Println("PyArrow helper - GSoC 26")

	// Run Repo Setup
	if err := repoHandler.Setup("https://github.com/OmBiradar/arrow.git", "runner_files/arrow"); err != nil {
		fmt.Printf("Failed to setup the arrow repository: %v", err)
	}

	branch := "parquet-reader-multithreaded-for-list-struct"

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

	packages := []string{"nested-pandas", "matplotlib"}
	if err := pythonHandler.InstallPackages(packages...); err != nil {
		fmt.Printf("Error installing packages %v: %v", packages, err)
	}

	// Test PyArrow
	if err := pythonHandler.Run("./test_pyarrow.py"); err != nil {
		fmt.Printf("Python Script Failed: %v\n", err)
		return
	}

	// Run PyArrow
	if err := pythonHandler.Run("../scripts/py-parquet-reader/run.py", "multi"); err != nil {
		fmt.Printf("Python Script failed: %v\n", err)
	}

	// The normal branch

	branch = "main"

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

	if err := pythonHandler.InstallPackages(packages...); err != nil {
		fmt.Printf("Error installing packages %v: %v", packages, err)
	}

	// Test PyArrow
	if err := pythonHandler.Run("./test_pyarrow.py"); err != nil {
		fmt.Printf("Python Script Failed: %v\n", err)
		return
	}

	// Run PyArrow
	if err := pythonHandler.Run("../scripts/py-parquet-reader/run.py", "main"); err != nil {
		fmt.Printf("Python Script failed: %v\n", err)
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
