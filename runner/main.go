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

func main() {
	fmt.Println("PyArrow helper - GSoC 26")

	// Run Repo Setup
	if err := repoHandler.Setup("https://github.com/OmBiradar/arrow.git", "runner_files/arrow"); err != nil {
		fmt.Printf("Failed to setup the arrow repository: %v", err)
	}

	branch := os.Getenv("BRANCH")

	switch branch {
	case "multi":
		branch = "parquet-reader-multithreaded-for-list-struct"
	case "select":
		branch = "select-struct-field-in-list"
	}

	fmt.Printf("The selected branch is : %v", branch)

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

	if err := pythonHandler.Run("./test_pyarrow.py"); err != nil {
		fmt.Printf("Python Script Failed: %v\n", err)
		return
	}

	notebooksDir := "../notebook"
	if err := os.MkdirAll(notebooksDir, 0o755); err != nil {
		fmt.Printf("Could not create the notebook directory: %v", err)
		return
	}

	packages := []string{"nested-pandas", "matplotlib"}
	if err := pythonHandler.InstallPackages(packages...); err != nil {
		fmt.Printf("Error installing packages %v: %v", packages, err)
	}

	// Start Jupyter (This will block execution until you stop the server)
	if err := pythonHandler.StartJupyter(notebooksDir); err != nil {
		fmt.Printf("Jupyter Server Error: %v\n", err)
		return
	}
}
