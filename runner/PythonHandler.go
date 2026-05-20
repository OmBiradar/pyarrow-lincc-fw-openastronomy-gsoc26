package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

type PythonHandler struct {
	PythonSrcDir string
	VenvDir      string
	ArrowHome    string
}

func (p *PythonHandler) runCmd(dir string, env []string, name string, args ...string) error {
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	cmd.Env = os.Environ()
	if len(env) > 0 {
		cmd.Env = append(cmd.Env, env...)
	}

	return cmd.Run()
}

func (p *PythonHandler) Setup() error {
	var err error

	p.PythonSrcDir, err = filepath.Abs("runner_files/arrow/python")
	if err != nil {
		return fmt.Errorf("failed to resolve python source path: %w", err)
	}
	p.VenvDir, err = filepath.Abs("runner_files/venv")
	if err != nil {
		return fmt.Errorf("failed to resolve venv path: %w", err)
	}
	p.ArrowHome, err = filepath.Abs("runner_files/install")
	if err != nil {
		return fmt.Errorf("failed to resolve arrow install path: %w", err)
	}

	fmt.Printf("🐍 Creating Python virtual environment at: %s\n", p.VenvDir)
	if err := p.runCmd(".", nil, "python3", "-m", "venv", p.VenvDir); err != nil {
		return fmt.Errorf("failed to create venv: %w", err)
	}

	pipExe := filepath.Join(p.VenvDir, "bin", "pip")

	fmt.Println("📦 Installing PyArrow build dependencies...")
	reqFile := filepath.Join(p.PythonSrcDir, "requirements-build.txt")
	if err := p.runCmd(p.PythonSrcDir, nil, pipExe, "install", "-r", reqFile); err != nil {
		return fmt.Errorf("failed to install build requirements: %w", err)
	}

	fmt.Println("🚀 Building PyArrow against custom C++ binaries...")

	envVars := []string{
		fmt.Sprintf("ARROW_HOME=%s", p.ArrowHome),
		fmt.Sprintf("CMAKE_PREFIX_PATH=%s", p.ArrowHome),
		fmt.Sprintf("LD_LIBRARY_PATH=%s/lib:%s", p.ArrowHome, os.Getenv("LD_LIBRARY_PATH")),

		"PYARROW_WITH_COMPUTE=1",
		"PYARROW_WITH_DATASET=1",
		"PYARROW_WITH_PARQUET=1",
	}

	if err := p.runCmd(p.PythonSrcDir, envVars, pipExe, "install", "-v", "-e", "."); err != nil {
		return fmt.Errorf("failed to build and install PyArrow: %w", err)
	}

	fmt.Println("✅ PyArrow successfully installed in the virtual environment!")
	return nil
}

func (p *PythonHandler) Run(scriptPath string) error {
	if p.VenvDir == "" || p.ArrowHome == "" {
		return fmt.Errorf("handler not initialized: run Setup() before Run()")
	}

	absScriptPath, err := filepath.Abs(scriptPath)
	if err != nil {
		return fmt.Errorf("failed to resolve script path: %w", err)
	}

	fmt.Printf("▶️ Running Python script: %s\n", absScriptPath)

	pythonExe := filepath.Join(p.VenvDir, "bin", "python")

	envVars := []string{
		fmt.Sprintf("LD_LIBRARY_PATH=%s/lib:%s", p.ArrowHome, os.Getenv("LD_LIBRARY_PATH")),
	}

	scriptDir := filepath.Dir(absScriptPath)

	if err := p.runCmd(scriptDir, envVars, pythonExe, absScriptPath); err != nil {
		return fmt.Errorf("script execution failed: %w", err)
	}

	fmt.Println("✅ Script execution complete.")
	return nil
}

func (p *PythonHandler) StartJupyter(workspaceDir string) error {
	if p.VenvDir == "" || p.ArrowHome == "" {
		return fmt.Errorf("handler not initialized: run Setup() before StartJupyter()")
	}

	absWorkspace, err := filepath.Abs(workspaceDir)
	if err != nil {
		return fmt.Errorf("failed to resolve workspace path: %w", err)
	}

	pipExe := filepath.Join(p.VenvDir, "bin", "pip")
	jupyterExe := filepath.Join(p.VenvDir, "bin", "jupyter")

	fmt.Println("📦 Installing JupyterLab (this might take a moment)...")
	if err := p.runCmd(p.VenvDir, nil, pipExe, "install", "jupyterlab"); err != nil {
		return fmt.Errorf("failed to install JupyterLab: %w", err)
	}

	envVars := []string{
		fmt.Sprintf("LD_LIBRARY_PATH=%s/lib:%s", p.ArrowHome, os.Getenv("LD_LIBRARY_PATH")),
	}

	fmt.Printf("🚀 Starting JupyterLab in %s...\n", absWorkspace)
	fmt.Println("⚠️  Press Ctrl+C in this terminal to shut down the server.")

	if err := p.runCmd(absWorkspace, envVars, jupyterExe, "lab"); err != nil {
		return fmt.Errorf("jupyter server exited with error: %w", err)
	}

	return nil
}

func (p *PythonHandler) InstallPackages(packages ...string) error {
	if p.VenvDir == "" {
		return fmt.Errorf("handler not initialized: run Setup() before InstallPackages()")
	}
	if len(packages) == 0 {
		return fmt.Errorf("no packages provided to install")
	}

	pipExe := filepath.Join(p.VenvDir, "bin", "pip")

	args := []string{"install"}
	args = append(args, packages...)

	fmt.Printf("📦 Installing additional packages: %v...\n", packages)

	if err := p.runCmd(p.VenvDir, nil, pipExe, args...); err != nil {
		return fmt.Errorf("failed to install packages %v: %w", packages, err)
	}

	fmt.Println("✅ Packages successfully installed!")
	return nil
}
