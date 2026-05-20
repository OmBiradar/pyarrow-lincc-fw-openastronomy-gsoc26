package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

type CppHandler struct {
	SourceDir  string
	BuildDir   string
	InstallDir string
}

func (b *CppHandler) runCmd(name string, args ...string) error {
	cmd := exec.Command(name, args...)

	cmd.Dir = b.BuildDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	arrowHome, err := filepath.Abs(b.InstallDir)
	if err != nil {
		return fmt.Errorf("failed to resolve absolute install path: %w", err)
	}

	cmd.Env = os.Environ()

	cmd.Env = append(cmd.Env, fmt.Sprintf("ARROW_HOME=%s", arrowHome))
	cmd.Env = append(cmd.Env, fmt.Sprintf("LD_LIBRARY_PATH=%s/lib:%s", arrowHome, os.Getenv("LD_LIBRARY_PATH")))
	cmd.Env = append(cmd.Env, fmt.Sprintf("CMAKE_PREFIX_PATH=%s:%s", arrowHome, os.Getenv("CMAKE_PREFIX_PATH")))

	return cmd.Run()
}

func (b *CppHandler) Clean() error {
	fmt.Printf("🧹 Cleaning build directory: %s\n", b.BuildDir)
	return os.RemoveAll(b.BuildDir)
}

func (b *CppHandler) Configure() error {
	fmt.Println("⚙️  Configuring CMake...")

	if err := os.MkdirAll(b.BuildDir, 0o755); err != nil {
		return fmt.Errorf("failed to create build dir: %w", err)
	}
	args := []string{
		"-GNinja",
		"-S", b.SourceDir,
		"-B", b.BuildDir,
		"-DCMAKE_INSTALL_PREFIX=" + b.InstallDir,

		// Release mode
		"-DCMAKE_BUILD_TYPE=Release",

		// optimizations
		"-DCMAKE_C_FLAGS=-O3 -march=native",
		"-DCMAKE_CXX_FLAGS=-O3 -march=native",

		"-DCMAKE_C_COMPILER_LAUNCHER=ccache",
		"-DCMAKE_CXX_COMPILER_LAUNCHER=ccache",

		// High-performance memory allocators
		"-DARROW_JEMALLOC=ON",
		"-DARROW_MIMALLOC=ON",

		// other components
		"-DARROW_COMPUTE=ON",
		"-DARROW_CSV=ON",
		"-DARROW_DATASET=ON",
		"-DARROW_FILESYSTEM=ON",
		"-DARROW_JSON=ON",
		"-DARROW_PARQUET=ON",

		// Compressions
		"-DARROW_WITH_SNAPPY=ON",
		"-DARROW_WITH_ZSTD=ON", // default for astronomy files lvl 15
		"-DARROW_WITH_LZ4=ON",
		"-DARROW_WITH_ZLIB=ON",
		"-DARROW_WITH_BROTLI=ON",
		"-DARROW_WITH_BZ2=ON",

		"-DCMAKE_POLICY_VERSION_MINIMUM=3.5",
	}

	return b.runCmd("cmake", args...)
}

func (b *CppHandler) BuildAndInstall() error {
	fmt.Println("🔨 Compiling and installing with Ninja...")
	return b.runCmd("ninja", "install")
}

func (b *CppHandler) Rebuild() error {
	fmt.Println("🚀 Starting full rebuild process...")

	if err := b.Clean(); err != nil {
		return fmt.Errorf("clean stage failed: %w", err)
	}

	if err := b.Configure(); err != nil {
		return fmt.Errorf("configure stage failed: %w", err)
	}

	if err := b.BuildAndInstall(); err != nil {
		return fmt.Errorf("build stage failed: %w", err)
	}

	fmt.Println("✅ Rebuild complete! Binaries installed to:", b.InstallDir)
	return nil
}

func (b *CppHandler) Setup() error {
	var err error

	b.SourceDir, err = filepath.Abs("runner_files/arrow/cpp")
	if err != nil {
		return err
	}

	b.BuildDir, err = filepath.Abs("runner_files/cpp_build")
	if err != nil {
		return err
	}

	b.InstallDir, err = filepath.Abs("runner_files/install")
	if err != nil {
		return err
	}

	return b.Rebuild()
}
