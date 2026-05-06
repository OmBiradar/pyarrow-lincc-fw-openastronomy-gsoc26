package main

import (
	"log/slog"
	"time"
)

type ArrowManager struct {
	Repo string
	Branch string
	Path string
	Downloaded bool
}

type ScriptRunner struct {
	EnvVars []string
}

type BenchmarkResult struct {
	ExecutionTimes []time.Duration
	Errors []error
}

type TargetScript struct {
	Name string
	Path string
	arrow* ArrowManager
	Runner ScriptRunner
	Benchmark BenchmarkResult
}


func main() {
	slog.Info("Orchestrator has started.");

	// TODO: Read from yaml of form - "Script name, path, arrow repo + branch"
	
	slog.Info("Orchestrator has stopped.");
}