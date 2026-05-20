package main

import (
	"fmt"
	"os"
	"os/exec"
)

type RepoHandler struct {
	repoURL  string
	branch   string
	localDir string
}

func (r *RepoHandler) Setup(repoURL string, localDir string) error {
	r.repoURL = repoURL
	r.branch = "main" // Default
	r.localDir = localDir

	if _, err := os.Stat(r.localDir); !os.IsNotExist(err) {
		fmt.Printf("Repository already exists at %s. Updating...\n", r.localDir)

		return r.Get("main")
	}

	fmt.Printf("Cloning %s into %s...\n", r.repoURL, r.localDir)
	cmd := exec.Command("git", "clone", r.repoURL, r.localDir)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to clone repository: %w", err)
	}

	return nil
}

func (r *RepoHandler) Get(branch string) error {
	r.branch = branch

	fmt.Printf("Switching to and pulling branch: %s\n", r.branch)

	commands := [][]string{
		{"fetch", "origin"},
		{"checkout", r.branch},
		{"pull", "origin", r.branch},
	}

	for _, args := range commands {
		cmd := exec.Command("git", args...)
		cmd.Dir = r.localDir
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		if err := cmd.Run(); err != nil {
			return fmt.Errorf("git %s failed: %w", args[0], err)
		}
	}

	return nil
}
