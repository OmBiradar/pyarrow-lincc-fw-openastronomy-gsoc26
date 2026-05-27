package main

import (
	"fmt"
	"os"
)

func IfExistsDir(path string) bool {
	if path == "" {
		return false
	}
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		fmt.Printf("Error: Directory does not exist: %s\n", path)
		return false
	}
	return true
}
