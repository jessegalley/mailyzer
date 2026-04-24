package main

import (
	"os"

	"github.com/jessegalley/mailyzer/cmd"
)

func main() {
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
