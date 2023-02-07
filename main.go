package main

import (
	"fmt"
	"os"

	"github.com/fgrosse/kafkactl/cmd"
)

func main() {
	err := cmd.New().Execute()
	if err != nil {
		fmt.Fprintln(os.Stderr, "ERROR:", err)
		os.Exit(1)
	}
}
