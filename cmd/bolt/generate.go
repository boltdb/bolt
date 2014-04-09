package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// Generate data for benchmarks.
func Generate(numEvents int, destPath string) {
	f, err := os.Create(destPath)
	if err != nil {
		fatal(err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			fatal(err)
		}
	}()
	w := bufio.NewWriter(f)

	for i := 0; i < numEvents; i++ {
		if _, err := w.Write([]byte(fmt.Sprintf("key%d:%s\n", i, strings.Repeat("0", 64)))); err != nil {
			fatal(err)
		}
	}

	if err = w.Flush(); err != nil {
		fatal(err)
	}
}
