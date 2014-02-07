package bolt

import "fmt"

// TODO(benbjohnson): Remove assertions before release.

// __assert__ will panic with a given formatted message if the given condition is false.
func __assert__(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assertion failed: " + msg, v...))
	}
}
