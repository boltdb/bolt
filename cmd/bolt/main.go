package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"time"

	"github.com/boltdb/bolt"
)

var (
	// ErrUsage is returned when a usage message was printed and the process
	// should simply exit with an error.
	ErrUsage = errors.New("usage")

	// ErrUnknownCommand is returned when a CLI command is not specified.
	ErrUnknownCommand = errors.New("unknown command")

	// ErrPathRequired is returned when the path to a Bolt database is not specified.
	ErrPathRequired = errors.New("path required")

	// ErrFileNotFound is returned when a Bolt database does not exist.
	ErrFileNotFound = errors.New("file not found")

	// ErrInvalidValue is returned when a benchmark reads an unexpected value.
	ErrInvalidValue = errors.New("invalid value")

	// ErrCorrupt is returned when a checking a data file finds errors.
	ErrCorrupt = errors.New("invalid value")

	// ErrNonDivisibleBatchSize is returned when the batch size can't be evenly
	// divided by the iteration count.
	ErrNonDivisibleBatchSize = errors.New("number of iterations must be divisible by the batch size")
)

func main() {
	m := NewMain()
	if err := m.Run(os.Args[1:]...); err == ErrUsage {
		os.Exit(2)
	} else if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
}

// Main represents the main program execution.
type Main struct {
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

// NewMain returns a new instance of Main connect to the standard input/output.
func NewMain() *Main {
	return &Main{
		Stdin:  os.Stdin,
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}
}

// Run executes the program.
func (m *Main) Run(args ...string) error {
	// Require a command at the beginning.
	if len(args) == 0 || strings.HasPrefix(args[0], "-") {
		fmt.Fprintln(m.Stderr, m.Usage())
		return ErrUsage
	}

	// Execute command.
	switch args[0] {
	case "help":
		fmt.Fprintln(m.Stderr, m.Usage())
		return ErrUsage
	case "bench":
		return newBenchCommand(m).Run(args[1:]...)
	case "check":
		return newCheckCommand(m).Run(args[1:]...)
	case "info":
		return newInfoCommand(m).Run(args[1:]...)
	case "pages":
		return newPagesCommand(m).Run(args[1:]...)
	case "stats":
		return newStatsCommand(m).Run(args[1:]...)
	default:
		return ErrUnknownCommand
	}
}

// Usage returns the help message.
func (m *Main) Usage() string {
	return strings.TrimLeft(`
Bolt is a tool for inspecting bolt databases.

Usage:

	bolt command [arguments]

The commands are:

    bench       run synthetic benchmark against bolt
    check       verifies integrity of bolt database
    info        print basic info
    help        print this screen
    pages       print list of pages with their types
    stats       iterate over all pages and generate usage stats

Use "bolt [command] -h" for more information about a command.
`, "\n")
}

// CheckCommand represents the "check" command execution.
type CheckCommand struct {
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

// NewCheckCommand returns a CheckCommand.
func newCheckCommand(m *Main) *CheckCommand {
	return &CheckCommand{
		Stdin:  m.Stdin,
		Stdout: m.Stdout,
		Stderr: m.Stderr,
	}
}

// Run executes the command.
func (cmd *CheckCommand) Run(args ...string) error {
	// Parse flags.
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	help := fs.Bool("h", false, "")
	if err := fs.Parse(args); err != nil {
		return err
	} else if *help {
		fmt.Fprintln(cmd.Stderr, cmd.Usage())
		return ErrUsage
	}

	// Require database path.
	path := fs.Arg(0)
	if path == "" {
		return ErrPathRequired
	} else if _, err := os.Stat(path); os.IsNotExist(err) {
		return ErrFileNotFound
	}

	// Open database.
	db, err := bolt.Open(path, 0666, nil)
	if err != nil {
		return err
	}
	defer db.Close()

	// Perform consistency check.
	return db.View(func(tx *bolt.Tx) error {
		var count int
		ch := tx.Check()
	loop:
		for {
			select {
			case err, ok := <-ch:
				if !ok {
					break loop
				}
				fmt.Fprintln(cmd.Stdout, err)
				count++
			}
		}

		// Print summary of errors.
		if count > 0 {
			fmt.Fprintf(cmd.Stdout, "%d errors found\n", count)
			return ErrCorrupt
		}

		// Notify user that database is valid.
		fmt.Fprintln(cmd.Stdout, "OK")
		return nil
	})
}

// Usage returns the help message.
func (cmd *CheckCommand) Usage() string {
	return strings.TrimLeft(`
usage: bolt check PATH

Check opens a database at PATH and runs an exhaustive check to verify that
all pages are accessible or are marked as freed. It also verifies that no
pages are double referenced.

Verification errors will stream out as they are found and the process will
return after all pages have been checked.
`, "\n")
}

// InfoCommand represents the "info" command execution.
type InfoCommand struct {
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

// NewInfoCommand returns a InfoCommand.
func newInfoCommand(m *Main) *InfoCommand {
	return &InfoCommand{
		Stdin:  m.Stdin,
		Stdout: m.Stdout,
		Stderr: m.Stderr,
	}
}

// Run executes the command.
func (cmd *InfoCommand) Run(args ...string) error {
	// Parse flags.
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	help := fs.Bool("h", false, "")
	if err := fs.Parse(args); err != nil {
		return err
	} else if *help {
		fmt.Fprintln(cmd.Stderr, cmd.Usage())
		return ErrUsage
	}

	// Require database path.
	path := fs.Arg(0)
	if path == "" {
		return ErrPathRequired
	} else if _, err := os.Stat(path); os.IsNotExist(err) {
		return ErrFileNotFound
	}

	// Open the database.
	db, err := bolt.Open(path, 0666, nil)
	if err != nil {
		return err
	}
	defer db.Close()

	// Print basic database info.
	info := db.Info()
	fmt.Fprintf(cmd.Stdout, "Page Size: %d\n", info.PageSize)

	return nil
}

// Usage returns the help message.
func (cmd *InfoCommand) Usage() string {
	return strings.TrimLeft(`
usage: bolt info PATH

Info prints basic information about the Bolt database at PATH.
`, "\n")
}

// PagesCommand represents the "pages" command execution.
type PagesCommand struct {
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

// NewPagesCommand returns a PagesCommand.
func newPagesCommand(m *Main) *PagesCommand {
	return &PagesCommand{
		Stdin:  m.Stdin,
		Stdout: m.Stdout,
		Stderr: m.Stderr,
	}
}

// Run executes the command.
func (cmd *PagesCommand) Run(args ...string) error {
	// Parse flags.
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	help := fs.Bool("h", false, "")
	if err := fs.Parse(args); err != nil {
		return err
	} else if *help {
		fmt.Fprintln(cmd.Stderr, cmd.Usage())
		return ErrUsage
	}

	// Require database path.
	path := fs.Arg(0)
	if path == "" {
		return ErrPathRequired
	} else if _, err := os.Stat(path); os.IsNotExist(err) {
		return ErrFileNotFound
	}

	// Open database.
	db, err := bolt.Open(path, 0666, nil)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	// Write header.
	fmt.Fprintln(cmd.Stdout, "ID       TYPE       ITEMS  OVRFLW")
	fmt.Fprintln(cmd.Stdout, "======== ========== ====== ======")

	return db.Update(func(tx *bolt.Tx) error {
		var id int
		for {
			p, err := tx.Page(id)
			if err != nil {
				return &PageError{ID: id, Err: err}
			} else if p == nil {
				break
			}

			// Only display count and overflow if this is a non-free page.
			var count, overflow string
			if p.Type != "free" {
				count = strconv.Itoa(p.Count)
				if p.OverflowCount > 0 {
					overflow = strconv.Itoa(p.OverflowCount)
				}
			}

			// Print table row.
			fmt.Fprintf(cmd.Stdout, "%-8d %-10s %-6s %-6s\n", p.ID, p.Type, count, overflow)

			// Move to the next non-overflow page.
			id += 1
			if p.Type != "free" {
				id += p.OverflowCount
			}
		}
		return nil
	})
}

// Usage returns the help message.
func (cmd *PagesCommand) Usage() string {
	return strings.TrimLeft(`
usage: bolt pages PATH

Pages prints a table of pages with their type (meta, leaf, branch, freelist).
Leaf and branch pages will show a key count in the "items" column while the
freelist will show the number of free pages in the "items" column.

The "overflow" column shows the number of blocks that the page spills over
into. Normally there is no overflow but large keys and values can cause
a single page to take up multiple blocks.
`, "\n")
}

// StatsCommand represents the "stats" command execution.
type StatsCommand struct {
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

// NewStatsCommand returns a StatsCommand.
func newStatsCommand(m *Main) *StatsCommand {
	return &StatsCommand{
		Stdin:  m.Stdin,
		Stdout: m.Stdout,
		Stderr: m.Stderr,
	}
}

// Run executes the command.
func (cmd *StatsCommand) Run(args ...string) error {
	// Parse flags.
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	help := fs.Bool("h", false, "")
	if err := fs.Parse(args); err != nil {
		return err
	} else if *help {
		fmt.Fprintln(cmd.Stderr, cmd.Usage())
		return ErrUsage
	}

	// Require database path.
	path, prefix := fs.Arg(0), fs.Arg(1)
	if path == "" {
		return ErrPathRequired
	} else if _, err := os.Stat(path); os.IsNotExist(err) {
		return ErrFileNotFound
	}

	// Open database.
	db, err := bolt.Open(path, 0666, nil)
	if err != nil {
		return err
	}
	defer db.Close()

	return db.View(func(tx *bolt.Tx) error {
		var s bolt.BucketStats
		var count int
		if err := tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			if bytes.HasPrefix(name, []byte(prefix)) {
				s.Add(b.Stats())
				count += 1
			}
			return nil
		}); err != nil {
			return err
		}

		fmt.Fprintf(cmd.Stdout, "Aggregate statistics for %d buckets\n\n", count)

		fmt.Fprintln(cmd.Stdout, "Page count statistics")
		fmt.Fprintf(cmd.Stdout, "\tNumber of logical branch pages: %d\n", s.BranchPageN)
		fmt.Fprintf(cmd.Stdout, "\tNumber of physical branch overflow pages: %d\n", s.BranchOverflowN)
		fmt.Fprintf(cmd.Stdout, "\tNumber of logical leaf pages: %d\n", s.LeafPageN)
		fmt.Fprintf(cmd.Stdout, "\tNumber of physical leaf overflow pages: %d\n", s.LeafOverflowN)

		fmt.Fprintln(cmd.Stdout, "Tree statistics")
		fmt.Fprintf(cmd.Stdout, "\tNumber of keys/value pairs: %d\n", s.KeyN)
		fmt.Fprintf(cmd.Stdout, "\tNumber of levels in B+tree: %d\n", s.Depth)

		fmt.Fprintln(cmd.Stdout, "Page size utilization")
		fmt.Fprintf(cmd.Stdout, "\tBytes allocated for physical branch pages: %d\n", s.BranchAlloc)
		var percentage int
		if s.BranchAlloc != 0 {
			percentage = int(float32(s.BranchInuse) * 100.0 / float32(s.BranchAlloc))
		}
		fmt.Fprintf(cmd.Stdout, "\tBytes actually used for branch data: %d (%d%%)\n", s.BranchInuse, percentage)
		fmt.Fprintf(cmd.Stdout, "\tBytes allocated for physical leaf pages: %d\n", s.LeafAlloc)
		percentage = 0
		if s.LeafAlloc != 0 {
			percentage = int(float32(s.LeafInuse) * 100.0 / float32(s.LeafAlloc))
		}
		fmt.Fprintf(cmd.Stdout, "\tBytes actually used for leaf data: %d (%d%%)\n", s.LeafInuse, percentage)

		fmt.Fprintln(cmd.Stdout, "Bucket statistics")
		fmt.Fprintf(cmd.Stdout, "\tTotal number of buckets: %d\n", s.BucketN)
		percentage = int(float32(s.InlineBucketN) * 100.0 / float32(s.BucketN))
		fmt.Fprintf(cmd.Stdout, "\tTotal number on inlined buckets: %d (%d%%)\n", s.InlineBucketN, percentage)
		percentage = 0
		if s.LeafInuse != 0 {
			percentage = int(float32(s.InlineBucketInuse) * 100.0 / float32(s.LeafInuse))
		}
		fmt.Fprintf(cmd.Stdout, "\tBytes used for inlined buckets: %d (%d%%)\n", s.InlineBucketInuse, percentage)

		return nil
	})
}

// Usage returns the help message.
func (cmd *StatsCommand) Usage() string {
	return strings.TrimLeft(`
usage: bolt stats PATH

Stats performs an extensive search of the database to track every page
reference. It starts at the current meta page and recursively iterates
through every accessible bucket.

The following errors can be reported:

    already freed
        The page is referenced more than once in the freelist.

    unreachable unfreed
        The page is not referenced by a bucket or in the freelist.

    reachable freed
        The page is referenced by a bucket but is also in the freelist.

    out of bounds
        A page is referenced that is above the high water mark.

    multiple references
        A page is referenced by more than one other page.

    invalid type
        The page type is not "meta", "leaf", "branch", or "freelist".

No errors should occur in your database. However, if for some reason you
experience corruption, please submit a ticket to the Bolt project page:

  https://github.com/boltdb/bolt/issues
`, "\n")
}

var benchBucketName = []byte("bench")

// BenchCommand represents the "bench" command execution.
type BenchCommand struct {
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

// NewBenchCommand returns a BenchCommand using the
func newBenchCommand(m *Main) *BenchCommand {
	return &BenchCommand{
		Stdin:  m.Stdin,
		Stdout: m.Stdout,
		Stderr: m.Stderr,
	}
}

// Run executes the "bench" command.
func (cmd *BenchCommand) Run(args ...string) error {
	// Parse CLI arguments.
	options, err := cmd.ParseFlags(args)
	if err != nil {
		return err
	}

	// Remove path if "-work" is not set. Otherwise keep path.
	if options.Work {
		fmt.Fprintf(cmd.Stdout, "work: %s\n", options.Path)
	} else {
		defer os.Remove(options.Path)
	}

	// Create database.
	db, err := bolt.Open(options.Path, 0666, nil)
	if err != nil {
		return err
	}
	db.NoSync = options.NoSync
	defer db.Close()

	// Write to the database.
	var results BenchResults
	if err := cmd.runWrites(db, options, &results); err != nil {
		return fmt.Errorf("write: ", err)
	}

	// Read from the database.
	if err := cmd.runReads(db, options, &results); err != nil {
		return fmt.Errorf("bench: read: %s", err)
	}

	// Print results.
	fmt.Fprintf(os.Stderr, "# Write\t%v\t(%v/op)\t(%v op/sec)\n", results.WriteDuration, results.WriteOpDuration(), results.WriteOpsPerSecond())
	fmt.Fprintf(os.Stderr, "# Read\t%v\t(%v/op)\t(%v op/sec)\n", results.ReadDuration, results.ReadOpDuration(), results.ReadOpsPerSecond())
	fmt.Fprintln(os.Stderr, "")
	return nil
}

// ParseFlags parses the command line flags.
func (cmd *BenchCommand) ParseFlags(args []string) (*BenchOptions, error) {
	var options BenchOptions

	// Parse flagset.
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	fs.StringVar(&options.ProfileMode, "profile-mode", "rw", "")
	fs.StringVar(&options.WriteMode, "write-mode", "seq", "")
	fs.StringVar(&options.ReadMode, "read-mode", "seq", "")
	fs.IntVar(&options.Iterations, "count", 1000, "")
	fs.IntVar(&options.BatchSize, "batch-size", 0, "")
	fs.IntVar(&options.KeySize, "key-size", 8, "")
	fs.IntVar(&options.ValueSize, "value-size", 32, "")
	fs.StringVar(&options.CPUProfile, "cpuprofile", "", "")
	fs.StringVar(&options.MemProfile, "memprofile", "", "")
	fs.StringVar(&options.BlockProfile, "blockprofile", "", "")
	fs.StringVar(&options.BlockProfile, "blockprofile", "", "")
	fs.Float64Var(&options.FillPercent, "fill-percent", bolt.DefaultFillPercent, "")
	fs.BoolVar(&options.NoSync, "no-sync", false, "")
	fs.BoolVar(&options.Work, "work", false, "")
	fs.StringVar(&options.Path, "path", "", "")
	fs.SetOutput(cmd.Stderr)
	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	// Set batch size to iteration size if not set.
	// Require that batch size can be evenly divided by the iteration count.
	if options.BatchSize == 0 {
		options.BatchSize = options.Iterations
	} else if options.Iterations%options.BatchSize != 0 {
		return nil, ErrNonDivisibleBatchSize
	}

	// Generate temp path if one is not passed in.
	if options.Path == "" {
		f, err := ioutil.TempFile("", "bolt-bench-")
		if err != nil {
			return nil, fmt.Errorf("temp file: %s", err)
		}
		f.Close()
		os.Remove(f.Name())
		options.Path = f.Name()
	}

	return &options, nil
}

// Writes to the database.
func (cmd *BenchCommand) runWrites(db *bolt.DB, options *BenchOptions, results *BenchResults) error {
	// Start profiling for writes.
	if options.ProfileMode == "rw" || options.ProfileMode == "w" {
		cmd.startProfiling(options)
	}

	t := time.Now()

	var err error
	switch options.WriteMode {
	case "seq":
		err = cmd.runWritesSequential(db, options, results)
	case "rnd":
		err = cmd.runWritesRandom(db, options, results)
	case "seq-nest":
		err = cmd.runWritesSequentialNested(db, options, results)
	case "rnd-nest":
		err = cmd.runWritesRandomNested(db, options, results)
	default:
		return fmt.Errorf("invalid write mode: %s", options.WriteMode)
	}

	// Save time to write.
	results.WriteDuration = time.Since(t)

	// Stop profiling for writes only.
	if options.ProfileMode == "w" {
		cmd.stopProfiling()
	}

	return err
}

func (cmd *BenchCommand) runWritesSequential(db *bolt.DB, options *BenchOptions, results *BenchResults) error {
	var i = uint32(0)
	return cmd.runWritesWithSource(db, options, results, func() uint32 { i++; return i })
}

func (cmd *BenchCommand) runWritesRandom(db *bolt.DB, options *BenchOptions, results *BenchResults) error {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return cmd.runWritesWithSource(db, options, results, func() uint32 { return r.Uint32() })
}

func (cmd *BenchCommand) runWritesSequentialNested(db *bolt.DB, options *BenchOptions, results *BenchResults) error {
	var i = uint32(0)
	return cmd.runWritesWithSource(db, options, results, func() uint32 { i++; return i })
}

func (cmd *BenchCommand) runWritesRandomNested(db *bolt.DB, options *BenchOptions, results *BenchResults) error {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return cmd.runWritesWithSource(db, options, results, func() uint32 { return r.Uint32() })
}

func (cmd *BenchCommand) runWritesWithSource(db *bolt.DB, options *BenchOptions, results *BenchResults, keySource func() uint32) error {
	results.WriteOps = options.Iterations

	for i := 0; i < options.Iterations; i += options.BatchSize {
		if err := db.Update(func(tx *bolt.Tx) error {
			b, _ := tx.CreateBucketIfNotExists(benchBucketName)
			b.FillPercent = options.FillPercent

			for j := 0; j < options.BatchSize; j++ {
				key := make([]byte, options.KeySize)
				value := make([]byte, options.ValueSize)

				// Write key as uint32.
				binary.BigEndian.PutUint32(key, keySource())

				// Insert key/value.
				if err := b.Put(key, value); err != nil {
					return err
				}
			}

			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

func (cmd *BenchCommand) runWritesNestedWithSource(db *bolt.DB, options *BenchOptions, results *BenchResults, keySource func() uint32) error {
	results.WriteOps = options.Iterations

	for i := 0; i < options.Iterations; i += options.BatchSize {
		if err := db.Update(func(tx *bolt.Tx) error {
			top, err := tx.CreateBucketIfNotExists(benchBucketName)
			if err != nil {
				return err
			}
			top.FillPercent = options.FillPercent

			// Create bucket key.
			name := make([]byte, options.KeySize)
			binary.BigEndian.PutUint32(name, keySource())

			// Create bucket.
			b, err := top.CreateBucketIfNotExists(name)
			if err != nil {
				return err
			}
			b.FillPercent = options.FillPercent

			for j := 0; j < options.BatchSize; j++ {
				var key = make([]byte, options.KeySize)
				var value = make([]byte, options.ValueSize)

				// Generate key as uint32.
				binary.BigEndian.PutUint32(key, keySource())

				// Insert value into subbucket.
				if err := b.Put(key, value); err != nil {
					return err
				}
			}

			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

// Reads from the database.
func (cmd *BenchCommand) runReads(db *bolt.DB, options *BenchOptions, results *BenchResults) error {
	// Start profiling for reads.
	if options.ProfileMode == "r" {
		cmd.startProfiling(options)
	}

	t := time.Now()

	var err error
	switch options.ReadMode {
	case "seq":
		switch options.WriteMode {
		case "seq-nest", "rnd-nest":
			err = cmd.runReadsSequentialNested(db, options, results)
		default:
			err = cmd.runReadsSequential(db, options, results)
		}
	default:
		return fmt.Errorf("invalid read mode: %s", options.ReadMode)
	}

	// Save read time.
	results.ReadDuration = time.Since(t)

	// Stop profiling for reads.
	if options.ProfileMode == "rw" || options.ProfileMode == "r" {
		cmd.stopProfiling()
	}

	return err
}

func (cmd *BenchCommand) runReadsSequential(db *bolt.DB, options *BenchOptions, results *BenchResults) error {
	return db.View(func(tx *bolt.Tx) error {
		t := time.Now()

		for {
			var count int

			c := tx.Bucket(benchBucketName).Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				if v == nil {
					return errors.New("invalid value")
				}
				count++
			}

			if options.WriteMode == "seq" && count != options.Iterations {
				return fmt.Errorf("read seq: iter mismatch: expected %d, got %d", options.Iterations, count)
			}

			results.ReadOps += count

			// Make sure we do this for at least a second.
			if time.Since(t) >= time.Second {
				break
			}
		}

		return nil
	})
}

func (cmd *BenchCommand) runReadsSequentialNested(db *bolt.DB, options *BenchOptions, results *BenchResults) error {
	return db.View(func(tx *bolt.Tx) error {
		t := time.Now()

		for {
			var count int
			var top = tx.Bucket(benchBucketName)
			if err := top.ForEach(func(name, _ []byte) error {
				c := top.Bucket(name).Cursor()
				for k, v := c.First(); k != nil; k, v = c.Next() {
					if v == nil {
						return ErrInvalidValue
					}
					count++
				}
				return nil
			}); err != nil {
				return err
			}

			if options.WriteMode == "seq-nest" && count != options.Iterations {
				return fmt.Errorf("read seq-nest: iter mismatch: expected %d, got %d", options.Iterations, count)
			}

			results.ReadOps += count

			// Make sure we do this for at least a second.
			if time.Since(t) >= time.Second {
				break
			}
		}

		return nil
	})
}

// File handlers for the various profiles.
var cpuprofile, memprofile, blockprofile *os.File

// Starts all profiles set on the options.
func (cmd *BenchCommand) startProfiling(options *BenchOptions) {
	var err error

	// Start CPU profiling.
	if options.CPUProfile != "" {
		cpuprofile, err = os.Create(options.CPUProfile)
		if err != nil {
			fmt.Fprintf(cmd.Stderr, "bench: could not create cpu profile %q: %v\n", options.CPUProfile, err)
			os.Exit(1)
		}
		pprof.StartCPUProfile(cpuprofile)
	}

	// Start memory profiling.
	if options.MemProfile != "" {
		memprofile, err = os.Create(options.MemProfile)
		if err != nil {
			fmt.Fprintf(cmd.Stderr, "bench: could not create memory profile %q: %v\n", options.MemProfile, err)
			os.Exit(1)
		}
		runtime.MemProfileRate = 4096
	}

	// Start fatal profiling.
	if options.BlockProfile != "" {
		blockprofile, err = os.Create(options.BlockProfile)
		if err != nil {
			fmt.Fprintf(cmd.Stderr, "bench: could not create block profile %q: %v\n", options.BlockProfile, err)
			os.Exit(1)
		}
		runtime.SetBlockProfileRate(1)
	}
}

// Stops all profiles.
func (cmd *BenchCommand) stopProfiling() {
	if cpuprofile != nil {
		pprof.StopCPUProfile()
		cpuprofile.Close()
		cpuprofile = nil
	}

	if memprofile != nil {
		pprof.Lookup("heap").WriteTo(memprofile, 0)
		memprofile.Close()
		memprofile = nil
	}

	if blockprofile != nil {
		pprof.Lookup("block").WriteTo(blockprofile, 0)
		blockprofile.Close()
		blockprofile = nil
		runtime.SetBlockProfileRate(0)
	}
}

// BenchOptions represents the set of options that can be passed to "bolt bench".
type BenchOptions struct {
	ProfileMode   string
	WriteMode     string
	ReadMode      string
	Iterations    int
	BatchSize     int
	KeySize       int
	ValueSize     int
	CPUProfile    string
	MemProfile    string
	BlockProfile  string
	StatsInterval time.Duration
	FillPercent   float64
	NoSync        bool
	Work          bool
	Path          string
}

// BenchResults represents the performance results of the benchmark.
type BenchResults struct {
	WriteOps      int
	WriteDuration time.Duration
	ReadOps       int
	ReadDuration  time.Duration
}

// Returns the duration for a single write operation.
func (r *BenchResults) WriteOpDuration() time.Duration {
	if r.WriteOps == 0 {
		return 0
	}
	return r.WriteDuration / time.Duration(r.WriteOps)
}

// Returns average number of write operations that can be performed per second.
func (r *BenchResults) WriteOpsPerSecond() int {
	var op = r.WriteOpDuration()
	if op == 0 {
		return 0
	}
	return int(time.Second) / int(op)
}

// Returns the duration for a single read operation.
func (r *BenchResults) ReadOpDuration() time.Duration {
	if r.ReadOps == 0 {
		return 0
	}
	return r.ReadDuration / time.Duration(r.ReadOps)
}

// Returns average number of read operations that can be performed per second.
func (r *BenchResults) ReadOpsPerSecond() int {
	var op = r.ReadOpDuration()
	if op == 0 {
		return 0
	}
	return int(time.Second) / int(op)
}

type PageError struct {
	ID  int
	Err error
}

func (e *PageError) Error() string {
	return fmt.Sprintf("page error: id=%d, err=%s", e.ID, e.Err)
}
