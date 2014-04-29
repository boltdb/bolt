package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"runtime"
	"runtime/pprof"
	"time"

	"github.com/boltdb/bolt"
)

// File handlers for the various profiles.
var cpuprofile, memprofile, blockprofile *os.File

var benchBucketName = []byte("bench")

// Bench executes a customizable, synthetic benchmark against Bolt.
func Bench(options *BenchOptions) {
	var results BenchResults

	// Find temporary location.
	path := tempfile()
	defer os.Remove(path)

	// Create database.
	db, err := bolt.Open(path, 0600)
	if err != nil {
		fatal(err)
		return
	}
	defer db.Close()

	// Start profiling for writes.
	if options.ProfileMode == "rw" || options.ProfileMode == "w" {
		benchStartProfiling(options)
	}

	// Write to the database.
	if err := benchWrite(db, options, &results); err != nil {
		fatal("bench: write: ", err)
	}

	// Stop profiling for writes only.
	if options.ProfileMode == "w" {
		benchStopProfiling()
	}

	// Start profiling for reads.
	if options.ProfileMode == "r" {
		benchStartProfiling(options)
	}

	// Read from the database.
	if err := benchRead(db, options, &results); err != nil {
		fatal("bench: read: ", err)
	}

	// Stop profiling for writes only.
	if options.ProfileMode == "rw" || options.ProfileMode == "r" {
		benchStopProfiling()
	}

	// Print results.
	fmt.Printf("# Write\t%v\t(%v/op)\t(%v op/sec)\n", results.WriteDuration, results.WriteOpDuration(), results.WriteOpsPerSecond())
	fmt.Printf("# Read\t%v\t(%v/op)\t(%v op/sec)\n", results.ReadDuration, results.ReadOpDuration(), results.ReadOpsPerSecond())
	fmt.Println("")
}

// Writes to the database.
func benchWrite(db *bolt.DB, options *BenchOptions, results *BenchResults) error {
	var err error
	var t = time.Now()

	switch options.WriteMode {
	case "seq":
		err = benchWriteSequential(db, options, results)
	default:
		return fmt.Errorf("invalid write mode: %s", options.WriteMode)
	}

	results.WriteDuration = time.Since(t)

	return err
}

func benchWriteSequential(db *bolt.DB, options *BenchOptions, results *BenchResults) error {
	// Default batch size to iteration count, if not specified.
	var batchSize, iterations = options.BatchSize, options.Iterations
	if batchSize == 0 {
		batchSize = iterations
	}

	// Insert in batches.
	var count int
	for i := 0; i < (iterations/batchSize)+1; i++ {
		err := db.Update(func(tx *bolt.Tx) error {
			b, _ := tx.CreateBucketIfNotExists(benchBucketName)

			for j := 0; j < batchSize && count < iterations; j++ {
				var key = make([]byte, options.KeySize)
				var value = make([]byte, options.ValueSize)
				binary.BigEndian.PutUint32(key, uint32(count))

				if err := b.Put(key, value); err != nil {
					return err
				}

				count++
			}

			return nil
		})
		if err != nil {
			return err
		}
	}

	// Update the write op count.
	results.WriteOps = count

	return nil
}

// Reads from the database.
func benchRead(db *bolt.DB, options *BenchOptions, results *BenchResults) error {
	var err error
	var t = time.Now()

	switch options.ReadMode {
	case "seq":
		err = benchReadSequential(db, options, results)
	default:
		return fmt.Errorf("invalid read mode: %s", options.ReadMode)
	}

	results.ReadDuration = time.Since(t)

	return err
}

func benchReadSequential(db *bolt.DB, options *BenchOptions, results *BenchResults) error {
	return db.View(func(tx *bolt.Tx) error {
		var t = time.Now()

		for {
			c := tx.Bucket(benchBucketName).Cursor()
			var count int
			for k, v := c.First(); k != nil; k, v = c.Next() {
				if v == nil {
					return errors.New("invalid value")
				}
				count++
			}

			if count != options.Iterations {
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

// Starts all profiles set on the options.
func benchStartProfiling(options *BenchOptions) {
	var err error

	// Start CPU profiling.
	if options.CPUProfile != "" {
		cpuprofile, err = os.Create(options.CPUProfile)
		if err != nil {
			fatal("bench: could not create cpu profile %q: %v", options.CPUProfile, err)
		}
		pprof.StartCPUProfile(cpuprofile)
	}

	// Start memory profiling.
	if options.MemProfile != "" {
		memprofile, err = os.Create(options.MemProfile)
		if err != nil {
			fatal("bench: could not create memory profile %q: %v", options.MemProfile, err)
		}
		runtime.MemProfileRate = 4096
	}

	// Start fatal profiling.
	if options.BlockProfile != "" {
		blockprofile, err = os.Create(options.BlockProfile)
		if err != nil {
			fatal("bench: could not create block profile %q: %v", options.BlockProfile, err)
		}
		runtime.SetBlockProfileRate(1)
	}
}

// Stops all profiles.
func benchStopProfiling() {
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

// BenchOptions represents the set of options that can be passed to Bench().
type BenchOptions struct {
	ProfileMode  string
	WriteMode    string
	ReadMode     string
	Iterations   int
	BatchSize    int
	KeySize      int
	ValueSize    int
	CPUProfile   string
	MemProfile   string
	BlockProfile string
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

// tempfile returns a temporary file path.
func tempfile() string {
	f, _ := ioutil.TempFile("", "bolt-bench-")
	f.Close()
	os.Remove(f.Name())
	return f.Name()
}
