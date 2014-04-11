package bench

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)

type bucketItems map[string]string
type buckets map[string]bucketItems

type Benchmark struct {
	buckets buckets
}

func New(filePath string) (*Benchmark, error) {
	data := readFromFile(filePath)
}

func readFromFile(filePath string) (*Benchmark, error) {
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return nil, err
	}

	file, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	b := new(Benchmark)
	if err := json.Unmarshal(file, &b.buckets); err != nil {
		return nil, err
	}

	return b, nil
}

func (b *Benchmark) Run() error {
	fmt.Println("Do things, run benchmarks, tell people...")
	return nil
}
