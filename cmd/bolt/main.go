package main

import (
	"bytes"
	"log"
	"os"
	"strconv"

	"github.com/boltdb/bolt"
	"github.com/codegangsta/cli"
)

func main() {
	log.SetFlags(0)
	NewApp().Run(os.Args)
}

// NewApp creates an Application instance.
func NewApp() *cli.App {
	app := cli.NewApp()
	app.Name = "bolt"
	app.Usage = "BoltDB toolkit"
	app.Commands = []cli.Command{
		{
			Name:   "get",
			Usage:  "Retrieve a value for given key in a bucket",
			Action: GetCommand,
		},
		{
			Name:   "set",
			Usage:  "Sets a value for given key in a bucket",
			Action: SetCommand,
		},
		{
			Name:   "keys",
			Usage:  "Retrieve a list of all keys in a bucket",
			Action: KeysCommand,
		},
		{
			Name:   "pages",
			Usage:  "Dumps page information for a database",
			Action: PagesCommand,
		},
	}
	return app
}

// GetCommand retrieves the value for a given bucket/key.
func GetCommand(c *cli.Context) {
	path, name, key := c.Args().Get(0), c.Args().Get(1), c.Args().Get(2)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		fatal(err)
		return
	}

	db, err := bolt.Open(path, 0600)
	if err != nil {
		fatal(err)
		return
	}
	defer db.Close()

	err = db.With(func(tx *bolt.Tx) error {
		// Find bucket.
		b := tx.Bucket(name)
		if b == nil {
			fatalf("bucket not found: %s", name)
			return nil
		}

		// Find value for a given key.
		value := b.Get([]byte(key))
		if value == nil {
			fatalf("key not found: %s", key)
			return nil
		}

		logger.Println(string(value))
		return nil
	})
	if err != nil {
		fatal(err)
		return
	}
}

// SetCommand sets the value for a given key in a bucket.
func SetCommand(c *cli.Context) {
	path, name, key, value := c.Args().Get(0), c.Args().Get(1), c.Args().Get(2), c.Args().Get(3)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		fatal(err)
		return
	}

	db, err := bolt.Open(path, 0600)
	if err != nil {
		fatal(err)
		return
	}
	defer db.Close()

	err = db.Do(func(tx *bolt.Tx) error {
		// Find bucket.
		b := tx.Bucket(name)
		if b == nil {
			fatalf("bucket not found: %s", name)
			return nil
		}

		// Set value for a given key.
		return b.Put([]byte(key), []byte(value))
	})
	if err != nil {
		fatal(err)
		return
	}
}

// KeysCommand retrieves a list of keys for a given bucket.
func KeysCommand(c *cli.Context) {
	path, name := c.Args().Get(0), c.Args().Get(1)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		fatal(err)
		return
	}

	db, err := bolt.Open(path, 0600)
	if err != nil {
		fatal(err)
		return
	}
	defer db.Close()

	err = db.With(func(tx *bolt.Tx) error {
		// Find bucket.
		b := tx.Bucket(name)
		if b == nil {
			fatalf("bucket not found: %s", name)
			return nil
		}

		// Iterate over each key.
		return b.ForEach(func(key, _ []byte) error {
			logger.Println(string(key))
			return nil
		})
	})
	if err != nil {
		fatal(err)
		return
	}
}

// PagesCommand prints a list of all pages in a database.
func PagesCommand(c *cli.Context) {
	path := c.Args().Get(0)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		fatal(err)
		return
	}

	db, err := bolt.Open(path, 0600)
	if err != nil {
		fatal(err)
		return
	}
	defer db.Close()

	logger.Println("ID       TYPE       ITEMS  OVRFLW")
	logger.Println("======== ========== ====== ======")

	db.Do(func(tx *bolt.Tx) error {
		var id int
		for {
			p, err := tx.Page(id)
			if err != nil {
				fatalf("page error: %d: %s", id, err)
			} else if p == nil {
				break
			}

			var overflow string
			if p.OverflowCount > 0 {
				overflow = strconv.Itoa(p.OverflowCount)
			}
			logger.Printf("%-8d %-10s %-6d %-6s", p.ID, p.Type, p.Count, overflow)
			id += 1 + p.OverflowCount
		}
		return nil
	})
}

var logger = log.New(os.Stderr, "", 0)
var logBuffer *bytes.Buffer

func fatal(v ...interface{}) {
	logger.Print(v...)
	if !testMode {
		os.Exit(1)
	}
}

func fatalf(format string, v ...interface{}) {
	logger.Printf(format, v...)
	if !testMode {
		os.Exit(1)
	}
}

func fatalln(v ...interface{}) {
	logger.Println(v...)
	if !testMode {
		os.Exit(1)
	}
}

// LogBuffer returns the contents of the log.
// This only works while the CLI is in test mode.
func LogBuffer() string {
	if logBuffer != nil {
		return logBuffer.String()
	}
	return ""
}

var testMode bool

// SetTestMode sets whether the CLI is running in test mode and resets the logger.
func SetTestMode(value bool) {
	testMode = value
	if testMode {
		logBuffer = bytes.NewBuffer(nil)
		logger = log.New(logBuffer, "", 0)
	} else {
		logger = log.New(os.Stderr, "", 0)
	}
}
