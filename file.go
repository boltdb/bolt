package bolt

import (
	"os"
)

type file interface {
	Fd() uintptr
	ReadAt(b []byte, off int64) (n int, err error)
	Stat() (fi os.FileInfo, err error)
	WriteAt(b []byte, off int64) (n int, err error)
}
