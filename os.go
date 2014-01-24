package bolt

import (
	"os"
)

type _os interface {
	OpenFile(name string, flag int, perm os.FileMode) (file file, err error)
	Stat(name string) (fi os.FileInfo, err error)
	Getpagesize() int
}

type file interface {
	Fd() uintptr
	ReadAt(b []byte, off int64) (n int, err error)
	Stat() (fi os.FileInfo, err error)
	WriteAt(b []byte, off int64) (n int, err error)
}

type sysos struct{}

func (o *sysos) OpenFile(name string, flag int, perm os.FileMode) (file file, err error) {
	return os.OpenFile(name, flag, perm)
}

func (o *sysos) Stat(name string) (fi os.FileInfo, err error) {
	return os.Stat(name)
}

func (o *sysos) Getpagesize() int {
	return os.Getpagesize()
}
