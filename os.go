package bolt

import (
	"os"
)

type _os interface {
	OpenFile(name string, flag int, perm os.FileMode) (file *os.File, err error)
	Stat(name string) (fi os.FileInfo, err error)
	Getpagesize() int
}

type sysos struct{}

func (o *sysos) OpenFile(name string, flag int, perm os.FileMode) (file *os.File, err error) {
	return os.OpenFile(name, flag, perm)
}

func (o *sysos) Stat(name string) (fi os.FileInfo, err error) {
	return os.Stat(name)
}

func (o *sysos) Getpagesize() int {
	return os.Getpagesize()
}
