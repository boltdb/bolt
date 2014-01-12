package bolt

type file interface {
	Fd() uintptr
	Name() string
	ReadAt(b []byte, off int64) (n int, err error)
	WriteAt(b []byte, off int64) (n int, err error)
}
