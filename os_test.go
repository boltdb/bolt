package bolt

import (
	"os"

	"github.com/stretchr/testify/mock"
)

type mockos struct {
	mock.Mock
}

func (m *mockos) OpenFile(name string, flag int, perm os.FileMode) (file file, err error) {
	args := m.Called(name, flag, perm)
	return args.Get(0).(*mockfile), args.Error(1)
}

func (m *mockos) Stat(name string) (fi os.FileInfo, err error) {
	args := m.Called(name)
	return args.Get(0).(os.FileInfo), args.Error(1)
}

func (m *mockos) Getpagesize() int {
	args := m.Called()
	return args.Int(0)
}

type mockfile struct {
	mock.Mock
	fd uintptr
}

func (m *mockfile) Fd() uintptr {
	return m.fd
}

func (m *mockfile) ReadAt(b []byte, off int64) (n int, err error) {
	args := m.Called(b, off)
	return args.Int(0), args.Error(1)
}

func (m *mockfile) Stat() (os.FileInfo, error) {
	args := m.Called()
	return args.Get(0).(os.FileInfo), args.Error(1)
}

func (m *mockfile) WriteAt(b []byte, off int64) (n int, err error) {
	args := m.Called(b, off)
	return args.Int(0), args.Error(1)
}

type mockfileinfo struct {
	name    string
	size    int64
	mode    os.FileMode
	modTime time.Time
	isDir   bool
	sys     interface{}
}

func (m *mockfileinfo) Name() string {
	return m.name
}

func (m *mockfileinfo) Size() int64 {
	return m.size
}

func (m *mockfileinfo) Mode() os.FileMode {
	return m.mode
}

func (m *mockfileinfo) ModTime() time.Time {
	return m.modTime
}

func (m *mockfileinfo) IsDir() bool {
	return m.isDir
}

func (m *mockfileinfo) Sys() interface{} {
	return m.sys
}
