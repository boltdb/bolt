package bolt

import (
	"os"

	"github.com/stretchr/testify/mock"
)

type mockos struct {
	mock.Mock
}

func (m *mockos) OpenFile(name string, flag int, perm os.FileMode) (file *os.File, err error) {
	args := m.Called(name, flag, perm)
	return args.Get(0).(*os.File), args.Error(1)
}

func (m *mockos) Stat(name string) (fi os.FileInfo, err error) {
	args := m.Called(name)
	return args.Get(0).(os.FileInfo), args.Error(1)
}

func (m *mockos) Getpagesize() int {
	args := m.Called()
	return args.Int(0)
}
