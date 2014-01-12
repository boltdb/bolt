package bolt

import (
	"github.com/stretchr/testify/mock"
)

type mockfile struct {
	mock.Mock
	fd   uintptr
	name string
}

func (m *mockfile) Fd() uintptr {
	return m.fd
}

func (m *mockfile) Name() string {
	return m.name
}

func (m *mockfile) ReadAt(b []byte, off int64) (n int, err error) {
	args := m.Called(b, off)
	return args.Int(0), args.Error(1)
}

func (m *mockfile) WriteAt(b []byte, off int64) (n int, err error) {
	args := m.Called(b, off)
	return args.Int(0), args.Error(1)
}
