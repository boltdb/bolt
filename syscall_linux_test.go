package bolt

import (
	"github.com/stretchr/testify/mock"
)

type mocksyscall struct {
	mock.Mock
}

func (m *mocksyscall) Mmap(fd int, offset int64, length int, prot int, flags int) (data []byte, err error) {
	args := m.Called(fd, offset, length, prot, flags)
	return args.Get(0).([]byte), args.Error(1)
}

func (m *mocksyscall) Munmap(b []byte) error {
	args := m.Called(b)
	return args.Error(0)
}
