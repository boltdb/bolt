package bolt

import (
	"syscall"
)

type _syscall interface {
	Mmap(fd int, offset int64, length int, prot int, flags int) (data []byte, err error)
}

type syssyscall struct{}

func (o *syssyscall) Mmap(fd int, offset int64, length int, prot int, flags int) (data []byte, err error) {
	// err = (EACCES, EBADF, EINVAL, ENODEV, ENOMEM, ENXIO, EOVERFLOW)
	return syscall.Mmap(fd, offset, length, prot, flags)
}
