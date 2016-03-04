// +build linux
// +build ppc64le ppc64
// +build !386,!amd64,!arm,!arm64,!ppc,!s390x

package bolt

// maxMapSize represents the largest mmap size supported by Bolt.
const maxMapSize = 0xFFFFFFFFFFFF // 256TB

// maxAllocSize is the size used when creating array pointers.
const maxAllocSize = 0x7FFFFFFF
