package bolt

// maxMapSize represents the largest mmap size supported by Bolt.
const maxMapSize = 0x3FFFFFFF // 1GB

// maxAllocSize is the size used when creating array pointers.
const maxAllocSize = 0xFFFFFFF

// Are unaligned load/stores broken on this arch?
// MIPS needs 32-bit words are aligned to 4 byte boundaries.
var brokenUnaligned = true
