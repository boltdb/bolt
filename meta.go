package bolt

import (
	"errors"
	"hash/fnv"
	"unsafe"
)

const magic uint32 = 0xED0CDAED

var (
	// ErrInvalid is returned when a data file is not a Bolt-formatted database.
	ErrInvalid = errors.New("invalid database")

	// ErrVersionMismatch is returned when the data file was created with a
	// different version of Bolt.
	ErrVersionMismatch = errors.New("version mismatch")

	// ErrChecksum is returned when either meta page checksum does not match.
	ErrChecksum = errors.New("checksum error")
)

type meta struct {
	magic    uint32
	version  uint32
	pageSize uint32
	flags    uint32
	buckets  pgid
	freelist pgid
	pgid     pgid
	txid     txid
	checksum uint64
}

// validate checks the marker bytes and version of the meta page to ensure it matches this binary.
func (m *meta) validate() error {
	if m.checksum != 0 && m.checksum != m.sum64() {
		return ErrChecksum
	} else if m.magic != magic {
		return ErrInvalid
	} else if m.version != version {
		return ErrVersionMismatch
	}
	return nil
}

// copy copies one meta object to another.
func (m *meta) copy(dest *meta) {
	*dest = *m
}

// write writes the meta onto a page.
func (m *meta) write(p *page) {
	// Page id is either going to be 0 or 1 which we can determine by the transaction ID.
	p.id = pgid(m.txid % 2)
	p.flags |= metaPageFlag

	// Calculate the checksum.
	m.checksum = m.sum64()

	m.copy(p.meta())
}

// generates the checksum for the meta.
func (m *meta) sum64() uint64 {
	var h = fnv.New64a()
	_, _ = h.Write((*[unsafe.Offsetof(meta{}.checksum)]byte)(unsafe.Pointer(m))[:])
	return h.Sum64()
}
