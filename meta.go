package bolt

const magic uint32 = 0xED0CDAED

type meta struct {
	magic    uint32
	version  uint32
	pageSize uint32
	flags    uint32
	buckets  pgid
	freelist pgid
	pgid     pgid
	txid     txid
}

// validate checks the marker bytes and version of the meta page to ensure it matches this binary.
func (m *meta) validate() error {
	if m.magic != magic {
		return ErrInvalid
	} else if m.version != version {
		return ErrVersionMismatch
	}
	return nil
}

// copy copies one meta object to another.
func (m *meta) copy(dest *meta) {
	dest.magic = m.magic
	dest.version = m.version
	dest.pageSize = m.pageSize
	dest.buckets = m.buckets
	dest.freelist = m.freelist
	dest.pgid = m.pgid
	dest.txid = m.txid
}

// write writes the meta onto a page.
func (m *meta) write(p *page) {
	// Page id is either going to be 0 or 1 which we can determine by the transaction ID.
	p.id = pgid(m.txid % 2)
	p.flags |= metaPageFlag

	m.copy(p.meta())
}
