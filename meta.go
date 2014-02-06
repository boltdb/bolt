package bolt

const magic uint32 = 0xED0CDAED

type meta struct {
	magic    uint32
	version  uint32
	pageSize uint32
	flags    uint32
	sys      pgid
	free     pgid
	pgid     pgid
	txnid    txnid
}

// validate checks the marker bytes and version of the meta page to ensure it matches this binary.
func (m *meta) validate() error {
	if m.magic != magic {
		return InvalidError
	} else if m.version != version {
		return VersionMismatchError
	}
	return nil
}

// copy copies one meta object to another.
func (m *meta) copy(dest *meta) {
	dest.magic = m.magic
	dest.version = m.version
	dest.pageSize = m.pageSize
	dest.pgid = m.pgid
	dest.free = m.free
	dest.txnid = m.txnid
	dest.sys = m.sys
}

// write writes the meta onto a page.
func (m *meta) write(p *page) {
	// Page id is either going to be 0 or 1 which we can determine by the Txn ID.
	p.id = pgid(m.txnid % 2)
	p.flags |= p_meta

	m.copy(p.meta())
}
