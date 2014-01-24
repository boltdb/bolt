package bolt

var (
	InvalidMetaPageError = &Error{"Invalid meta page", nil}
)

const magic uint32 = 0xC0DEC0DE
const version uint32 = 1

type meta struct {
	magic    uint32
	version  uint32
	buckets  bucket
	pageSize uint32
	pgid     pgid
	txnid    txnid
}

// validate checks the marker bytes and version of the meta page to ensure it matches this binary.
func (m *meta) validate() error {
	if m.magic != magic {
		return InvalidError
	} else if m.version != Version {
		return VersionMismatchError
	}
	return nil
}
