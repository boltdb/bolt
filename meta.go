package bolt

var (
	InvalidError         = &Error{"Invalid database", nil}
	VersionMismatchError = &Error{"version mismatch", nil}
	InvalidMetaPageError = &Error{"invalid meta page", nil}
)

const magic uint32 = 0xC0DEC0DE
const version uint32 = 1

type meta struct {
	magic    uint32
	version  uint32
	sys      bucket
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
