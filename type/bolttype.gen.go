package bolttype

// ForEachBytesToBytes is a for each with key of type []byte and value of type []byte.
func (b *Bucket) ForEachBytesToBytes(fn func(key []byte, value []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := (rawKey)
			value := (rawValue)
			return fn(key, value)
		},
	)
}

// GetBytesToBytes is a get with key of type []byte and value of type []byte.
func (b *Bucket) GetBytesToBytes(key []byte) []byte {
	return (b.Get((key)))
}

// GetRequiredBytesToBytes is a get with key of type []byte and value of type []byte that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesToBytes(key []byte) ([]byte, error) {
	value := b.Get((key))
	if value == nil {
		return nil, NewKeyNotFoundError(key)
	}
	return (value), nil
}

// PutBytesToBytes is a put with key of type []byte and value of type []byte.
func (b *Bucket) PutBytesToBytes(key []byte, value []byte) error {
	return b.Put((key), (value))
}

// FirstBytesToBytes is first with key of type []byte and value of type []byte.
func (c *Cursor) FirstBytesToBytes() ([]byte, []byte) {
	rawKey, rawValue := c.First()
	key := (rawKey)
	value := (rawValue)
	return key, value
}

// LastBytesToBytes is first with key of type []byte and value of type []byte.
func (c *Cursor) LastBytesToBytes() ([]byte, []byte) {
	rawKey, rawValue := c.Last()
	key := (rawKey)
	value := (rawValue)
	return key, value
}

// NextBytesToBytes is first with key of type []byte and value of type []byte.
func (c *Cursor) NextBytesToBytes() ([]byte, []byte) {
	rawKey, rawValue := c.Next()
	key := (rawKey)
	value := (rawValue)
	return key, value
}

// PrevBytesToBytes is first with key of type []byte and value of type []byte.
func (c *Cursor) PrevBytesToBytes() ([]byte, []byte) {
	rawKey, rawValue := c.Prev()
	key := (rawKey)
	value := (rawValue)
	return key, value
}

// SeekBytesToBytes is first with key of type []byte and value of type []byte.
func (c *Cursor) SeekBytesToBytes(key []byte) ([]byte, []byte) {
	rawKey, rawValue := c.Seek((key))
	key = (rawKey)
	value := (rawValue)
	return key, value
}

// ForEachBytesToString is a for each with key of type []byte and value of type string.
func (b *Bucket) ForEachBytesToString(fn func(key []byte, value string) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := (rawKey)
			value := string(rawValue)
			return fn(key, value)
		},
	)
}

// GetBytesToString is a get with key of type []byte and value of type string.
func (b *Bucket) GetBytesToString(key []byte) string {
	return string(b.Get((key)))
}

// GetRequiredBytesToString is a get with key of type []byte and value of type string that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesToString(key []byte) (string, error) {
	value := b.Get((key))
	if value == nil {
		return "", NewKeyNotFoundError(key)
	}
	return string(value), nil
}

// PutBytesToString is a put with key of type []byte and value of type string.
func (b *Bucket) PutBytesToString(key []byte, value string) error {
	return b.Put((key), []byte(value))
}

// FirstBytesToString is first with key of type []byte and value of type string.
func (c *Cursor) FirstBytesToString() ([]byte, string) {
	rawKey, rawValue := c.First()
	key := (rawKey)
	value := string(rawValue)
	return key, value
}

// LastBytesToString is first with key of type []byte and value of type string.
func (c *Cursor) LastBytesToString() ([]byte, string) {
	rawKey, rawValue := c.Last()
	key := (rawKey)
	value := string(rawValue)
	return key, value
}

// NextBytesToString is first with key of type []byte and value of type string.
func (c *Cursor) NextBytesToString() ([]byte, string) {
	rawKey, rawValue := c.Next()
	key := (rawKey)
	value := string(rawValue)
	return key, value
}

// PrevBytesToString is first with key of type []byte and value of type string.
func (c *Cursor) PrevBytesToString() ([]byte, string) {
	rawKey, rawValue := c.Prev()
	key := (rawKey)
	value := string(rawValue)
	return key, value
}

// SeekBytesToString is first with key of type []byte and value of type string.
func (c *Cursor) SeekBytesToString(key []byte) ([]byte, string) {
	rawKey, rawValue := c.Seek((key))
	key = (rawKey)
	value := string(rawValue)
	return key, value
}

// ForEachBytesToUint16 is a for each with key of type []byte and value of type uint16.
func (b *Bucket) ForEachBytesToUint16(fn func(key []byte, value uint16) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := (rawKey)
			value := bytesTouint16(rawValue)
			return fn(key, value)
		},
	)
}

// GetBytesToUint16 is a get with key of type []byte and value of type uint16.
func (b *Bucket) GetBytesToUint16(key []byte) uint16 {
	return bytesTouint16(b.Get((key)))
}

// GetRequiredBytesToUint16 is a get with key of type []byte and value of type uint16 that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesToUint16(key []byte) (uint16, error) {
	value := b.Get((key))
	if value == nil {
		return 0, NewKeyNotFoundError(key)
	}
	return bytesTouint16(value), nil
}

// PutBytesToUint16 is a put with key of type []byte and value of type uint16.
func (b *Bucket) PutBytesToUint16(key []byte, value uint16) error {
	return b.Put((key), uint16Tobytes(value))
}

// FirstBytesToUint16 is first with key of type []byte and value of type uint16.
func (c *Cursor) FirstBytesToUint16() ([]byte, uint16) {
	rawKey, rawValue := c.First()
	key := (rawKey)
	value := bytesTouint16(rawValue)
	return key, value
}

// LastBytesToUint16 is first with key of type []byte and value of type uint16.
func (c *Cursor) LastBytesToUint16() ([]byte, uint16) {
	rawKey, rawValue := c.Last()
	key := (rawKey)
	value := bytesTouint16(rawValue)
	return key, value
}

// NextBytesToUint16 is first with key of type []byte and value of type uint16.
func (c *Cursor) NextBytesToUint16() ([]byte, uint16) {
	rawKey, rawValue := c.Next()
	key := (rawKey)
	value := bytesTouint16(rawValue)
	return key, value
}

// PrevBytesToUint16 is first with key of type []byte and value of type uint16.
func (c *Cursor) PrevBytesToUint16() ([]byte, uint16) {
	rawKey, rawValue := c.Prev()
	key := (rawKey)
	value := bytesTouint16(rawValue)
	return key, value
}

// SeekBytesToUint16 is first with key of type []byte and value of type uint16.
func (c *Cursor) SeekBytesToUint16(key []byte) ([]byte, uint16) {
	rawKey, rawValue := c.Seek((key))
	key = (rawKey)
	value := bytesTouint16(rawValue)
	return key, value
}

// ForEachBytesToUint32 is a for each with key of type []byte and value of type uint32.
func (b *Bucket) ForEachBytesToUint32(fn func(key []byte, value uint32) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := (rawKey)
			value := bytesTouint32(rawValue)
			return fn(key, value)
		},
	)
}

// GetBytesToUint32 is a get with key of type []byte and value of type uint32.
func (b *Bucket) GetBytesToUint32(key []byte) uint32 {
	return bytesTouint32(b.Get((key)))
}

// GetRequiredBytesToUint32 is a get with key of type []byte and value of type uint32 that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesToUint32(key []byte) (uint32, error) {
	value := b.Get((key))
	if value == nil {
		return 0, NewKeyNotFoundError(key)
	}
	return bytesTouint32(value), nil
}

// PutBytesToUint32 is a put with key of type []byte and value of type uint32.
func (b *Bucket) PutBytesToUint32(key []byte, value uint32) error {
	return b.Put((key), uint32Tobytes(value))
}

// FirstBytesToUint32 is first with key of type []byte and value of type uint32.
func (c *Cursor) FirstBytesToUint32() ([]byte, uint32) {
	rawKey, rawValue := c.First()
	key := (rawKey)
	value := bytesTouint32(rawValue)
	return key, value
}

// LastBytesToUint32 is first with key of type []byte and value of type uint32.
func (c *Cursor) LastBytesToUint32() ([]byte, uint32) {
	rawKey, rawValue := c.Last()
	key := (rawKey)
	value := bytesTouint32(rawValue)
	return key, value
}

// NextBytesToUint32 is first with key of type []byte and value of type uint32.
func (c *Cursor) NextBytesToUint32() ([]byte, uint32) {
	rawKey, rawValue := c.Next()
	key := (rawKey)
	value := bytesTouint32(rawValue)
	return key, value
}

// PrevBytesToUint32 is first with key of type []byte and value of type uint32.
func (c *Cursor) PrevBytesToUint32() ([]byte, uint32) {
	rawKey, rawValue := c.Prev()
	key := (rawKey)
	value := bytesTouint32(rawValue)
	return key, value
}

// SeekBytesToUint32 is first with key of type []byte and value of type uint32.
func (c *Cursor) SeekBytesToUint32(key []byte) ([]byte, uint32) {
	rawKey, rawValue := c.Seek((key))
	key = (rawKey)
	value := bytesTouint32(rawValue)
	return key, value
}

// ForEachBytesToUint64 is a for each with key of type []byte and value of type uint64.
func (b *Bucket) ForEachBytesToUint64(fn func(key []byte, value uint64) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := (rawKey)
			value := bytesTouint64(rawValue)
			return fn(key, value)
		},
	)
}

// GetBytesToUint64 is a get with key of type []byte and value of type uint64.
func (b *Bucket) GetBytesToUint64(key []byte) uint64 {
	return bytesTouint64(b.Get((key)))
}

// GetRequiredBytesToUint64 is a get with key of type []byte and value of type uint64 that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesToUint64(key []byte) (uint64, error) {
	value := b.Get((key))
	if value == nil {
		return 0, NewKeyNotFoundError(key)
	}
	return bytesTouint64(value), nil
}

// PutBytesToUint64 is a put with key of type []byte and value of type uint64.
func (b *Bucket) PutBytesToUint64(key []byte, value uint64) error {
	return b.Put((key), uint64Tobytes(value))
}

// FirstBytesToUint64 is first with key of type []byte and value of type uint64.
func (c *Cursor) FirstBytesToUint64() ([]byte, uint64) {
	rawKey, rawValue := c.First()
	key := (rawKey)
	value := bytesTouint64(rawValue)
	return key, value
}

// LastBytesToUint64 is first with key of type []byte and value of type uint64.
func (c *Cursor) LastBytesToUint64() ([]byte, uint64) {
	rawKey, rawValue := c.Last()
	key := (rawKey)
	value := bytesTouint64(rawValue)
	return key, value
}

// NextBytesToUint64 is first with key of type []byte and value of type uint64.
func (c *Cursor) NextBytesToUint64() ([]byte, uint64) {
	rawKey, rawValue := c.Next()
	key := (rawKey)
	value := bytesTouint64(rawValue)
	return key, value
}

// PrevBytesToUint64 is first with key of type []byte and value of type uint64.
func (c *Cursor) PrevBytesToUint64() ([]byte, uint64) {
	rawKey, rawValue := c.Prev()
	key := (rawKey)
	value := bytesTouint64(rawValue)
	return key, value
}

// SeekBytesToUint64 is first with key of type []byte and value of type uint64.
func (c *Cursor) SeekBytesToUint64(key []byte) ([]byte, uint64) {
	rawKey, rawValue := c.Seek((key))
	key = (rawKey)
	value := bytesTouint64(rawValue)
	return key, value
}

// ForEachBytesToUint16Uint16 is a for each with key of type []byte and value of type pair(uint16,uint16).
func (b *Bucket) ForEachBytesToUint16Uint16(fn func(key []byte, value1 uint16, value2 uint16) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := (rawKey)
			value1, value2 := bytesTouint16uint16(rawValue)
			return fn(key, value1, value2)
		},
	)
}

// GetBytesToUint16Uint16 is a get with key of type []byte and value of type pair(uint16,uint16).
func (b *Bucket) GetBytesToUint16Uint16(key []byte) (uint16, uint16) {
	return bytesTouint16uint16(b.Get((key)))
}

// GetRequiredBytesToUint16Uint16 is a get with key of type []byte and value of type pair(uint16,uint16) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesToUint16Uint16(key []byte) (uint16, uint16, error) {
	value := b.Get((key))
	if value == nil {
		return 0, 0, NewKeyNotFoundError(key)
	}
	value1, value2 := bytesTouint16uint16(value)
	return value1, value2, nil
}

// PutBytesToUint16Uint16 is a put with key of type []byte and value of type pair(uint16,uint16).
func (b *Bucket) PutBytesToUint16Uint16(key []byte, value1 uint16, value2 uint16) error {
	return b.Put((key), uint16uint16Tobytes(value1, value2))
}

// FirstBytesToUint16Uint16 is first with key of type []byte and value of type pair(uint16,uint16).
func (c *Cursor) FirstBytesToUint16Uint16() ([]byte, uint16, uint16) {
	rawKey, rawValue := c.First()
	key := (rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key, value1, value2
}

// LastBytesToUint16Uint16 is first with key of type []byte and value of type pair(uint16,uint16).
func (c *Cursor) LastBytesToUint16Uint16() ([]byte, uint16, uint16) {
	rawKey, rawValue := c.Last()
	key := (rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key, value1, value2
}

// NextBytesToUint16Uint16 is first with key of type []byte and value of type pair(uint16,uint16).
func (c *Cursor) NextBytesToUint16Uint16() ([]byte, uint16, uint16) {
	rawKey, rawValue := c.Next()
	key := (rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key, value1, value2
}

// PrevBytesToUint16Uint16 is first with key of type []byte and value of type pair(uint16,uint16).
func (c *Cursor) PrevBytesToUint16Uint16() ([]byte, uint16, uint16) {
	rawKey, rawValue := c.Prev()
	key := (rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key, value1, value2
}

// SeekBytesToUint16Uint16 is first with key of type []byte and value of type pair(uint16,uint16).
func (c *Cursor) SeekBytesToUint16Uint16(key []byte) ([]byte, uint16, uint16) {
	rawKey, rawValue := c.Seek((key))
	key = (rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key, value1, value2
}

// ForEachBytesToUint32Uint32 is a for each with key of type []byte and value of type pair(uint32,uint32).
func (b *Bucket) ForEachBytesToUint32Uint32(fn func(key []byte, value1 uint32, value2 uint32) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := (rawKey)
			value1, value2 := bytesTouint32uint32(rawValue)
			return fn(key, value1, value2)
		},
	)
}

// GetBytesToUint32Uint32 is a get with key of type []byte and value of type pair(uint32,uint32).
func (b *Bucket) GetBytesToUint32Uint32(key []byte) (uint32, uint32) {
	return bytesTouint32uint32(b.Get((key)))
}

// GetRequiredBytesToUint32Uint32 is a get with key of type []byte and value of type pair(uint32,uint32) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesToUint32Uint32(key []byte) (uint32, uint32, error) {
	value := b.Get((key))
	if value == nil {
		return 0, 0, NewKeyNotFoundError(key)
	}
	value1, value2 := bytesTouint32uint32(value)
	return value1, value2, nil
}

// PutBytesToUint32Uint32 is a put with key of type []byte and value of type pair(uint32,uint32).
func (b *Bucket) PutBytesToUint32Uint32(key []byte, value1 uint32, value2 uint32) error {
	return b.Put((key), uint32uint32Tobytes(value1, value2))
}

// FirstBytesToUint32Uint32 is first with key of type []byte and value of type pair(uint32,uint32).
func (c *Cursor) FirstBytesToUint32Uint32() ([]byte, uint32, uint32) {
	rawKey, rawValue := c.First()
	key := (rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key, value1, value2
}

// LastBytesToUint32Uint32 is first with key of type []byte and value of type pair(uint32,uint32).
func (c *Cursor) LastBytesToUint32Uint32() ([]byte, uint32, uint32) {
	rawKey, rawValue := c.Last()
	key := (rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key, value1, value2
}

// NextBytesToUint32Uint32 is first with key of type []byte and value of type pair(uint32,uint32).
func (c *Cursor) NextBytesToUint32Uint32() ([]byte, uint32, uint32) {
	rawKey, rawValue := c.Next()
	key := (rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key, value1, value2
}

// PrevBytesToUint32Uint32 is first with key of type []byte and value of type pair(uint32,uint32).
func (c *Cursor) PrevBytesToUint32Uint32() ([]byte, uint32, uint32) {
	rawKey, rawValue := c.Prev()
	key := (rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key, value1, value2
}

// SeekBytesToUint32Uint32 is first with key of type []byte and value of type pair(uint32,uint32).
func (c *Cursor) SeekBytesToUint32Uint32(key []byte) ([]byte, uint32, uint32) {
	rawKey, rawValue := c.Seek((key))
	key = (rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key, value1, value2
}

// ForEachBytesToUint64Uint64 is a for each with key of type []byte and value of type pair(uint64,uint64).
func (b *Bucket) ForEachBytesToUint64Uint64(fn func(key []byte, value1 uint64, value2 uint64) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := (rawKey)
			value1, value2 := bytesTouint64uint64(rawValue)
			return fn(key, value1, value2)
		},
	)
}

// GetBytesToUint64Uint64 is a get with key of type []byte and value of type pair(uint64,uint64).
func (b *Bucket) GetBytesToUint64Uint64(key []byte) (uint64, uint64) {
	return bytesTouint64uint64(b.Get((key)))
}

// GetRequiredBytesToUint64Uint64 is a get with key of type []byte and value of type pair(uint64,uint64) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesToUint64Uint64(key []byte) (uint64, uint64, error) {
	value := b.Get((key))
	if value == nil {
		return 0, 0, NewKeyNotFoundError(key)
	}
	value1, value2 := bytesTouint64uint64(value)
	return value1, value2, nil
}

// PutBytesToUint64Uint64 is a put with key of type []byte and value of type pair(uint64,uint64).
func (b *Bucket) PutBytesToUint64Uint64(key []byte, value1 uint64, value2 uint64) error {
	return b.Put((key), uint64uint64Tobytes(value1, value2))
}

// FirstBytesToUint64Uint64 is first with key of type []byte and value of type pair(uint64,uint64).
func (c *Cursor) FirstBytesToUint64Uint64() ([]byte, uint64, uint64) {
	rawKey, rawValue := c.First()
	key := (rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key, value1, value2
}

// LastBytesToUint64Uint64 is first with key of type []byte and value of type pair(uint64,uint64).
func (c *Cursor) LastBytesToUint64Uint64() ([]byte, uint64, uint64) {
	rawKey, rawValue := c.Last()
	key := (rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key, value1, value2
}

// NextBytesToUint64Uint64 is first with key of type []byte and value of type pair(uint64,uint64).
func (c *Cursor) NextBytesToUint64Uint64() ([]byte, uint64, uint64) {
	rawKey, rawValue := c.Next()
	key := (rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key, value1, value2
}

// PrevBytesToUint64Uint64 is first with key of type []byte and value of type pair(uint64,uint64).
func (c *Cursor) PrevBytesToUint64Uint64() ([]byte, uint64, uint64) {
	rawKey, rawValue := c.Prev()
	key := (rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key, value1, value2
}

// SeekBytesToUint64Uint64 is first with key of type []byte and value of type pair(uint64,uint64).
func (c *Cursor) SeekBytesToUint64Uint64(key []byte) ([]byte, uint64, uint64) {
	rawKey, rawValue := c.Seek((key))
	key = (rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key, value1, value2
}

// ForEachBytesToUint16Bytes is a for each with key of type []byte and value of type pair(uint16,[]byte).
func (b *Bucket) ForEachBytesToUint16Bytes(fn func(key []byte, value1 uint16, value2 []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := (rawKey)
			value1, value2 := bytesTouint16bytes(rawValue)
			return fn(key, value1, value2)
		},
	)
}

// GetBytesToUint16Bytes is a get with key of type []byte and value of type pair(uint16,[]byte).
func (b *Bucket) GetBytesToUint16Bytes(key []byte) (uint16, []byte) {
	return bytesTouint16bytes(b.Get((key)))
}

// GetRequiredBytesToUint16Bytes is a get with key of type []byte and value of type pair(uint16,[]byte) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesToUint16Bytes(key []byte) (uint16, []byte, error) {
	value := b.Get((key))
	if value == nil {
		return 0, nil, NewKeyNotFoundError(key)
	}
	value1, value2 := bytesTouint16bytes(value)
	return value1, value2, nil
}

// PutBytesToUint16Bytes is a put with key of type []byte and value of type pair(uint16,[]byte).
func (b *Bucket) PutBytesToUint16Bytes(key []byte, value1 uint16, value2 []byte) error {
	return b.Put((key), uint16bytesTobytes(value1, value2))
}

// FirstBytesToUint16Bytes is first with key of type []byte and value of type pair(uint16,[]byte).
func (c *Cursor) FirstBytesToUint16Bytes() ([]byte, uint16, []byte) {
	rawKey, rawValue := c.First()
	key := (rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key, value1, value2
}

// LastBytesToUint16Bytes is first with key of type []byte and value of type pair(uint16,[]byte).
func (c *Cursor) LastBytesToUint16Bytes() ([]byte, uint16, []byte) {
	rawKey, rawValue := c.Last()
	key := (rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key, value1, value2
}

// NextBytesToUint16Bytes is first with key of type []byte and value of type pair(uint16,[]byte).
func (c *Cursor) NextBytesToUint16Bytes() ([]byte, uint16, []byte) {
	rawKey, rawValue := c.Next()
	key := (rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key, value1, value2
}

// PrevBytesToUint16Bytes is first with key of type []byte and value of type pair(uint16,[]byte).
func (c *Cursor) PrevBytesToUint16Bytes() ([]byte, uint16, []byte) {
	rawKey, rawValue := c.Prev()
	key := (rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key, value1, value2
}

// SeekBytesToUint16Bytes is first with key of type []byte and value of type pair(uint16,[]byte).
func (c *Cursor) SeekBytesToUint16Bytes(key []byte) ([]byte, uint16, []byte) {
	rawKey, rawValue := c.Seek((key))
	key = (rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key, value1, value2
}

// ForEachBytesToUint32Bytes is a for each with key of type []byte and value of type pair(uint32,[]byte).
func (b *Bucket) ForEachBytesToUint32Bytes(fn func(key []byte, value1 uint32, value2 []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := (rawKey)
			value1, value2 := bytesTouint32bytes(rawValue)
			return fn(key, value1, value2)
		},
	)
}

// GetBytesToUint32Bytes is a get with key of type []byte and value of type pair(uint32,[]byte).
func (b *Bucket) GetBytesToUint32Bytes(key []byte) (uint32, []byte) {
	return bytesTouint32bytes(b.Get((key)))
}

// GetRequiredBytesToUint32Bytes is a get with key of type []byte and value of type pair(uint32,[]byte) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesToUint32Bytes(key []byte) (uint32, []byte, error) {
	value := b.Get((key))
	if value == nil {
		return 0, nil, NewKeyNotFoundError(key)
	}
	value1, value2 := bytesTouint32bytes(value)
	return value1, value2, nil
}

// PutBytesToUint32Bytes is a put with key of type []byte and value of type pair(uint32,[]byte).
func (b *Bucket) PutBytesToUint32Bytes(key []byte, value1 uint32, value2 []byte) error {
	return b.Put((key), uint32bytesTobytes(value1, value2))
}

// FirstBytesToUint32Bytes is first with key of type []byte and value of type pair(uint32,[]byte).
func (c *Cursor) FirstBytesToUint32Bytes() ([]byte, uint32, []byte) {
	rawKey, rawValue := c.First()
	key := (rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key, value1, value2
}

// LastBytesToUint32Bytes is first with key of type []byte and value of type pair(uint32,[]byte).
func (c *Cursor) LastBytesToUint32Bytes() ([]byte, uint32, []byte) {
	rawKey, rawValue := c.Last()
	key := (rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key, value1, value2
}

// NextBytesToUint32Bytes is first with key of type []byte and value of type pair(uint32,[]byte).
func (c *Cursor) NextBytesToUint32Bytes() ([]byte, uint32, []byte) {
	rawKey, rawValue := c.Next()
	key := (rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key, value1, value2
}

// PrevBytesToUint32Bytes is first with key of type []byte and value of type pair(uint32,[]byte).
func (c *Cursor) PrevBytesToUint32Bytes() ([]byte, uint32, []byte) {
	rawKey, rawValue := c.Prev()
	key := (rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key, value1, value2
}

// SeekBytesToUint32Bytes is first with key of type []byte and value of type pair(uint32,[]byte).
func (c *Cursor) SeekBytesToUint32Bytes(key []byte) ([]byte, uint32, []byte) {
	rawKey, rawValue := c.Seek((key))
	key = (rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key, value1, value2
}

// ForEachBytesToUint64Bytes is a for each with key of type []byte and value of type pair(uint64,[]byte).
func (b *Bucket) ForEachBytesToUint64Bytes(fn func(key []byte, value1 uint64, value2 []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := (rawKey)
			value1, value2 := bytesTouint64bytes(rawValue)
			return fn(key, value1, value2)
		},
	)
}

// GetBytesToUint64Bytes is a get with key of type []byte and value of type pair(uint64,[]byte).
func (b *Bucket) GetBytesToUint64Bytes(key []byte) (uint64, []byte) {
	return bytesTouint64bytes(b.Get((key)))
}

// GetRequiredBytesToUint64Bytes is a get with key of type []byte and value of type pair(uint64,[]byte) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesToUint64Bytes(key []byte) (uint64, []byte, error) {
	value := b.Get((key))
	if value == nil {
		return 0, nil, NewKeyNotFoundError(key)
	}
	value1, value2 := bytesTouint64bytes(value)
	return value1, value2, nil
}

// PutBytesToUint64Bytes is a put with key of type []byte and value of type pair(uint64,[]byte).
func (b *Bucket) PutBytesToUint64Bytes(key []byte, value1 uint64, value2 []byte) error {
	return b.Put((key), uint64bytesTobytes(value1, value2))
}

// FirstBytesToUint64Bytes is first with key of type []byte and value of type pair(uint64,[]byte).
func (c *Cursor) FirstBytesToUint64Bytes() ([]byte, uint64, []byte) {
	rawKey, rawValue := c.First()
	key := (rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key, value1, value2
}

// LastBytesToUint64Bytes is first with key of type []byte and value of type pair(uint64,[]byte).
func (c *Cursor) LastBytesToUint64Bytes() ([]byte, uint64, []byte) {
	rawKey, rawValue := c.Last()
	key := (rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key, value1, value2
}

// NextBytesToUint64Bytes is first with key of type []byte and value of type pair(uint64,[]byte).
func (c *Cursor) NextBytesToUint64Bytes() ([]byte, uint64, []byte) {
	rawKey, rawValue := c.Next()
	key := (rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key, value1, value2
}

// PrevBytesToUint64Bytes is first with key of type []byte and value of type pair(uint64,[]byte).
func (c *Cursor) PrevBytesToUint64Bytes() ([]byte, uint64, []byte) {
	rawKey, rawValue := c.Prev()
	key := (rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key, value1, value2
}

// SeekBytesToUint64Bytes is first with key of type []byte and value of type pair(uint64,[]byte).
func (c *Cursor) SeekBytesToUint64Bytes(key []byte) ([]byte, uint64, []byte) {
	rawKey, rawValue := c.Seek((key))
	key = (rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key, value1, value2
}

// ForEachBytesToBytesUint16 is a for each with key of type []byte and value of type pair([]byte,uint16).
func (b *Bucket) ForEachBytesToBytesUint16(fn func(key []byte, value1 []byte, value2 uint16) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := (rawKey)
			value1, value2 := bytesTobytesuint16(rawValue)
			return fn(key, value1, value2)
		},
	)
}

// GetBytesToBytesUint16 is a get with key of type []byte and value of type pair([]byte,uint16).
func (b *Bucket) GetBytesToBytesUint16(key []byte) ([]byte, uint16) {
	return bytesTobytesuint16(b.Get((key)))
}

// GetRequiredBytesToBytesUint16 is a get with key of type []byte and value of type pair([]byte,uint16) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesToBytesUint16(key []byte) ([]byte, uint16, error) {
	value := b.Get((key))
	if value == nil {
		return nil, 0, NewKeyNotFoundError(key)
	}
	value1, value2 := bytesTobytesuint16(value)
	return value1, value2, nil
}

// PutBytesToBytesUint16 is a put with key of type []byte and value of type pair([]byte,uint16).
func (b *Bucket) PutBytesToBytesUint16(key []byte, value1 []byte, value2 uint16) error {
	return b.Put((key), bytesuint16Tobytes(value1, value2))
}

// FirstBytesToBytesUint16 is first with key of type []byte and value of type pair([]byte,uint16).
func (c *Cursor) FirstBytesToBytesUint16() ([]byte, []byte, uint16) {
	rawKey, rawValue := c.First()
	key := (rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key, value1, value2
}

// LastBytesToBytesUint16 is first with key of type []byte and value of type pair([]byte,uint16).
func (c *Cursor) LastBytesToBytesUint16() ([]byte, []byte, uint16) {
	rawKey, rawValue := c.Last()
	key := (rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key, value1, value2
}

// NextBytesToBytesUint16 is first with key of type []byte and value of type pair([]byte,uint16).
func (c *Cursor) NextBytesToBytesUint16() ([]byte, []byte, uint16) {
	rawKey, rawValue := c.Next()
	key := (rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key, value1, value2
}

// PrevBytesToBytesUint16 is first with key of type []byte and value of type pair([]byte,uint16).
func (c *Cursor) PrevBytesToBytesUint16() ([]byte, []byte, uint16) {
	rawKey, rawValue := c.Prev()
	key := (rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key, value1, value2
}

// SeekBytesToBytesUint16 is first with key of type []byte and value of type pair([]byte,uint16).
func (c *Cursor) SeekBytesToBytesUint16(key []byte) ([]byte, []byte, uint16) {
	rawKey, rawValue := c.Seek((key))
	key = (rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key, value1, value2
}

// ForEachBytesToBytesUint32 is a for each with key of type []byte and value of type pair([]byte,uint32).
func (b *Bucket) ForEachBytesToBytesUint32(fn func(key []byte, value1 []byte, value2 uint32) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := (rawKey)
			value1, value2 := bytesTobytesuint32(rawValue)
			return fn(key, value1, value2)
		},
	)
}

// GetBytesToBytesUint32 is a get with key of type []byte and value of type pair([]byte,uint32).
func (b *Bucket) GetBytesToBytesUint32(key []byte) ([]byte, uint32) {
	return bytesTobytesuint32(b.Get((key)))
}

// GetRequiredBytesToBytesUint32 is a get with key of type []byte and value of type pair([]byte,uint32) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesToBytesUint32(key []byte) ([]byte, uint32, error) {
	value := b.Get((key))
	if value == nil {
		return nil, 0, NewKeyNotFoundError(key)
	}
	value1, value2 := bytesTobytesuint32(value)
	return value1, value2, nil
}

// PutBytesToBytesUint32 is a put with key of type []byte and value of type pair([]byte,uint32).
func (b *Bucket) PutBytesToBytesUint32(key []byte, value1 []byte, value2 uint32) error {
	return b.Put((key), bytesuint32Tobytes(value1, value2))
}

// FirstBytesToBytesUint32 is first with key of type []byte and value of type pair([]byte,uint32).
func (c *Cursor) FirstBytesToBytesUint32() ([]byte, []byte, uint32) {
	rawKey, rawValue := c.First()
	key := (rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key, value1, value2
}

// LastBytesToBytesUint32 is first with key of type []byte and value of type pair([]byte,uint32).
func (c *Cursor) LastBytesToBytesUint32() ([]byte, []byte, uint32) {
	rawKey, rawValue := c.Last()
	key := (rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key, value1, value2
}

// NextBytesToBytesUint32 is first with key of type []byte and value of type pair([]byte,uint32).
func (c *Cursor) NextBytesToBytesUint32() ([]byte, []byte, uint32) {
	rawKey, rawValue := c.Next()
	key := (rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key, value1, value2
}

// PrevBytesToBytesUint32 is first with key of type []byte and value of type pair([]byte,uint32).
func (c *Cursor) PrevBytesToBytesUint32() ([]byte, []byte, uint32) {
	rawKey, rawValue := c.Prev()
	key := (rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key, value1, value2
}

// SeekBytesToBytesUint32 is first with key of type []byte and value of type pair([]byte,uint32).
func (c *Cursor) SeekBytesToBytesUint32(key []byte) ([]byte, []byte, uint32) {
	rawKey, rawValue := c.Seek((key))
	key = (rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key, value1, value2
}

// ForEachBytesToBytesUint64 is a for each with key of type []byte and value of type pair([]byte,uint64).
func (b *Bucket) ForEachBytesToBytesUint64(fn func(key []byte, value1 []byte, value2 uint64) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := (rawKey)
			value1, value2 := bytesTobytesuint64(rawValue)
			return fn(key, value1, value2)
		},
	)
}

// GetBytesToBytesUint64 is a get with key of type []byte and value of type pair([]byte,uint64).
func (b *Bucket) GetBytesToBytesUint64(key []byte) ([]byte, uint64) {
	return bytesTobytesuint64(b.Get((key)))
}

// GetRequiredBytesToBytesUint64 is a get with key of type []byte and value of type pair([]byte,uint64) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesToBytesUint64(key []byte) ([]byte, uint64, error) {
	value := b.Get((key))
	if value == nil {
		return nil, 0, NewKeyNotFoundError(key)
	}
	value1, value2 := bytesTobytesuint64(value)
	return value1, value2, nil
}

// PutBytesToBytesUint64 is a put with key of type []byte and value of type pair([]byte,uint64).
func (b *Bucket) PutBytesToBytesUint64(key []byte, value1 []byte, value2 uint64) error {
	return b.Put((key), bytesuint64Tobytes(value1, value2))
}

// FirstBytesToBytesUint64 is first with key of type []byte and value of type pair([]byte,uint64).
func (c *Cursor) FirstBytesToBytesUint64() ([]byte, []byte, uint64) {
	rawKey, rawValue := c.First()
	key := (rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key, value1, value2
}

// LastBytesToBytesUint64 is first with key of type []byte and value of type pair([]byte,uint64).
func (c *Cursor) LastBytesToBytesUint64() ([]byte, []byte, uint64) {
	rawKey, rawValue := c.Last()
	key := (rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key, value1, value2
}

// NextBytesToBytesUint64 is first with key of type []byte and value of type pair([]byte,uint64).
func (c *Cursor) NextBytesToBytesUint64() ([]byte, []byte, uint64) {
	rawKey, rawValue := c.Next()
	key := (rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key, value1, value2
}

// PrevBytesToBytesUint64 is first with key of type []byte and value of type pair([]byte,uint64).
func (c *Cursor) PrevBytesToBytesUint64() ([]byte, []byte, uint64) {
	rawKey, rawValue := c.Prev()
	key := (rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key, value1, value2
}

// SeekBytesToBytesUint64 is first with key of type []byte and value of type pair([]byte,uint64).
func (c *Cursor) SeekBytesToBytesUint64(key []byte) ([]byte, []byte, uint64) {
	rawKey, rawValue := c.Seek((key))
	key = (rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key, value1, value2
}

// DeleteBytes is a delete with key of type []byte.
func (b *Bucket) DeleteBytes(key []byte) error {
	return b.Delete((key))
}

// ForEachStringToBytes is a for each with key of type string and value of type []byte.
func (b *Bucket) ForEachStringToBytes(fn func(key string, value []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := string(rawKey)
			value := (rawValue)
			return fn(key, value)
		},
	)
}

// GetStringToBytes is a get with key of type string and value of type []byte.
func (b *Bucket) GetStringToBytes(key string) []byte {
	return (b.Get([]byte(key)))
}

// GetRequiredStringToBytes is a get with key of type string and value of type []byte that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredStringToBytes(key string) ([]byte, error) {
	value := b.Get([]byte(key))
	if value == nil {
		return nil, NewKeyNotFoundError(key)
	}
	return (value), nil
}

// PutStringToBytes is a put with key of type string and value of type []byte.
func (b *Bucket) PutStringToBytes(key string, value []byte) error {
	return b.Put([]byte(key), (value))
}

// FirstStringToBytes is first with key of type string and value of type []byte.
func (c *Cursor) FirstStringToBytes() (string, []byte) {
	rawKey, rawValue := c.First()
	key := string(rawKey)
	value := (rawValue)
	return key, value
}

// LastStringToBytes is first with key of type string and value of type []byte.
func (c *Cursor) LastStringToBytes() (string, []byte) {
	rawKey, rawValue := c.Last()
	key := string(rawKey)
	value := (rawValue)
	return key, value
}

// NextStringToBytes is first with key of type string and value of type []byte.
func (c *Cursor) NextStringToBytes() (string, []byte) {
	rawKey, rawValue := c.Next()
	key := string(rawKey)
	value := (rawValue)
	return key, value
}

// PrevStringToBytes is first with key of type string and value of type []byte.
func (c *Cursor) PrevStringToBytes() (string, []byte) {
	rawKey, rawValue := c.Prev()
	key := string(rawKey)
	value := (rawValue)
	return key, value
}

// SeekStringToBytes is first with key of type string and value of type []byte.
func (c *Cursor) SeekStringToBytes(key string) (string, []byte) {
	rawKey, rawValue := c.Seek([]byte(key))
	key = string(rawKey)
	value := (rawValue)
	return key, value
}

// ForEachStringToString is a for each with key of type string and value of type string.
func (b *Bucket) ForEachStringToString(fn func(key string, value string) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := string(rawKey)
			value := string(rawValue)
			return fn(key, value)
		},
	)
}

// GetStringToString is a get with key of type string and value of type string.
func (b *Bucket) GetStringToString(key string) string {
	return string(b.Get([]byte(key)))
}

// GetRequiredStringToString is a get with key of type string and value of type string that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredStringToString(key string) (string, error) {
	value := b.Get([]byte(key))
	if value == nil {
		return "", NewKeyNotFoundError(key)
	}
	return string(value), nil
}

// PutStringToString is a put with key of type string and value of type string.
func (b *Bucket) PutStringToString(key string, value string) error {
	return b.Put([]byte(key), []byte(value))
}

// FirstStringToString is first with key of type string and value of type string.
func (c *Cursor) FirstStringToString() (string, string) {
	rawKey, rawValue := c.First()
	key := string(rawKey)
	value := string(rawValue)
	return key, value
}

// LastStringToString is first with key of type string and value of type string.
func (c *Cursor) LastStringToString() (string, string) {
	rawKey, rawValue := c.Last()
	key := string(rawKey)
	value := string(rawValue)
	return key, value
}

// NextStringToString is first with key of type string and value of type string.
func (c *Cursor) NextStringToString() (string, string) {
	rawKey, rawValue := c.Next()
	key := string(rawKey)
	value := string(rawValue)
	return key, value
}

// PrevStringToString is first with key of type string and value of type string.
func (c *Cursor) PrevStringToString() (string, string) {
	rawKey, rawValue := c.Prev()
	key := string(rawKey)
	value := string(rawValue)
	return key, value
}

// SeekStringToString is first with key of type string and value of type string.
func (c *Cursor) SeekStringToString(key string) (string, string) {
	rawKey, rawValue := c.Seek([]byte(key))
	key = string(rawKey)
	value := string(rawValue)
	return key, value
}

// ForEachStringToUint16 is a for each with key of type string and value of type uint16.
func (b *Bucket) ForEachStringToUint16(fn func(key string, value uint16) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := string(rawKey)
			value := bytesTouint16(rawValue)
			return fn(key, value)
		},
	)
}

// GetStringToUint16 is a get with key of type string and value of type uint16.
func (b *Bucket) GetStringToUint16(key string) uint16 {
	return bytesTouint16(b.Get([]byte(key)))
}

// GetRequiredStringToUint16 is a get with key of type string and value of type uint16 that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredStringToUint16(key string) (uint16, error) {
	value := b.Get([]byte(key))
	if value == nil {
		return 0, NewKeyNotFoundError(key)
	}
	return bytesTouint16(value), nil
}

// PutStringToUint16 is a put with key of type string and value of type uint16.
func (b *Bucket) PutStringToUint16(key string, value uint16) error {
	return b.Put([]byte(key), uint16Tobytes(value))
}

// FirstStringToUint16 is first with key of type string and value of type uint16.
func (c *Cursor) FirstStringToUint16() (string, uint16) {
	rawKey, rawValue := c.First()
	key := string(rawKey)
	value := bytesTouint16(rawValue)
	return key, value
}

// LastStringToUint16 is first with key of type string and value of type uint16.
func (c *Cursor) LastStringToUint16() (string, uint16) {
	rawKey, rawValue := c.Last()
	key := string(rawKey)
	value := bytesTouint16(rawValue)
	return key, value
}

// NextStringToUint16 is first with key of type string and value of type uint16.
func (c *Cursor) NextStringToUint16() (string, uint16) {
	rawKey, rawValue := c.Next()
	key := string(rawKey)
	value := bytesTouint16(rawValue)
	return key, value
}

// PrevStringToUint16 is first with key of type string and value of type uint16.
func (c *Cursor) PrevStringToUint16() (string, uint16) {
	rawKey, rawValue := c.Prev()
	key := string(rawKey)
	value := bytesTouint16(rawValue)
	return key, value
}

// SeekStringToUint16 is first with key of type string and value of type uint16.
func (c *Cursor) SeekStringToUint16(key string) (string, uint16) {
	rawKey, rawValue := c.Seek([]byte(key))
	key = string(rawKey)
	value := bytesTouint16(rawValue)
	return key, value
}

// ForEachStringToUint32 is a for each with key of type string and value of type uint32.
func (b *Bucket) ForEachStringToUint32(fn func(key string, value uint32) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := string(rawKey)
			value := bytesTouint32(rawValue)
			return fn(key, value)
		},
	)
}

// GetStringToUint32 is a get with key of type string and value of type uint32.
func (b *Bucket) GetStringToUint32(key string) uint32 {
	return bytesTouint32(b.Get([]byte(key)))
}

// GetRequiredStringToUint32 is a get with key of type string and value of type uint32 that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredStringToUint32(key string) (uint32, error) {
	value := b.Get([]byte(key))
	if value == nil {
		return 0, NewKeyNotFoundError(key)
	}
	return bytesTouint32(value), nil
}

// PutStringToUint32 is a put with key of type string and value of type uint32.
func (b *Bucket) PutStringToUint32(key string, value uint32) error {
	return b.Put([]byte(key), uint32Tobytes(value))
}

// FirstStringToUint32 is first with key of type string and value of type uint32.
func (c *Cursor) FirstStringToUint32() (string, uint32) {
	rawKey, rawValue := c.First()
	key := string(rawKey)
	value := bytesTouint32(rawValue)
	return key, value
}

// LastStringToUint32 is first with key of type string and value of type uint32.
func (c *Cursor) LastStringToUint32() (string, uint32) {
	rawKey, rawValue := c.Last()
	key := string(rawKey)
	value := bytesTouint32(rawValue)
	return key, value
}

// NextStringToUint32 is first with key of type string and value of type uint32.
func (c *Cursor) NextStringToUint32() (string, uint32) {
	rawKey, rawValue := c.Next()
	key := string(rawKey)
	value := bytesTouint32(rawValue)
	return key, value
}

// PrevStringToUint32 is first with key of type string and value of type uint32.
func (c *Cursor) PrevStringToUint32() (string, uint32) {
	rawKey, rawValue := c.Prev()
	key := string(rawKey)
	value := bytesTouint32(rawValue)
	return key, value
}

// SeekStringToUint32 is first with key of type string and value of type uint32.
func (c *Cursor) SeekStringToUint32(key string) (string, uint32) {
	rawKey, rawValue := c.Seek([]byte(key))
	key = string(rawKey)
	value := bytesTouint32(rawValue)
	return key, value
}

// ForEachStringToUint64 is a for each with key of type string and value of type uint64.
func (b *Bucket) ForEachStringToUint64(fn func(key string, value uint64) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := string(rawKey)
			value := bytesTouint64(rawValue)
			return fn(key, value)
		},
	)
}

// GetStringToUint64 is a get with key of type string and value of type uint64.
func (b *Bucket) GetStringToUint64(key string) uint64 {
	return bytesTouint64(b.Get([]byte(key)))
}

// GetRequiredStringToUint64 is a get with key of type string and value of type uint64 that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredStringToUint64(key string) (uint64, error) {
	value := b.Get([]byte(key))
	if value == nil {
		return 0, NewKeyNotFoundError(key)
	}
	return bytesTouint64(value), nil
}

// PutStringToUint64 is a put with key of type string and value of type uint64.
func (b *Bucket) PutStringToUint64(key string, value uint64) error {
	return b.Put([]byte(key), uint64Tobytes(value))
}

// FirstStringToUint64 is first with key of type string and value of type uint64.
func (c *Cursor) FirstStringToUint64() (string, uint64) {
	rawKey, rawValue := c.First()
	key := string(rawKey)
	value := bytesTouint64(rawValue)
	return key, value
}

// LastStringToUint64 is first with key of type string and value of type uint64.
func (c *Cursor) LastStringToUint64() (string, uint64) {
	rawKey, rawValue := c.Last()
	key := string(rawKey)
	value := bytesTouint64(rawValue)
	return key, value
}

// NextStringToUint64 is first with key of type string and value of type uint64.
func (c *Cursor) NextStringToUint64() (string, uint64) {
	rawKey, rawValue := c.Next()
	key := string(rawKey)
	value := bytesTouint64(rawValue)
	return key, value
}

// PrevStringToUint64 is first with key of type string and value of type uint64.
func (c *Cursor) PrevStringToUint64() (string, uint64) {
	rawKey, rawValue := c.Prev()
	key := string(rawKey)
	value := bytesTouint64(rawValue)
	return key, value
}

// SeekStringToUint64 is first with key of type string and value of type uint64.
func (c *Cursor) SeekStringToUint64(key string) (string, uint64) {
	rawKey, rawValue := c.Seek([]byte(key))
	key = string(rawKey)
	value := bytesTouint64(rawValue)
	return key, value
}

// ForEachStringToUint16Uint16 is a for each with key of type string and value of type pair(uint16,uint16).
func (b *Bucket) ForEachStringToUint16Uint16(fn func(key string, value1 uint16, value2 uint16) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := string(rawKey)
			value1, value2 := bytesTouint16uint16(rawValue)
			return fn(key, value1, value2)
		},
	)
}

// GetStringToUint16Uint16 is a get with key of type string and value of type pair(uint16,uint16).
func (b *Bucket) GetStringToUint16Uint16(key string) (uint16, uint16) {
	return bytesTouint16uint16(b.Get([]byte(key)))
}

// GetRequiredStringToUint16Uint16 is a get with key of type string and value of type pair(uint16,uint16) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredStringToUint16Uint16(key string) (uint16, uint16, error) {
	value := b.Get([]byte(key))
	if value == nil {
		return 0, 0, NewKeyNotFoundError(key)
	}
	value1, value2 := bytesTouint16uint16(value)
	return value1, value2, nil
}

// PutStringToUint16Uint16 is a put with key of type string and value of type pair(uint16,uint16).
func (b *Bucket) PutStringToUint16Uint16(key string, value1 uint16, value2 uint16) error {
	return b.Put([]byte(key), uint16uint16Tobytes(value1, value2))
}

// FirstStringToUint16Uint16 is first with key of type string and value of type pair(uint16,uint16).
func (c *Cursor) FirstStringToUint16Uint16() (string, uint16, uint16) {
	rawKey, rawValue := c.First()
	key := string(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key, value1, value2
}

// LastStringToUint16Uint16 is first with key of type string and value of type pair(uint16,uint16).
func (c *Cursor) LastStringToUint16Uint16() (string, uint16, uint16) {
	rawKey, rawValue := c.Last()
	key := string(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key, value1, value2
}

// NextStringToUint16Uint16 is first with key of type string and value of type pair(uint16,uint16).
func (c *Cursor) NextStringToUint16Uint16() (string, uint16, uint16) {
	rawKey, rawValue := c.Next()
	key := string(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key, value1, value2
}

// PrevStringToUint16Uint16 is first with key of type string and value of type pair(uint16,uint16).
func (c *Cursor) PrevStringToUint16Uint16() (string, uint16, uint16) {
	rawKey, rawValue := c.Prev()
	key := string(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key, value1, value2
}

// SeekStringToUint16Uint16 is first with key of type string and value of type pair(uint16,uint16).
func (c *Cursor) SeekStringToUint16Uint16(key string) (string, uint16, uint16) {
	rawKey, rawValue := c.Seek([]byte(key))
	key = string(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key, value1, value2
}

// ForEachStringToUint32Uint32 is a for each with key of type string and value of type pair(uint32,uint32).
func (b *Bucket) ForEachStringToUint32Uint32(fn func(key string, value1 uint32, value2 uint32) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := string(rawKey)
			value1, value2 := bytesTouint32uint32(rawValue)
			return fn(key, value1, value2)
		},
	)
}

// GetStringToUint32Uint32 is a get with key of type string and value of type pair(uint32,uint32).
func (b *Bucket) GetStringToUint32Uint32(key string) (uint32, uint32) {
	return bytesTouint32uint32(b.Get([]byte(key)))
}

// GetRequiredStringToUint32Uint32 is a get with key of type string and value of type pair(uint32,uint32) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredStringToUint32Uint32(key string) (uint32, uint32, error) {
	value := b.Get([]byte(key))
	if value == nil {
		return 0, 0, NewKeyNotFoundError(key)
	}
	value1, value2 := bytesTouint32uint32(value)
	return value1, value2, nil
}

// PutStringToUint32Uint32 is a put with key of type string and value of type pair(uint32,uint32).
func (b *Bucket) PutStringToUint32Uint32(key string, value1 uint32, value2 uint32) error {
	return b.Put([]byte(key), uint32uint32Tobytes(value1, value2))
}

// FirstStringToUint32Uint32 is first with key of type string and value of type pair(uint32,uint32).
func (c *Cursor) FirstStringToUint32Uint32() (string, uint32, uint32) {
	rawKey, rawValue := c.First()
	key := string(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key, value1, value2
}

// LastStringToUint32Uint32 is first with key of type string and value of type pair(uint32,uint32).
func (c *Cursor) LastStringToUint32Uint32() (string, uint32, uint32) {
	rawKey, rawValue := c.Last()
	key := string(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key, value1, value2
}

// NextStringToUint32Uint32 is first with key of type string and value of type pair(uint32,uint32).
func (c *Cursor) NextStringToUint32Uint32() (string, uint32, uint32) {
	rawKey, rawValue := c.Next()
	key := string(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key, value1, value2
}

// PrevStringToUint32Uint32 is first with key of type string and value of type pair(uint32,uint32).
func (c *Cursor) PrevStringToUint32Uint32() (string, uint32, uint32) {
	rawKey, rawValue := c.Prev()
	key := string(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key, value1, value2
}

// SeekStringToUint32Uint32 is first with key of type string and value of type pair(uint32,uint32).
func (c *Cursor) SeekStringToUint32Uint32(key string) (string, uint32, uint32) {
	rawKey, rawValue := c.Seek([]byte(key))
	key = string(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key, value1, value2
}

// ForEachStringToUint64Uint64 is a for each with key of type string and value of type pair(uint64,uint64).
func (b *Bucket) ForEachStringToUint64Uint64(fn func(key string, value1 uint64, value2 uint64) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := string(rawKey)
			value1, value2 := bytesTouint64uint64(rawValue)
			return fn(key, value1, value2)
		},
	)
}

// GetStringToUint64Uint64 is a get with key of type string and value of type pair(uint64,uint64).
func (b *Bucket) GetStringToUint64Uint64(key string) (uint64, uint64) {
	return bytesTouint64uint64(b.Get([]byte(key)))
}

// GetRequiredStringToUint64Uint64 is a get with key of type string and value of type pair(uint64,uint64) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredStringToUint64Uint64(key string) (uint64, uint64, error) {
	value := b.Get([]byte(key))
	if value == nil {
		return 0, 0, NewKeyNotFoundError(key)
	}
	value1, value2 := bytesTouint64uint64(value)
	return value1, value2, nil
}

// PutStringToUint64Uint64 is a put with key of type string and value of type pair(uint64,uint64).
func (b *Bucket) PutStringToUint64Uint64(key string, value1 uint64, value2 uint64) error {
	return b.Put([]byte(key), uint64uint64Tobytes(value1, value2))
}

// FirstStringToUint64Uint64 is first with key of type string and value of type pair(uint64,uint64).
func (c *Cursor) FirstStringToUint64Uint64() (string, uint64, uint64) {
	rawKey, rawValue := c.First()
	key := string(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key, value1, value2
}

// LastStringToUint64Uint64 is first with key of type string and value of type pair(uint64,uint64).
func (c *Cursor) LastStringToUint64Uint64() (string, uint64, uint64) {
	rawKey, rawValue := c.Last()
	key := string(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key, value1, value2
}

// NextStringToUint64Uint64 is first with key of type string and value of type pair(uint64,uint64).
func (c *Cursor) NextStringToUint64Uint64() (string, uint64, uint64) {
	rawKey, rawValue := c.Next()
	key := string(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key, value1, value2
}

// PrevStringToUint64Uint64 is first with key of type string and value of type pair(uint64,uint64).
func (c *Cursor) PrevStringToUint64Uint64() (string, uint64, uint64) {
	rawKey, rawValue := c.Prev()
	key := string(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key, value1, value2
}

// SeekStringToUint64Uint64 is first with key of type string and value of type pair(uint64,uint64).
func (c *Cursor) SeekStringToUint64Uint64(key string) (string, uint64, uint64) {
	rawKey, rawValue := c.Seek([]byte(key))
	key = string(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key, value1, value2
}

// ForEachStringToUint16Bytes is a for each with key of type string and value of type pair(uint16,[]byte).
func (b *Bucket) ForEachStringToUint16Bytes(fn func(key string, value1 uint16, value2 []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := string(rawKey)
			value1, value2 := bytesTouint16bytes(rawValue)
			return fn(key, value1, value2)
		},
	)
}

// GetStringToUint16Bytes is a get with key of type string and value of type pair(uint16,[]byte).
func (b *Bucket) GetStringToUint16Bytes(key string) (uint16, []byte) {
	return bytesTouint16bytes(b.Get([]byte(key)))
}

// GetRequiredStringToUint16Bytes is a get with key of type string and value of type pair(uint16,[]byte) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredStringToUint16Bytes(key string) (uint16, []byte, error) {
	value := b.Get([]byte(key))
	if value == nil {
		return 0, nil, NewKeyNotFoundError(key)
	}
	value1, value2 := bytesTouint16bytes(value)
	return value1, value2, nil
}

// PutStringToUint16Bytes is a put with key of type string and value of type pair(uint16,[]byte).
func (b *Bucket) PutStringToUint16Bytes(key string, value1 uint16, value2 []byte) error {
	return b.Put([]byte(key), uint16bytesTobytes(value1, value2))
}

// FirstStringToUint16Bytes is first with key of type string and value of type pair(uint16,[]byte).
func (c *Cursor) FirstStringToUint16Bytes() (string, uint16, []byte) {
	rawKey, rawValue := c.First()
	key := string(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key, value1, value2
}

// LastStringToUint16Bytes is first with key of type string and value of type pair(uint16,[]byte).
func (c *Cursor) LastStringToUint16Bytes() (string, uint16, []byte) {
	rawKey, rawValue := c.Last()
	key := string(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key, value1, value2
}

// NextStringToUint16Bytes is first with key of type string and value of type pair(uint16,[]byte).
func (c *Cursor) NextStringToUint16Bytes() (string, uint16, []byte) {
	rawKey, rawValue := c.Next()
	key := string(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key, value1, value2
}

// PrevStringToUint16Bytes is first with key of type string and value of type pair(uint16,[]byte).
func (c *Cursor) PrevStringToUint16Bytes() (string, uint16, []byte) {
	rawKey, rawValue := c.Prev()
	key := string(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key, value1, value2
}

// SeekStringToUint16Bytes is first with key of type string and value of type pair(uint16,[]byte).
func (c *Cursor) SeekStringToUint16Bytes(key string) (string, uint16, []byte) {
	rawKey, rawValue := c.Seek([]byte(key))
	key = string(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key, value1, value2
}

// ForEachStringToUint32Bytes is a for each with key of type string and value of type pair(uint32,[]byte).
func (b *Bucket) ForEachStringToUint32Bytes(fn func(key string, value1 uint32, value2 []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := string(rawKey)
			value1, value2 := bytesTouint32bytes(rawValue)
			return fn(key, value1, value2)
		},
	)
}

// GetStringToUint32Bytes is a get with key of type string and value of type pair(uint32,[]byte).
func (b *Bucket) GetStringToUint32Bytes(key string) (uint32, []byte) {
	return bytesTouint32bytes(b.Get([]byte(key)))
}

// GetRequiredStringToUint32Bytes is a get with key of type string and value of type pair(uint32,[]byte) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredStringToUint32Bytes(key string) (uint32, []byte, error) {
	value := b.Get([]byte(key))
	if value == nil {
		return 0, nil, NewKeyNotFoundError(key)
	}
	value1, value2 := bytesTouint32bytes(value)
	return value1, value2, nil
}

// PutStringToUint32Bytes is a put with key of type string and value of type pair(uint32,[]byte).
func (b *Bucket) PutStringToUint32Bytes(key string, value1 uint32, value2 []byte) error {
	return b.Put([]byte(key), uint32bytesTobytes(value1, value2))
}

// FirstStringToUint32Bytes is first with key of type string and value of type pair(uint32,[]byte).
func (c *Cursor) FirstStringToUint32Bytes() (string, uint32, []byte) {
	rawKey, rawValue := c.First()
	key := string(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key, value1, value2
}

// LastStringToUint32Bytes is first with key of type string and value of type pair(uint32,[]byte).
func (c *Cursor) LastStringToUint32Bytes() (string, uint32, []byte) {
	rawKey, rawValue := c.Last()
	key := string(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key, value1, value2
}

// NextStringToUint32Bytes is first with key of type string and value of type pair(uint32,[]byte).
func (c *Cursor) NextStringToUint32Bytes() (string, uint32, []byte) {
	rawKey, rawValue := c.Next()
	key := string(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key, value1, value2
}

// PrevStringToUint32Bytes is first with key of type string and value of type pair(uint32,[]byte).
func (c *Cursor) PrevStringToUint32Bytes() (string, uint32, []byte) {
	rawKey, rawValue := c.Prev()
	key := string(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key, value1, value2
}

// SeekStringToUint32Bytes is first with key of type string and value of type pair(uint32,[]byte).
func (c *Cursor) SeekStringToUint32Bytes(key string) (string, uint32, []byte) {
	rawKey, rawValue := c.Seek([]byte(key))
	key = string(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key, value1, value2
}

// ForEachStringToUint64Bytes is a for each with key of type string and value of type pair(uint64,[]byte).
func (b *Bucket) ForEachStringToUint64Bytes(fn func(key string, value1 uint64, value2 []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := string(rawKey)
			value1, value2 := bytesTouint64bytes(rawValue)
			return fn(key, value1, value2)
		},
	)
}

// GetStringToUint64Bytes is a get with key of type string and value of type pair(uint64,[]byte).
func (b *Bucket) GetStringToUint64Bytes(key string) (uint64, []byte) {
	return bytesTouint64bytes(b.Get([]byte(key)))
}

// GetRequiredStringToUint64Bytes is a get with key of type string and value of type pair(uint64,[]byte) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredStringToUint64Bytes(key string) (uint64, []byte, error) {
	value := b.Get([]byte(key))
	if value == nil {
		return 0, nil, NewKeyNotFoundError(key)
	}
	value1, value2 := bytesTouint64bytes(value)
	return value1, value2, nil
}

// PutStringToUint64Bytes is a put with key of type string and value of type pair(uint64,[]byte).
func (b *Bucket) PutStringToUint64Bytes(key string, value1 uint64, value2 []byte) error {
	return b.Put([]byte(key), uint64bytesTobytes(value1, value2))
}

// FirstStringToUint64Bytes is first with key of type string and value of type pair(uint64,[]byte).
func (c *Cursor) FirstStringToUint64Bytes() (string, uint64, []byte) {
	rawKey, rawValue := c.First()
	key := string(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key, value1, value2
}

// LastStringToUint64Bytes is first with key of type string and value of type pair(uint64,[]byte).
func (c *Cursor) LastStringToUint64Bytes() (string, uint64, []byte) {
	rawKey, rawValue := c.Last()
	key := string(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key, value1, value2
}

// NextStringToUint64Bytes is first with key of type string and value of type pair(uint64,[]byte).
func (c *Cursor) NextStringToUint64Bytes() (string, uint64, []byte) {
	rawKey, rawValue := c.Next()
	key := string(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key, value1, value2
}

// PrevStringToUint64Bytes is first with key of type string and value of type pair(uint64,[]byte).
func (c *Cursor) PrevStringToUint64Bytes() (string, uint64, []byte) {
	rawKey, rawValue := c.Prev()
	key := string(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key, value1, value2
}

// SeekStringToUint64Bytes is first with key of type string and value of type pair(uint64,[]byte).
func (c *Cursor) SeekStringToUint64Bytes(key string) (string, uint64, []byte) {
	rawKey, rawValue := c.Seek([]byte(key))
	key = string(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key, value1, value2
}

// ForEachStringToBytesUint16 is a for each with key of type string and value of type pair([]byte,uint16).
func (b *Bucket) ForEachStringToBytesUint16(fn func(key string, value1 []byte, value2 uint16) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := string(rawKey)
			value1, value2 := bytesTobytesuint16(rawValue)
			return fn(key, value1, value2)
		},
	)
}

// GetStringToBytesUint16 is a get with key of type string and value of type pair([]byte,uint16).
func (b *Bucket) GetStringToBytesUint16(key string) ([]byte, uint16) {
	return bytesTobytesuint16(b.Get([]byte(key)))
}

// GetRequiredStringToBytesUint16 is a get with key of type string and value of type pair([]byte,uint16) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredStringToBytesUint16(key string) ([]byte, uint16, error) {
	value := b.Get([]byte(key))
	if value == nil {
		return nil, 0, NewKeyNotFoundError(key)
	}
	value1, value2 := bytesTobytesuint16(value)
	return value1, value2, nil
}

// PutStringToBytesUint16 is a put with key of type string and value of type pair([]byte,uint16).
func (b *Bucket) PutStringToBytesUint16(key string, value1 []byte, value2 uint16) error {
	return b.Put([]byte(key), bytesuint16Tobytes(value1, value2))
}

// FirstStringToBytesUint16 is first with key of type string and value of type pair([]byte,uint16).
func (c *Cursor) FirstStringToBytesUint16() (string, []byte, uint16) {
	rawKey, rawValue := c.First()
	key := string(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key, value1, value2
}

// LastStringToBytesUint16 is first with key of type string and value of type pair([]byte,uint16).
func (c *Cursor) LastStringToBytesUint16() (string, []byte, uint16) {
	rawKey, rawValue := c.Last()
	key := string(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key, value1, value2
}

// NextStringToBytesUint16 is first with key of type string and value of type pair([]byte,uint16).
func (c *Cursor) NextStringToBytesUint16() (string, []byte, uint16) {
	rawKey, rawValue := c.Next()
	key := string(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key, value1, value2
}

// PrevStringToBytesUint16 is first with key of type string and value of type pair([]byte,uint16).
func (c *Cursor) PrevStringToBytesUint16() (string, []byte, uint16) {
	rawKey, rawValue := c.Prev()
	key := string(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key, value1, value2
}

// SeekStringToBytesUint16 is first with key of type string and value of type pair([]byte,uint16).
func (c *Cursor) SeekStringToBytesUint16(key string) (string, []byte, uint16) {
	rawKey, rawValue := c.Seek([]byte(key))
	key = string(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key, value1, value2
}

// ForEachStringToBytesUint32 is a for each with key of type string and value of type pair([]byte,uint32).
func (b *Bucket) ForEachStringToBytesUint32(fn func(key string, value1 []byte, value2 uint32) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := string(rawKey)
			value1, value2 := bytesTobytesuint32(rawValue)
			return fn(key, value1, value2)
		},
	)
}

// GetStringToBytesUint32 is a get with key of type string and value of type pair([]byte,uint32).
func (b *Bucket) GetStringToBytesUint32(key string) ([]byte, uint32) {
	return bytesTobytesuint32(b.Get([]byte(key)))
}

// GetRequiredStringToBytesUint32 is a get with key of type string and value of type pair([]byte,uint32) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredStringToBytesUint32(key string) ([]byte, uint32, error) {
	value := b.Get([]byte(key))
	if value == nil {
		return nil, 0, NewKeyNotFoundError(key)
	}
	value1, value2 := bytesTobytesuint32(value)
	return value1, value2, nil
}

// PutStringToBytesUint32 is a put with key of type string and value of type pair([]byte,uint32).
func (b *Bucket) PutStringToBytesUint32(key string, value1 []byte, value2 uint32) error {
	return b.Put([]byte(key), bytesuint32Tobytes(value1, value2))
}

// FirstStringToBytesUint32 is first with key of type string and value of type pair([]byte,uint32).
func (c *Cursor) FirstStringToBytesUint32() (string, []byte, uint32) {
	rawKey, rawValue := c.First()
	key := string(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key, value1, value2
}

// LastStringToBytesUint32 is first with key of type string and value of type pair([]byte,uint32).
func (c *Cursor) LastStringToBytesUint32() (string, []byte, uint32) {
	rawKey, rawValue := c.Last()
	key := string(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key, value1, value2
}

// NextStringToBytesUint32 is first with key of type string and value of type pair([]byte,uint32).
func (c *Cursor) NextStringToBytesUint32() (string, []byte, uint32) {
	rawKey, rawValue := c.Next()
	key := string(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key, value1, value2
}

// PrevStringToBytesUint32 is first with key of type string and value of type pair([]byte,uint32).
func (c *Cursor) PrevStringToBytesUint32() (string, []byte, uint32) {
	rawKey, rawValue := c.Prev()
	key := string(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key, value1, value2
}

// SeekStringToBytesUint32 is first with key of type string and value of type pair([]byte,uint32).
func (c *Cursor) SeekStringToBytesUint32(key string) (string, []byte, uint32) {
	rawKey, rawValue := c.Seek([]byte(key))
	key = string(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key, value1, value2
}

// ForEachStringToBytesUint64 is a for each with key of type string and value of type pair([]byte,uint64).
func (b *Bucket) ForEachStringToBytesUint64(fn func(key string, value1 []byte, value2 uint64) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := string(rawKey)
			value1, value2 := bytesTobytesuint64(rawValue)
			return fn(key, value1, value2)
		},
	)
}

// GetStringToBytesUint64 is a get with key of type string and value of type pair([]byte,uint64).
func (b *Bucket) GetStringToBytesUint64(key string) ([]byte, uint64) {
	return bytesTobytesuint64(b.Get([]byte(key)))
}

// GetRequiredStringToBytesUint64 is a get with key of type string and value of type pair([]byte,uint64) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredStringToBytesUint64(key string) ([]byte, uint64, error) {
	value := b.Get([]byte(key))
	if value == nil {
		return nil, 0, NewKeyNotFoundError(key)
	}
	value1, value2 := bytesTobytesuint64(value)
	return value1, value2, nil
}

// PutStringToBytesUint64 is a put with key of type string and value of type pair([]byte,uint64).
func (b *Bucket) PutStringToBytesUint64(key string, value1 []byte, value2 uint64) error {
	return b.Put([]byte(key), bytesuint64Tobytes(value1, value2))
}

// FirstStringToBytesUint64 is first with key of type string and value of type pair([]byte,uint64).
func (c *Cursor) FirstStringToBytesUint64() (string, []byte, uint64) {
	rawKey, rawValue := c.First()
	key := string(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key, value1, value2
}

// LastStringToBytesUint64 is first with key of type string and value of type pair([]byte,uint64).
func (c *Cursor) LastStringToBytesUint64() (string, []byte, uint64) {
	rawKey, rawValue := c.Last()
	key := string(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key, value1, value2
}

// NextStringToBytesUint64 is first with key of type string and value of type pair([]byte,uint64).
func (c *Cursor) NextStringToBytesUint64() (string, []byte, uint64) {
	rawKey, rawValue := c.Next()
	key := string(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key, value1, value2
}

// PrevStringToBytesUint64 is first with key of type string and value of type pair([]byte,uint64).
func (c *Cursor) PrevStringToBytesUint64() (string, []byte, uint64) {
	rawKey, rawValue := c.Prev()
	key := string(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key, value1, value2
}

// SeekStringToBytesUint64 is first with key of type string and value of type pair([]byte,uint64).
func (c *Cursor) SeekStringToBytesUint64(key string) (string, []byte, uint64) {
	rawKey, rawValue := c.Seek([]byte(key))
	key = string(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key, value1, value2
}

// DeleteString is a delete with key of type string.
func (b *Bucket) DeleteString(key string) error {
	return b.Delete([]byte(key))
}

// ForEachUint16ToBytes is a for each with key of type uint16 and value of type []byte.
func (b *Bucket) ForEachUint16ToBytes(fn func(key uint16, value []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := bytesTouint16(rawKey)
			value := (rawValue)
			return fn(key, value)
		},
	)
}

// GetUint16ToBytes is a get with key of type uint16 and value of type []byte.
func (b *Bucket) GetUint16ToBytes(key uint16) []byte {
	return (b.Get(uint16Tobytes(key)))
}

// GetRequiredUint16ToBytes is a get with key of type uint16 and value of type []byte that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint16ToBytes(key uint16) ([]byte, error) {
	value := b.Get(uint16Tobytes(key))
	if value == nil {
		return nil, NewKeyNotFoundError(key)
	}
	return (value), nil
}

// PutUint16ToBytes is a put with key of type uint16 and value of type []byte.
func (b *Bucket) PutUint16ToBytes(key uint16, value []byte) error {
	return b.Put(uint16Tobytes(key), (value))
}

// FirstUint16ToBytes is first with key of type uint16 and value of type []byte.
func (c *Cursor) FirstUint16ToBytes() (uint16, []byte) {
	rawKey, rawValue := c.First()
	key := bytesTouint16(rawKey)
	value := (rawValue)
	return key, value
}

// LastUint16ToBytes is first with key of type uint16 and value of type []byte.
func (c *Cursor) LastUint16ToBytes() (uint16, []byte) {
	rawKey, rawValue := c.Last()
	key := bytesTouint16(rawKey)
	value := (rawValue)
	return key, value
}

// NextUint16ToBytes is first with key of type uint16 and value of type []byte.
func (c *Cursor) NextUint16ToBytes() (uint16, []byte) {
	rawKey, rawValue := c.Next()
	key := bytesTouint16(rawKey)
	value := (rawValue)
	return key, value
}

// PrevUint16ToBytes is first with key of type uint16 and value of type []byte.
func (c *Cursor) PrevUint16ToBytes() (uint16, []byte) {
	rawKey, rawValue := c.Prev()
	key := bytesTouint16(rawKey)
	value := (rawValue)
	return key, value
}

// SeekUint16ToBytes is first with key of type uint16 and value of type []byte.
func (c *Cursor) SeekUint16ToBytes(key uint16) (uint16, []byte) {
	rawKey, rawValue := c.Seek(uint16Tobytes(key))
	key = bytesTouint16(rawKey)
	value := (rawValue)
	return key, value
}

// ForEachUint16ToString is a for each with key of type uint16 and value of type string.
func (b *Bucket) ForEachUint16ToString(fn func(key uint16, value string) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := bytesTouint16(rawKey)
			value := string(rawValue)
			return fn(key, value)
		},
	)
}

// GetUint16ToString is a get with key of type uint16 and value of type string.
func (b *Bucket) GetUint16ToString(key uint16) string {
	return string(b.Get(uint16Tobytes(key)))
}

// GetRequiredUint16ToString is a get with key of type uint16 and value of type string that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint16ToString(key uint16) (string, error) {
	value := b.Get(uint16Tobytes(key))
	if value == nil {
		return "", NewKeyNotFoundError(key)
	}
	return string(value), nil
}

// PutUint16ToString is a put with key of type uint16 and value of type string.
func (b *Bucket) PutUint16ToString(key uint16, value string) error {
	return b.Put(uint16Tobytes(key), []byte(value))
}

// FirstUint16ToString is first with key of type uint16 and value of type string.
func (c *Cursor) FirstUint16ToString() (uint16, string) {
	rawKey, rawValue := c.First()
	key := bytesTouint16(rawKey)
	value := string(rawValue)
	return key, value
}

// LastUint16ToString is first with key of type uint16 and value of type string.
func (c *Cursor) LastUint16ToString() (uint16, string) {
	rawKey, rawValue := c.Last()
	key := bytesTouint16(rawKey)
	value := string(rawValue)
	return key, value
}

// NextUint16ToString is first with key of type uint16 and value of type string.
func (c *Cursor) NextUint16ToString() (uint16, string) {
	rawKey, rawValue := c.Next()
	key := bytesTouint16(rawKey)
	value := string(rawValue)
	return key, value
}

// PrevUint16ToString is first with key of type uint16 and value of type string.
func (c *Cursor) PrevUint16ToString() (uint16, string) {
	rawKey, rawValue := c.Prev()
	key := bytesTouint16(rawKey)
	value := string(rawValue)
	return key, value
}

// SeekUint16ToString is first with key of type uint16 and value of type string.
func (c *Cursor) SeekUint16ToString(key uint16) (uint16, string) {
	rawKey, rawValue := c.Seek(uint16Tobytes(key))
	key = bytesTouint16(rawKey)
	value := string(rawValue)
	return key, value
}

// ForEachUint16ToUint16 is a for each with key of type uint16 and value of type uint16.
func (b *Bucket) ForEachUint16ToUint16(fn func(key uint16, value uint16) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := bytesTouint16(rawKey)
			value := bytesTouint16(rawValue)
			return fn(key, value)
		},
	)
}

// GetUint16ToUint16 is a get with key of type uint16 and value of type uint16.
func (b *Bucket) GetUint16ToUint16(key uint16) uint16 {
	return bytesTouint16(b.Get(uint16Tobytes(key)))
}

// GetRequiredUint16ToUint16 is a get with key of type uint16 and value of type uint16 that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint16ToUint16(key uint16) (uint16, error) {
	value := b.Get(uint16Tobytes(key))
	if value == nil {
		return 0, NewKeyNotFoundError(key)
	}
	return bytesTouint16(value), nil
}

// PutUint16ToUint16 is a put with key of type uint16 and value of type uint16.
func (b *Bucket) PutUint16ToUint16(key uint16, value uint16) error {
	return b.Put(uint16Tobytes(key), uint16Tobytes(value))
}

// FirstUint16ToUint16 is first with key of type uint16 and value of type uint16.
func (c *Cursor) FirstUint16ToUint16() (uint16, uint16) {
	rawKey, rawValue := c.First()
	key := bytesTouint16(rawKey)
	value := bytesTouint16(rawValue)
	return key, value
}

// LastUint16ToUint16 is first with key of type uint16 and value of type uint16.
func (c *Cursor) LastUint16ToUint16() (uint16, uint16) {
	rawKey, rawValue := c.Last()
	key := bytesTouint16(rawKey)
	value := bytesTouint16(rawValue)
	return key, value
}

// NextUint16ToUint16 is first with key of type uint16 and value of type uint16.
func (c *Cursor) NextUint16ToUint16() (uint16, uint16) {
	rawKey, rawValue := c.Next()
	key := bytesTouint16(rawKey)
	value := bytesTouint16(rawValue)
	return key, value
}

// PrevUint16ToUint16 is first with key of type uint16 and value of type uint16.
func (c *Cursor) PrevUint16ToUint16() (uint16, uint16) {
	rawKey, rawValue := c.Prev()
	key := bytesTouint16(rawKey)
	value := bytesTouint16(rawValue)
	return key, value
}

// SeekUint16ToUint16 is first with key of type uint16 and value of type uint16.
func (c *Cursor) SeekUint16ToUint16(key uint16) (uint16, uint16) {
	rawKey, rawValue := c.Seek(uint16Tobytes(key))
	key = bytesTouint16(rawKey)
	value := bytesTouint16(rawValue)
	return key, value
}

// ForEachUint16ToUint32 is a for each with key of type uint16 and value of type uint32.
func (b *Bucket) ForEachUint16ToUint32(fn func(key uint16, value uint32) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := bytesTouint16(rawKey)
			value := bytesTouint32(rawValue)
			return fn(key, value)
		},
	)
}

// GetUint16ToUint32 is a get with key of type uint16 and value of type uint32.
func (b *Bucket) GetUint16ToUint32(key uint16) uint32 {
	return bytesTouint32(b.Get(uint16Tobytes(key)))
}

// GetRequiredUint16ToUint32 is a get with key of type uint16 and value of type uint32 that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint16ToUint32(key uint16) (uint32, error) {
	value := b.Get(uint16Tobytes(key))
	if value == nil {
		return 0, NewKeyNotFoundError(key)
	}
	return bytesTouint32(value), nil
}

// PutUint16ToUint32 is a put with key of type uint16 and value of type uint32.
func (b *Bucket) PutUint16ToUint32(key uint16, value uint32) error {
	return b.Put(uint16Tobytes(key), uint32Tobytes(value))
}

// FirstUint16ToUint32 is first with key of type uint16 and value of type uint32.
func (c *Cursor) FirstUint16ToUint32() (uint16, uint32) {
	rawKey, rawValue := c.First()
	key := bytesTouint16(rawKey)
	value := bytesTouint32(rawValue)
	return key, value
}

// LastUint16ToUint32 is first with key of type uint16 and value of type uint32.
func (c *Cursor) LastUint16ToUint32() (uint16, uint32) {
	rawKey, rawValue := c.Last()
	key := bytesTouint16(rawKey)
	value := bytesTouint32(rawValue)
	return key, value
}

// NextUint16ToUint32 is first with key of type uint16 and value of type uint32.
func (c *Cursor) NextUint16ToUint32() (uint16, uint32) {
	rawKey, rawValue := c.Next()
	key := bytesTouint16(rawKey)
	value := bytesTouint32(rawValue)
	return key, value
}

// PrevUint16ToUint32 is first with key of type uint16 and value of type uint32.
func (c *Cursor) PrevUint16ToUint32() (uint16, uint32) {
	rawKey, rawValue := c.Prev()
	key := bytesTouint16(rawKey)
	value := bytesTouint32(rawValue)
	return key, value
}

// SeekUint16ToUint32 is first with key of type uint16 and value of type uint32.
func (c *Cursor) SeekUint16ToUint32(key uint16) (uint16, uint32) {
	rawKey, rawValue := c.Seek(uint16Tobytes(key))
	key = bytesTouint16(rawKey)
	value := bytesTouint32(rawValue)
	return key, value
}

// ForEachUint16ToUint64 is a for each with key of type uint16 and value of type uint64.
func (b *Bucket) ForEachUint16ToUint64(fn func(key uint16, value uint64) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := bytesTouint16(rawKey)
			value := bytesTouint64(rawValue)
			return fn(key, value)
		},
	)
}

// GetUint16ToUint64 is a get with key of type uint16 and value of type uint64.
func (b *Bucket) GetUint16ToUint64(key uint16) uint64 {
	return bytesTouint64(b.Get(uint16Tobytes(key)))
}

// GetRequiredUint16ToUint64 is a get with key of type uint16 and value of type uint64 that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint16ToUint64(key uint16) (uint64, error) {
	value := b.Get(uint16Tobytes(key))
	if value == nil {
		return 0, NewKeyNotFoundError(key)
	}
	return bytesTouint64(value), nil
}

// PutUint16ToUint64 is a put with key of type uint16 and value of type uint64.
func (b *Bucket) PutUint16ToUint64(key uint16, value uint64) error {
	return b.Put(uint16Tobytes(key), uint64Tobytes(value))
}

// FirstUint16ToUint64 is first with key of type uint16 and value of type uint64.
func (c *Cursor) FirstUint16ToUint64() (uint16, uint64) {
	rawKey, rawValue := c.First()
	key := bytesTouint16(rawKey)
	value := bytesTouint64(rawValue)
	return key, value
}

// LastUint16ToUint64 is first with key of type uint16 and value of type uint64.
func (c *Cursor) LastUint16ToUint64() (uint16, uint64) {
	rawKey, rawValue := c.Last()
	key := bytesTouint16(rawKey)
	value := bytesTouint64(rawValue)
	return key, value
}

// NextUint16ToUint64 is first with key of type uint16 and value of type uint64.
func (c *Cursor) NextUint16ToUint64() (uint16, uint64) {
	rawKey, rawValue := c.Next()
	key := bytesTouint16(rawKey)
	value := bytesTouint64(rawValue)
	return key, value
}

// PrevUint16ToUint64 is first with key of type uint16 and value of type uint64.
func (c *Cursor) PrevUint16ToUint64() (uint16, uint64) {
	rawKey, rawValue := c.Prev()
	key := bytesTouint16(rawKey)
	value := bytesTouint64(rawValue)
	return key, value
}

// SeekUint16ToUint64 is first with key of type uint16 and value of type uint64.
func (c *Cursor) SeekUint16ToUint64(key uint16) (uint16, uint64) {
	rawKey, rawValue := c.Seek(uint16Tobytes(key))
	key = bytesTouint16(rawKey)
	value := bytesTouint64(rawValue)
	return key, value
}

// ForEachUint16ToUint16Uint16 is a for each with key of type uint16 and value of type pair(uint16,uint16).
func (b *Bucket) ForEachUint16ToUint16Uint16(fn func(key uint16, value1 uint16, value2 uint16) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := bytesTouint16(rawKey)
			value1, value2 := bytesTouint16uint16(rawValue)
			return fn(key, value1, value2)
		},
	)
}

// GetUint16ToUint16Uint16 is a get with key of type uint16 and value of type pair(uint16,uint16).
func (b *Bucket) GetUint16ToUint16Uint16(key uint16) (uint16, uint16) {
	return bytesTouint16uint16(b.Get(uint16Tobytes(key)))
}

// GetRequiredUint16ToUint16Uint16 is a get with key of type uint16 and value of type pair(uint16,uint16) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint16ToUint16Uint16(key uint16) (uint16, uint16, error) {
	value := b.Get(uint16Tobytes(key))
	if value == nil {
		return 0, 0, NewKeyNotFoundError(key)
	}
	value1, value2 := bytesTouint16uint16(value)
	return value1, value2, nil
}

// PutUint16ToUint16Uint16 is a put with key of type uint16 and value of type pair(uint16,uint16).
func (b *Bucket) PutUint16ToUint16Uint16(key uint16, value1 uint16, value2 uint16) error {
	return b.Put(uint16Tobytes(key), uint16uint16Tobytes(value1, value2))
}

// FirstUint16ToUint16Uint16 is first with key of type uint16 and value of type pair(uint16,uint16).
func (c *Cursor) FirstUint16ToUint16Uint16() (uint16, uint16, uint16) {
	rawKey, rawValue := c.First()
	key := bytesTouint16(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key, value1, value2
}

// LastUint16ToUint16Uint16 is first with key of type uint16 and value of type pair(uint16,uint16).
func (c *Cursor) LastUint16ToUint16Uint16() (uint16, uint16, uint16) {
	rawKey, rawValue := c.Last()
	key := bytesTouint16(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key, value1, value2
}

// NextUint16ToUint16Uint16 is first with key of type uint16 and value of type pair(uint16,uint16).
func (c *Cursor) NextUint16ToUint16Uint16() (uint16, uint16, uint16) {
	rawKey, rawValue := c.Next()
	key := bytesTouint16(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key, value1, value2
}

// PrevUint16ToUint16Uint16 is first with key of type uint16 and value of type pair(uint16,uint16).
func (c *Cursor) PrevUint16ToUint16Uint16() (uint16, uint16, uint16) {
	rawKey, rawValue := c.Prev()
	key := bytesTouint16(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key, value1, value2
}

// SeekUint16ToUint16Uint16 is first with key of type uint16 and value of type pair(uint16,uint16).
func (c *Cursor) SeekUint16ToUint16Uint16(key uint16) (uint16, uint16, uint16) {
	rawKey, rawValue := c.Seek(uint16Tobytes(key))
	key = bytesTouint16(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key, value1, value2
}

// ForEachUint16ToUint32Uint32 is a for each with key of type uint16 and value of type pair(uint32,uint32).
func (b *Bucket) ForEachUint16ToUint32Uint32(fn func(key uint16, value1 uint32, value2 uint32) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := bytesTouint16(rawKey)
			value1, value2 := bytesTouint32uint32(rawValue)
			return fn(key, value1, value2)
		},
	)
}

// GetUint16ToUint32Uint32 is a get with key of type uint16 and value of type pair(uint32,uint32).
func (b *Bucket) GetUint16ToUint32Uint32(key uint16) (uint32, uint32) {
	return bytesTouint32uint32(b.Get(uint16Tobytes(key)))
}

// GetRequiredUint16ToUint32Uint32 is a get with key of type uint16 and value of type pair(uint32,uint32) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint16ToUint32Uint32(key uint16) (uint32, uint32, error) {
	value := b.Get(uint16Tobytes(key))
	if value == nil {
		return 0, 0, NewKeyNotFoundError(key)
	}
	value1, value2 := bytesTouint32uint32(value)
	return value1, value2, nil
}

// PutUint16ToUint32Uint32 is a put with key of type uint16 and value of type pair(uint32,uint32).
func (b *Bucket) PutUint16ToUint32Uint32(key uint16, value1 uint32, value2 uint32) error {
	return b.Put(uint16Tobytes(key), uint32uint32Tobytes(value1, value2))
}

// FirstUint16ToUint32Uint32 is first with key of type uint16 and value of type pair(uint32,uint32).
func (c *Cursor) FirstUint16ToUint32Uint32() (uint16, uint32, uint32) {
	rawKey, rawValue := c.First()
	key := bytesTouint16(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key, value1, value2
}

// LastUint16ToUint32Uint32 is first with key of type uint16 and value of type pair(uint32,uint32).
func (c *Cursor) LastUint16ToUint32Uint32() (uint16, uint32, uint32) {
	rawKey, rawValue := c.Last()
	key := bytesTouint16(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key, value1, value2
}

// NextUint16ToUint32Uint32 is first with key of type uint16 and value of type pair(uint32,uint32).
func (c *Cursor) NextUint16ToUint32Uint32() (uint16, uint32, uint32) {
	rawKey, rawValue := c.Next()
	key := bytesTouint16(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key, value1, value2
}

// PrevUint16ToUint32Uint32 is first with key of type uint16 and value of type pair(uint32,uint32).
func (c *Cursor) PrevUint16ToUint32Uint32() (uint16, uint32, uint32) {
	rawKey, rawValue := c.Prev()
	key := bytesTouint16(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key, value1, value2
}

// SeekUint16ToUint32Uint32 is first with key of type uint16 and value of type pair(uint32,uint32).
func (c *Cursor) SeekUint16ToUint32Uint32(key uint16) (uint16, uint32, uint32) {
	rawKey, rawValue := c.Seek(uint16Tobytes(key))
	key = bytesTouint16(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key, value1, value2
}

// ForEachUint16ToUint64Uint64 is a for each with key of type uint16 and value of type pair(uint64,uint64).
func (b *Bucket) ForEachUint16ToUint64Uint64(fn func(key uint16, value1 uint64, value2 uint64) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := bytesTouint16(rawKey)
			value1, value2 := bytesTouint64uint64(rawValue)
			return fn(key, value1, value2)
		},
	)
}

// GetUint16ToUint64Uint64 is a get with key of type uint16 and value of type pair(uint64,uint64).
func (b *Bucket) GetUint16ToUint64Uint64(key uint16) (uint64, uint64) {
	return bytesTouint64uint64(b.Get(uint16Tobytes(key)))
}

// GetRequiredUint16ToUint64Uint64 is a get with key of type uint16 and value of type pair(uint64,uint64) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint16ToUint64Uint64(key uint16) (uint64, uint64, error) {
	value := b.Get(uint16Tobytes(key))
	if value == nil {
		return 0, 0, NewKeyNotFoundError(key)
	}
	value1, value2 := bytesTouint64uint64(value)
	return value1, value2, nil
}

// PutUint16ToUint64Uint64 is a put with key of type uint16 and value of type pair(uint64,uint64).
func (b *Bucket) PutUint16ToUint64Uint64(key uint16, value1 uint64, value2 uint64) error {
	return b.Put(uint16Tobytes(key), uint64uint64Tobytes(value1, value2))
}

// FirstUint16ToUint64Uint64 is first with key of type uint16 and value of type pair(uint64,uint64).
func (c *Cursor) FirstUint16ToUint64Uint64() (uint16, uint64, uint64) {
	rawKey, rawValue := c.First()
	key := bytesTouint16(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key, value1, value2
}

// LastUint16ToUint64Uint64 is first with key of type uint16 and value of type pair(uint64,uint64).
func (c *Cursor) LastUint16ToUint64Uint64() (uint16, uint64, uint64) {
	rawKey, rawValue := c.Last()
	key := bytesTouint16(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key, value1, value2
}

// NextUint16ToUint64Uint64 is first with key of type uint16 and value of type pair(uint64,uint64).
func (c *Cursor) NextUint16ToUint64Uint64() (uint16, uint64, uint64) {
	rawKey, rawValue := c.Next()
	key := bytesTouint16(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key, value1, value2
}

// PrevUint16ToUint64Uint64 is first with key of type uint16 and value of type pair(uint64,uint64).
func (c *Cursor) PrevUint16ToUint64Uint64() (uint16, uint64, uint64) {
	rawKey, rawValue := c.Prev()
	key := bytesTouint16(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key, value1, value2
}

// SeekUint16ToUint64Uint64 is first with key of type uint16 and value of type pair(uint64,uint64).
func (c *Cursor) SeekUint16ToUint64Uint64(key uint16) (uint16, uint64, uint64) {
	rawKey, rawValue := c.Seek(uint16Tobytes(key))
	key = bytesTouint16(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key, value1, value2
}

// ForEachUint16ToUint16Bytes is a for each with key of type uint16 and value of type pair(uint16,[]byte).
func (b *Bucket) ForEachUint16ToUint16Bytes(fn func(key uint16, value1 uint16, value2 []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := bytesTouint16(rawKey)
			value1, value2 := bytesTouint16bytes(rawValue)
			return fn(key, value1, value2)
		},
	)
}

// GetUint16ToUint16Bytes is a get with key of type uint16 and value of type pair(uint16,[]byte).
func (b *Bucket) GetUint16ToUint16Bytes(key uint16) (uint16, []byte) {
	return bytesTouint16bytes(b.Get(uint16Tobytes(key)))
}

// GetRequiredUint16ToUint16Bytes is a get with key of type uint16 and value of type pair(uint16,[]byte) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint16ToUint16Bytes(key uint16) (uint16, []byte, error) {
	value := b.Get(uint16Tobytes(key))
	if value == nil {
		return 0, nil, NewKeyNotFoundError(key)
	}
	value1, value2 := bytesTouint16bytes(value)
	return value1, value2, nil
}

// PutUint16ToUint16Bytes is a put with key of type uint16 and value of type pair(uint16,[]byte).
func (b *Bucket) PutUint16ToUint16Bytes(key uint16, value1 uint16, value2 []byte) error {
	return b.Put(uint16Tobytes(key), uint16bytesTobytes(value1, value2))
}

// FirstUint16ToUint16Bytes is first with key of type uint16 and value of type pair(uint16,[]byte).
func (c *Cursor) FirstUint16ToUint16Bytes() (uint16, uint16, []byte) {
	rawKey, rawValue := c.First()
	key := bytesTouint16(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key, value1, value2
}

// LastUint16ToUint16Bytes is first with key of type uint16 and value of type pair(uint16,[]byte).
func (c *Cursor) LastUint16ToUint16Bytes() (uint16, uint16, []byte) {
	rawKey, rawValue := c.Last()
	key := bytesTouint16(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key, value1, value2
}

// NextUint16ToUint16Bytes is first with key of type uint16 and value of type pair(uint16,[]byte).
func (c *Cursor) NextUint16ToUint16Bytes() (uint16, uint16, []byte) {
	rawKey, rawValue := c.Next()
	key := bytesTouint16(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key, value1, value2
}

// PrevUint16ToUint16Bytes is first with key of type uint16 and value of type pair(uint16,[]byte).
func (c *Cursor) PrevUint16ToUint16Bytes() (uint16, uint16, []byte) {
	rawKey, rawValue := c.Prev()
	key := bytesTouint16(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key, value1, value2
}

// SeekUint16ToUint16Bytes is first with key of type uint16 and value of type pair(uint16,[]byte).
func (c *Cursor) SeekUint16ToUint16Bytes(key uint16) (uint16, uint16, []byte) {
	rawKey, rawValue := c.Seek(uint16Tobytes(key))
	key = bytesTouint16(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key, value1, value2
}

// ForEachUint16ToUint32Bytes is a for each with key of type uint16 and value of type pair(uint32,[]byte).
func (b *Bucket) ForEachUint16ToUint32Bytes(fn func(key uint16, value1 uint32, value2 []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := bytesTouint16(rawKey)
			value1, value2 := bytesTouint32bytes(rawValue)
			return fn(key, value1, value2)
		},
	)
}

// GetUint16ToUint32Bytes is a get with key of type uint16 and value of type pair(uint32,[]byte).
func (b *Bucket) GetUint16ToUint32Bytes(key uint16) (uint32, []byte) {
	return bytesTouint32bytes(b.Get(uint16Tobytes(key)))
}

// GetRequiredUint16ToUint32Bytes is a get with key of type uint16 and value of type pair(uint32,[]byte) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint16ToUint32Bytes(key uint16) (uint32, []byte, error) {
	value := b.Get(uint16Tobytes(key))
	if value == nil {
		return 0, nil, NewKeyNotFoundError(key)
	}
	value1, value2 := bytesTouint32bytes(value)
	return value1, value2, nil
}

// PutUint16ToUint32Bytes is a put with key of type uint16 and value of type pair(uint32,[]byte).
func (b *Bucket) PutUint16ToUint32Bytes(key uint16, value1 uint32, value2 []byte) error {
	return b.Put(uint16Tobytes(key), uint32bytesTobytes(value1, value2))
}

// FirstUint16ToUint32Bytes is first with key of type uint16 and value of type pair(uint32,[]byte).
func (c *Cursor) FirstUint16ToUint32Bytes() (uint16, uint32, []byte) {
	rawKey, rawValue := c.First()
	key := bytesTouint16(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key, value1, value2
}

// LastUint16ToUint32Bytes is first with key of type uint16 and value of type pair(uint32,[]byte).
func (c *Cursor) LastUint16ToUint32Bytes() (uint16, uint32, []byte) {
	rawKey, rawValue := c.Last()
	key := bytesTouint16(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key, value1, value2
}

// NextUint16ToUint32Bytes is first with key of type uint16 and value of type pair(uint32,[]byte).
func (c *Cursor) NextUint16ToUint32Bytes() (uint16, uint32, []byte) {
	rawKey, rawValue := c.Next()
	key := bytesTouint16(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key, value1, value2
}

// PrevUint16ToUint32Bytes is first with key of type uint16 and value of type pair(uint32,[]byte).
func (c *Cursor) PrevUint16ToUint32Bytes() (uint16, uint32, []byte) {
	rawKey, rawValue := c.Prev()
	key := bytesTouint16(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key, value1, value2
}

// SeekUint16ToUint32Bytes is first with key of type uint16 and value of type pair(uint32,[]byte).
func (c *Cursor) SeekUint16ToUint32Bytes(key uint16) (uint16, uint32, []byte) {
	rawKey, rawValue := c.Seek(uint16Tobytes(key))
	key = bytesTouint16(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key, value1, value2
}

// ForEachUint16ToUint64Bytes is a for each with key of type uint16 and value of type pair(uint64,[]byte).
func (b *Bucket) ForEachUint16ToUint64Bytes(fn func(key uint16, value1 uint64, value2 []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := bytesTouint16(rawKey)
			value1, value2 := bytesTouint64bytes(rawValue)
			return fn(key, value1, value2)
		},
	)
}

// GetUint16ToUint64Bytes is a get with key of type uint16 and value of type pair(uint64,[]byte).
func (b *Bucket) GetUint16ToUint64Bytes(key uint16) (uint64, []byte) {
	return bytesTouint64bytes(b.Get(uint16Tobytes(key)))
}

// GetRequiredUint16ToUint64Bytes is a get with key of type uint16 and value of type pair(uint64,[]byte) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint16ToUint64Bytes(key uint16) (uint64, []byte, error) {
	value := b.Get(uint16Tobytes(key))
	if value == nil {
		return 0, nil, NewKeyNotFoundError(key)
	}
	value1, value2 := bytesTouint64bytes(value)
	return value1, value2, nil
}

// PutUint16ToUint64Bytes is a put with key of type uint16 and value of type pair(uint64,[]byte).
func (b *Bucket) PutUint16ToUint64Bytes(key uint16, value1 uint64, value2 []byte) error {
	return b.Put(uint16Tobytes(key), uint64bytesTobytes(value1, value2))
}

// FirstUint16ToUint64Bytes is first with key of type uint16 and value of type pair(uint64,[]byte).
func (c *Cursor) FirstUint16ToUint64Bytes() (uint16, uint64, []byte) {
	rawKey, rawValue := c.First()
	key := bytesTouint16(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key, value1, value2
}

// LastUint16ToUint64Bytes is first with key of type uint16 and value of type pair(uint64,[]byte).
func (c *Cursor) LastUint16ToUint64Bytes() (uint16, uint64, []byte) {
	rawKey, rawValue := c.Last()
	key := bytesTouint16(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key, value1, value2
}

// NextUint16ToUint64Bytes is first with key of type uint16 and value of type pair(uint64,[]byte).
func (c *Cursor) NextUint16ToUint64Bytes() (uint16, uint64, []byte) {
	rawKey, rawValue := c.Next()
	key := bytesTouint16(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key, value1, value2
}

// PrevUint16ToUint64Bytes is first with key of type uint16 and value of type pair(uint64,[]byte).
func (c *Cursor) PrevUint16ToUint64Bytes() (uint16, uint64, []byte) {
	rawKey, rawValue := c.Prev()
	key := bytesTouint16(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key, value1, value2
}

// SeekUint16ToUint64Bytes is first with key of type uint16 and value of type pair(uint64,[]byte).
func (c *Cursor) SeekUint16ToUint64Bytes(key uint16) (uint16, uint64, []byte) {
	rawKey, rawValue := c.Seek(uint16Tobytes(key))
	key = bytesTouint16(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key, value1, value2
}

// ForEachUint16ToBytesUint16 is a for each with key of type uint16 and value of type pair([]byte,uint16).
func (b *Bucket) ForEachUint16ToBytesUint16(fn func(key uint16, value1 []byte, value2 uint16) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := bytesTouint16(rawKey)
			value1, value2 := bytesTobytesuint16(rawValue)
			return fn(key, value1, value2)
		},
	)
}

// GetUint16ToBytesUint16 is a get with key of type uint16 and value of type pair([]byte,uint16).
func (b *Bucket) GetUint16ToBytesUint16(key uint16) ([]byte, uint16) {
	return bytesTobytesuint16(b.Get(uint16Tobytes(key)))
}

// GetRequiredUint16ToBytesUint16 is a get with key of type uint16 and value of type pair([]byte,uint16) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint16ToBytesUint16(key uint16) ([]byte, uint16, error) {
	value := b.Get(uint16Tobytes(key))
	if value == nil {
		return nil, 0, NewKeyNotFoundError(key)
	}
	value1, value2 := bytesTobytesuint16(value)
	return value1, value2, nil
}

// PutUint16ToBytesUint16 is a put with key of type uint16 and value of type pair([]byte,uint16).
func (b *Bucket) PutUint16ToBytesUint16(key uint16, value1 []byte, value2 uint16) error {
	return b.Put(uint16Tobytes(key), bytesuint16Tobytes(value1, value2))
}

// FirstUint16ToBytesUint16 is first with key of type uint16 and value of type pair([]byte,uint16).
func (c *Cursor) FirstUint16ToBytesUint16() (uint16, []byte, uint16) {
	rawKey, rawValue := c.First()
	key := bytesTouint16(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key, value1, value2
}

// LastUint16ToBytesUint16 is first with key of type uint16 and value of type pair([]byte,uint16).
func (c *Cursor) LastUint16ToBytesUint16() (uint16, []byte, uint16) {
	rawKey, rawValue := c.Last()
	key := bytesTouint16(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key, value1, value2
}

// NextUint16ToBytesUint16 is first with key of type uint16 and value of type pair([]byte,uint16).
func (c *Cursor) NextUint16ToBytesUint16() (uint16, []byte, uint16) {
	rawKey, rawValue := c.Next()
	key := bytesTouint16(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key, value1, value2
}

// PrevUint16ToBytesUint16 is first with key of type uint16 and value of type pair([]byte,uint16).
func (c *Cursor) PrevUint16ToBytesUint16() (uint16, []byte, uint16) {
	rawKey, rawValue := c.Prev()
	key := bytesTouint16(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key, value1, value2
}

// SeekUint16ToBytesUint16 is first with key of type uint16 and value of type pair([]byte,uint16).
func (c *Cursor) SeekUint16ToBytesUint16(key uint16) (uint16, []byte, uint16) {
	rawKey, rawValue := c.Seek(uint16Tobytes(key))
	key = bytesTouint16(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key, value1, value2
}

// ForEachUint16ToBytesUint32 is a for each with key of type uint16 and value of type pair([]byte,uint32).
func (b *Bucket) ForEachUint16ToBytesUint32(fn func(key uint16, value1 []byte, value2 uint32) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := bytesTouint16(rawKey)
			value1, value2 := bytesTobytesuint32(rawValue)
			return fn(key, value1, value2)
		},
	)
}

// GetUint16ToBytesUint32 is a get with key of type uint16 and value of type pair([]byte,uint32).
func (b *Bucket) GetUint16ToBytesUint32(key uint16) ([]byte, uint32) {
	return bytesTobytesuint32(b.Get(uint16Tobytes(key)))
}

// GetRequiredUint16ToBytesUint32 is a get with key of type uint16 and value of type pair([]byte,uint32) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint16ToBytesUint32(key uint16) ([]byte, uint32, error) {
	value := b.Get(uint16Tobytes(key))
	if value == nil {
		return nil, 0, NewKeyNotFoundError(key)
	}
	value1, value2 := bytesTobytesuint32(value)
	return value1, value2, nil
}

// PutUint16ToBytesUint32 is a put with key of type uint16 and value of type pair([]byte,uint32).
func (b *Bucket) PutUint16ToBytesUint32(key uint16, value1 []byte, value2 uint32) error {
	return b.Put(uint16Tobytes(key), bytesuint32Tobytes(value1, value2))
}

// FirstUint16ToBytesUint32 is first with key of type uint16 and value of type pair([]byte,uint32).
func (c *Cursor) FirstUint16ToBytesUint32() (uint16, []byte, uint32) {
	rawKey, rawValue := c.First()
	key := bytesTouint16(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key, value1, value2
}

// LastUint16ToBytesUint32 is first with key of type uint16 and value of type pair([]byte,uint32).
func (c *Cursor) LastUint16ToBytesUint32() (uint16, []byte, uint32) {
	rawKey, rawValue := c.Last()
	key := bytesTouint16(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key, value1, value2
}

// NextUint16ToBytesUint32 is first with key of type uint16 and value of type pair([]byte,uint32).
func (c *Cursor) NextUint16ToBytesUint32() (uint16, []byte, uint32) {
	rawKey, rawValue := c.Next()
	key := bytesTouint16(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key, value1, value2
}

// PrevUint16ToBytesUint32 is first with key of type uint16 and value of type pair([]byte,uint32).
func (c *Cursor) PrevUint16ToBytesUint32() (uint16, []byte, uint32) {
	rawKey, rawValue := c.Prev()
	key := bytesTouint16(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key, value1, value2
}

// SeekUint16ToBytesUint32 is first with key of type uint16 and value of type pair([]byte,uint32).
func (c *Cursor) SeekUint16ToBytesUint32(key uint16) (uint16, []byte, uint32) {
	rawKey, rawValue := c.Seek(uint16Tobytes(key))
	key = bytesTouint16(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key, value1, value2
}

// ForEachUint16ToBytesUint64 is a for each with key of type uint16 and value of type pair([]byte,uint64).
func (b *Bucket) ForEachUint16ToBytesUint64(fn func(key uint16, value1 []byte, value2 uint64) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := bytesTouint16(rawKey)
			value1, value2 := bytesTobytesuint64(rawValue)
			return fn(key, value1, value2)
		},
	)
}

// GetUint16ToBytesUint64 is a get with key of type uint16 and value of type pair([]byte,uint64).
func (b *Bucket) GetUint16ToBytesUint64(key uint16) ([]byte, uint64) {
	return bytesTobytesuint64(b.Get(uint16Tobytes(key)))
}

// GetRequiredUint16ToBytesUint64 is a get with key of type uint16 and value of type pair([]byte,uint64) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint16ToBytesUint64(key uint16) ([]byte, uint64, error) {
	value := b.Get(uint16Tobytes(key))
	if value == nil {
		return nil, 0, NewKeyNotFoundError(key)
	}
	value1, value2 := bytesTobytesuint64(value)
	return value1, value2, nil
}

// PutUint16ToBytesUint64 is a put with key of type uint16 and value of type pair([]byte,uint64).
func (b *Bucket) PutUint16ToBytesUint64(key uint16, value1 []byte, value2 uint64) error {
	return b.Put(uint16Tobytes(key), bytesuint64Tobytes(value1, value2))
}

// FirstUint16ToBytesUint64 is first with key of type uint16 and value of type pair([]byte,uint64).
func (c *Cursor) FirstUint16ToBytesUint64() (uint16, []byte, uint64) {
	rawKey, rawValue := c.First()
	key := bytesTouint16(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key, value1, value2
}

// LastUint16ToBytesUint64 is first with key of type uint16 and value of type pair([]byte,uint64).
func (c *Cursor) LastUint16ToBytesUint64() (uint16, []byte, uint64) {
	rawKey, rawValue := c.Last()
	key := bytesTouint16(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key, value1, value2
}

// NextUint16ToBytesUint64 is first with key of type uint16 and value of type pair([]byte,uint64).
func (c *Cursor) NextUint16ToBytesUint64() (uint16, []byte, uint64) {
	rawKey, rawValue := c.Next()
	key := bytesTouint16(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key, value1, value2
}

// PrevUint16ToBytesUint64 is first with key of type uint16 and value of type pair([]byte,uint64).
func (c *Cursor) PrevUint16ToBytesUint64() (uint16, []byte, uint64) {
	rawKey, rawValue := c.Prev()
	key := bytesTouint16(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key, value1, value2
}

// SeekUint16ToBytesUint64 is first with key of type uint16 and value of type pair([]byte,uint64).
func (c *Cursor) SeekUint16ToBytesUint64(key uint16) (uint16, []byte, uint64) {
	rawKey, rawValue := c.Seek(uint16Tobytes(key))
	key = bytesTouint16(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key, value1, value2
}

// DeleteUint16 is a delete with key of type uint16.
func (b *Bucket) DeleteUint16(key uint16) error {
	return b.Delete(uint16Tobytes(key))
}

// ForEachUint32ToBytes is a for each with key of type uint32 and value of type []byte.
func (b *Bucket) ForEachUint32ToBytes(fn func(key uint32, value []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := bytesTouint32(rawKey)
			value := (rawValue)
			return fn(key, value)
		},
	)
}

// GetUint32ToBytes is a get with key of type uint32 and value of type []byte.
func (b *Bucket) GetUint32ToBytes(key uint32) []byte {
	return (b.Get(uint32Tobytes(key)))
}

// GetRequiredUint32ToBytes is a get with key of type uint32 and value of type []byte that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint32ToBytes(key uint32) ([]byte, error) {
	value := b.Get(uint32Tobytes(key))
	if value == nil {
		return nil, NewKeyNotFoundError(key)
	}
	return (value), nil
}

// PutUint32ToBytes is a put with key of type uint32 and value of type []byte.
func (b *Bucket) PutUint32ToBytes(key uint32, value []byte) error {
	return b.Put(uint32Tobytes(key), (value))
}

// FirstUint32ToBytes is first with key of type uint32 and value of type []byte.
func (c *Cursor) FirstUint32ToBytes() (uint32, []byte) {
	rawKey, rawValue := c.First()
	key := bytesTouint32(rawKey)
	value := (rawValue)
	return key, value
}

// LastUint32ToBytes is first with key of type uint32 and value of type []byte.
func (c *Cursor) LastUint32ToBytes() (uint32, []byte) {
	rawKey, rawValue := c.Last()
	key := bytesTouint32(rawKey)
	value := (rawValue)
	return key, value
}

// NextUint32ToBytes is first with key of type uint32 and value of type []byte.
func (c *Cursor) NextUint32ToBytes() (uint32, []byte) {
	rawKey, rawValue := c.Next()
	key := bytesTouint32(rawKey)
	value := (rawValue)
	return key, value
}

// PrevUint32ToBytes is first with key of type uint32 and value of type []byte.
func (c *Cursor) PrevUint32ToBytes() (uint32, []byte) {
	rawKey, rawValue := c.Prev()
	key := bytesTouint32(rawKey)
	value := (rawValue)
	return key, value
}

// SeekUint32ToBytes is first with key of type uint32 and value of type []byte.
func (c *Cursor) SeekUint32ToBytes(key uint32) (uint32, []byte) {
	rawKey, rawValue := c.Seek(uint32Tobytes(key))
	key = bytesTouint32(rawKey)
	value := (rawValue)
	return key, value
}

// ForEachUint32ToString is a for each with key of type uint32 and value of type string.
func (b *Bucket) ForEachUint32ToString(fn func(key uint32, value string) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := bytesTouint32(rawKey)
			value := string(rawValue)
			return fn(key, value)
		},
	)
}

// GetUint32ToString is a get with key of type uint32 and value of type string.
func (b *Bucket) GetUint32ToString(key uint32) string {
	return string(b.Get(uint32Tobytes(key)))
}

// GetRequiredUint32ToString is a get with key of type uint32 and value of type string that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint32ToString(key uint32) (string, error) {
	value := b.Get(uint32Tobytes(key))
	if value == nil {
		return "", NewKeyNotFoundError(key)
	}
	return string(value), nil
}

// PutUint32ToString is a put with key of type uint32 and value of type string.
func (b *Bucket) PutUint32ToString(key uint32, value string) error {
	return b.Put(uint32Tobytes(key), []byte(value))
}

// FirstUint32ToString is first with key of type uint32 and value of type string.
func (c *Cursor) FirstUint32ToString() (uint32, string) {
	rawKey, rawValue := c.First()
	key := bytesTouint32(rawKey)
	value := string(rawValue)
	return key, value
}

// LastUint32ToString is first with key of type uint32 and value of type string.
func (c *Cursor) LastUint32ToString() (uint32, string) {
	rawKey, rawValue := c.Last()
	key := bytesTouint32(rawKey)
	value := string(rawValue)
	return key, value
}

// NextUint32ToString is first with key of type uint32 and value of type string.
func (c *Cursor) NextUint32ToString() (uint32, string) {
	rawKey, rawValue := c.Next()
	key := bytesTouint32(rawKey)
	value := string(rawValue)
	return key, value
}

// PrevUint32ToString is first with key of type uint32 and value of type string.
func (c *Cursor) PrevUint32ToString() (uint32, string) {
	rawKey, rawValue := c.Prev()
	key := bytesTouint32(rawKey)
	value := string(rawValue)
	return key, value
}

// SeekUint32ToString is first with key of type uint32 and value of type string.
func (c *Cursor) SeekUint32ToString(key uint32) (uint32, string) {
	rawKey, rawValue := c.Seek(uint32Tobytes(key))
	key = bytesTouint32(rawKey)
	value := string(rawValue)
	return key, value
}

// ForEachUint32ToUint16 is a for each with key of type uint32 and value of type uint16.
func (b *Bucket) ForEachUint32ToUint16(fn func(key uint32, value uint16) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := bytesTouint32(rawKey)
			value := bytesTouint16(rawValue)
			return fn(key, value)
		},
	)
}

// GetUint32ToUint16 is a get with key of type uint32 and value of type uint16.
func (b *Bucket) GetUint32ToUint16(key uint32) uint16 {
	return bytesTouint16(b.Get(uint32Tobytes(key)))
}

// GetRequiredUint32ToUint16 is a get with key of type uint32 and value of type uint16 that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint32ToUint16(key uint32) (uint16, error) {
	value := b.Get(uint32Tobytes(key))
	if value == nil {
		return 0, NewKeyNotFoundError(key)
	}
	return bytesTouint16(value), nil
}

// PutUint32ToUint16 is a put with key of type uint32 and value of type uint16.
func (b *Bucket) PutUint32ToUint16(key uint32, value uint16) error {
	return b.Put(uint32Tobytes(key), uint16Tobytes(value))
}

// FirstUint32ToUint16 is first with key of type uint32 and value of type uint16.
func (c *Cursor) FirstUint32ToUint16() (uint32, uint16) {
	rawKey, rawValue := c.First()
	key := bytesTouint32(rawKey)
	value := bytesTouint16(rawValue)
	return key, value
}

// LastUint32ToUint16 is first with key of type uint32 and value of type uint16.
func (c *Cursor) LastUint32ToUint16() (uint32, uint16) {
	rawKey, rawValue := c.Last()
	key := bytesTouint32(rawKey)
	value := bytesTouint16(rawValue)
	return key, value
}

// NextUint32ToUint16 is first with key of type uint32 and value of type uint16.
func (c *Cursor) NextUint32ToUint16() (uint32, uint16) {
	rawKey, rawValue := c.Next()
	key := bytesTouint32(rawKey)
	value := bytesTouint16(rawValue)
	return key, value
}

// PrevUint32ToUint16 is first with key of type uint32 and value of type uint16.
func (c *Cursor) PrevUint32ToUint16() (uint32, uint16) {
	rawKey, rawValue := c.Prev()
	key := bytesTouint32(rawKey)
	value := bytesTouint16(rawValue)
	return key, value
}

// SeekUint32ToUint16 is first with key of type uint32 and value of type uint16.
func (c *Cursor) SeekUint32ToUint16(key uint32) (uint32, uint16) {
	rawKey, rawValue := c.Seek(uint32Tobytes(key))
	key = bytesTouint32(rawKey)
	value := bytesTouint16(rawValue)
	return key, value
}

// ForEachUint32ToUint32 is a for each with key of type uint32 and value of type uint32.
func (b *Bucket) ForEachUint32ToUint32(fn func(key uint32, value uint32) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := bytesTouint32(rawKey)
			value := bytesTouint32(rawValue)
			return fn(key, value)
		},
	)
}

// GetUint32ToUint32 is a get with key of type uint32 and value of type uint32.
func (b *Bucket) GetUint32ToUint32(key uint32) uint32 {
	return bytesTouint32(b.Get(uint32Tobytes(key)))
}

// GetRequiredUint32ToUint32 is a get with key of type uint32 and value of type uint32 that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint32ToUint32(key uint32) (uint32, error) {
	value := b.Get(uint32Tobytes(key))
	if value == nil {
		return 0, NewKeyNotFoundError(key)
	}
	return bytesTouint32(value), nil
}

// PutUint32ToUint32 is a put with key of type uint32 and value of type uint32.
func (b *Bucket) PutUint32ToUint32(key uint32, value uint32) error {
	return b.Put(uint32Tobytes(key), uint32Tobytes(value))
}

// FirstUint32ToUint32 is first with key of type uint32 and value of type uint32.
func (c *Cursor) FirstUint32ToUint32() (uint32, uint32) {
	rawKey, rawValue := c.First()
	key := bytesTouint32(rawKey)
	value := bytesTouint32(rawValue)
	return key, value
}

// LastUint32ToUint32 is first with key of type uint32 and value of type uint32.
func (c *Cursor) LastUint32ToUint32() (uint32, uint32) {
	rawKey, rawValue := c.Last()
	key := bytesTouint32(rawKey)
	value := bytesTouint32(rawValue)
	return key, value
}

// NextUint32ToUint32 is first with key of type uint32 and value of type uint32.
func (c *Cursor) NextUint32ToUint32() (uint32, uint32) {
	rawKey, rawValue := c.Next()
	key := bytesTouint32(rawKey)
	value := bytesTouint32(rawValue)
	return key, value
}

// PrevUint32ToUint32 is first with key of type uint32 and value of type uint32.
func (c *Cursor) PrevUint32ToUint32() (uint32, uint32) {
	rawKey, rawValue := c.Prev()
	key := bytesTouint32(rawKey)
	value := bytesTouint32(rawValue)
	return key, value
}

// SeekUint32ToUint32 is first with key of type uint32 and value of type uint32.
func (c *Cursor) SeekUint32ToUint32(key uint32) (uint32, uint32) {
	rawKey, rawValue := c.Seek(uint32Tobytes(key))
	key = bytesTouint32(rawKey)
	value := bytesTouint32(rawValue)
	return key, value
}

// ForEachUint32ToUint64 is a for each with key of type uint32 and value of type uint64.
func (b *Bucket) ForEachUint32ToUint64(fn func(key uint32, value uint64) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := bytesTouint32(rawKey)
			value := bytesTouint64(rawValue)
			return fn(key, value)
		},
	)
}

// GetUint32ToUint64 is a get with key of type uint32 and value of type uint64.
func (b *Bucket) GetUint32ToUint64(key uint32) uint64 {
	return bytesTouint64(b.Get(uint32Tobytes(key)))
}

// GetRequiredUint32ToUint64 is a get with key of type uint32 and value of type uint64 that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint32ToUint64(key uint32) (uint64, error) {
	value := b.Get(uint32Tobytes(key))
	if value == nil {
		return 0, NewKeyNotFoundError(key)
	}
	return bytesTouint64(value), nil
}

// PutUint32ToUint64 is a put with key of type uint32 and value of type uint64.
func (b *Bucket) PutUint32ToUint64(key uint32, value uint64) error {
	return b.Put(uint32Tobytes(key), uint64Tobytes(value))
}

// FirstUint32ToUint64 is first with key of type uint32 and value of type uint64.
func (c *Cursor) FirstUint32ToUint64() (uint32, uint64) {
	rawKey, rawValue := c.First()
	key := bytesTouint32(rawKey)
	value := bytesTouint64(rawValue)
	return key, value
}

// LastUint32ToUint64 is first with key of type uint32 and value of type uint64.
func (c *Cursor) LastUint32ToUint64() (uint32, uint64) {
	rawKey, rawValue := c.Last()
	key := bytesTouint32(rawKey)
	value := bytesTouint64(rawValue)
	return key, value
}

// NextUint32ToUint64 is first with key of type uint32 and value of type uint64.
func (c *Cursor) NextUint32ToUint64() (uint32, uint64) {
	rawKey, rawValue := c.Next()
	key := bytesTouint32(rawKey)
	value := bytesTouint64(rawValue)
	return key, value
}

// PrevUint32ToUint64 is first with key of type uint32 and value of type uint64.
func (c *Cursor) PrevUint32ToUint64() (uint32, uint64) {
	rawKey, rawValue := c.Prev()
	key := bytesTouint32(rawKey)
	value := bytesTouint64(rawValue)
	return key, value
}

// SeekUint32ToUint64 is first with key of type uint32 and value of type uint64.
func (c *Cursor) SeekUint32ToUint64(key uint32) (uint32, uint64) {
	rawKey, rawValue := c.Seek(uint32Tobytes(key))
	key = bytesTouint32(rawKey)
	value := bytesTouint64(rawValue)
	return key, value
}

// ForEachUint32ToUint16Uint16 is a for each with key of type uint32 and value of type pair(uint16,uint16).
func (b *Bucket) ForEachUint32ToUint16Uint16(fn func(key uint32, value1 uint16, value2 uint16) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := bytesTouint32(rawKey)
			value1, value2 := bytesTouint16uint16(rawValue)
			return fn(key, value1, value2)
		},
	)
}

// GetUint32ToUint16Uint16 is a get with key of type uint32 and value of type pair(uint16,uint16).
func (b *Bucket) GetUint32ToUint16Uint16(key uint32) (uint16, uint16) {
	return bytesTouint16uint16(b.Get(uint32Tobytes(key)))
}

// GetRequiredUint32ToUint16Uint16 is a get with key of type uint32 and value of type pair(uint16,uint16) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint32ToUint16Uint16(key uint32) (uint16, uint16, error) {
	value := b.Get(uint32Tobytes(key))
	if value == nil {
		return 0, 0, NewKeyNotFoundError(key)
	}
	value1, value2 := bytesTouint16uint16(value)
	return value1, value2, nil
}

// PutUint32ToUint16Uint16 is a put with key of type uint32 and value of type pair(uint16,uint16).
func (b *Bucket) PutUint32ToUint16Uint16(key uint32, value1 uint16, value2 uint16) error {
	return b.Put(uint32Tobytes(key), uint16uint16Tobytes(value1, value2))
}

// FirstUint32ToUint16Uint16 is first with key of type uint32 and value of type pair(uint16,uint16).
func (c *Cursor) FirstUint32ToUint16Uint16() (uint32, uint16, uint16) {
	rawKey, rawValue := c.First()
	key := bytesTouint32(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key, value1, value2
}

// LastUint32ToUint16Uint16 is first with key of type uint32 and value of type pair(uint16,uint16).
func (c *Cursor) LastUint32ToUint16Uint16() (uint32, uint16, uint16) {
	rawKey, rawValue := c.Last()
	key := bytesTouint32(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key, value1, value2
}

// NextUint32ToUint16Uint16 is first with key of type uint32 and value of type pair(uint16,uint16).
func (c *Cursor) NextUint32ToUint16Uint16() (uint32, uint16, uint16) {
	rawKey, rawValue := c.Next()
	key := bytesTouint32(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key, value1, value2
}

// PrevUint32ToUint16Uint16 is first with key of type uint32 and value of type pair(uint16,uint16).
func (c *Cursor) PrevUint32ToUint16Uint16() (uint32, uint16, uint16) {
	rawKey, rawValue := c.Prev()
	key := bytesTouint32(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key, value1, value2
}

// SeekUint32ToUint16Uint16 is first with key of type uint32 and value of type pair(uint16,uint16).
func (c *Cursor) SeekUint32ToUint16Uint16(key uint32) (uint32, uint16, uint16) {
	rawKey, rawValue := c.Seek(uint32Tobytes(key))
	key = bytesTouint32(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key, value1, value2
}

// ForEachUint32ToUint32Uint32 is a for each with key of type uint32 and value of type pair(uint32,uint32).
func (b *Bucket) ForEachUint32ToUint32Uint32(fn func(key uint32, value1 uint32, value2 uint32) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := bytesTouint32(rawKey)
			value1, value2 := bytesTouint32uint32(rawValue)
			return fn(key, value1, value2)
		},
	)
}

// GetUint32ToUint32Uint32 is a get with key of type uint32 and value of type pair(uint32,uint32).
func (b *Bucket) GetUint32ToUint32Uint32(key uint32) (uint32, uint32) {
	return bytesTouint32uint32(b.Get(uint32Tobytes(key)))
}

// GetRequiredUint32ToUint32Uint32 is a get with key of type uint32 and value of type pair(uint32,uint32) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint32ToUint32Uint32(key uint32) (uint32, uint32, error) {
	value := b.Get(uint32Tobytes(key))
	if value == nil {
		return 0, 0, NewKeyNotFoundError(key)
	}
	value1, value2 := bytesTouint32uint32(value)
	return value1, value2, nil
}

// PutUint32ToUint32Uint32 is a put with key of type uint32 and value of type pair(uint32,uint32).
func (b *Bucket) PutUint32ToUint32Uint32(key uint32, value1 uint32, value2 uint32) error {
	return b.Put(uint32Tobytes(key), uint32uint32Tobytes(value1, value2))
}

// FirstUint32ToUint32Uint32 is first with key of type uint32 and value of type pair(uint32,uint32).
func (c *Cursor) FirstUint32ToUint32Uint32() (uint32, uint32, uint32) {
	rawKey, rawValue := c.First()
	key := bytesTouint32(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key, value1, value2
}

// LastUint32ToUint32Uint32 is first with key of type uint32 and value of type pair(uint32,uint32).
func (c *Cursor) LastUint32ToUint32Uint32() (uint32, uint32, uint32) {
	rawKey, rawValue := c.Last()
	key := bytesTouint32(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key, value1, value2
}

// NextUint32ToUint32Uint32 is first with key of type uint32 and value of type pair(uint32,uint32).
func (c *Cursor) NextUint32ToUint32Uint32() (uint32, uint32, uint32) {
	rawKey, rawValue := c.Next()
	key := bytesTouint32(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key, value1, value2
}

// PrevUint32ToUint32Uint32 is first with key of type uint32 and value of type pair(uint32,uint32).
func (c *Cursor) PrevUint32ToUint32Uint32() (uint32, uint32, uint32) {
	rawKey, rawValue := c.Prev()
	key := bytesTouint32(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key, value1, value2
}

// SeekUint32ToUint32Uint32 is first with key of type uint32 and value of type pair(uint32,uint32).
func (c *Cursor) SeekUint32ToUint32Uint32(key uint32) (uint32, uint32, uint32) {
	rawKey, rawValue := c.Seek(uint32Tobytes(key))
	key = bytesTouint32(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key, value1, value2
}

// ForEachUint32ToUint64Uint64 is a for each with key of type uint32 and value of type pair(uint64,uint64).
func (b *Bucket) ForEachUint32ToUint64Uint64(fn func(key uint32, value1 uint64, value2 uint64) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := bytesTouint32(rawKey)
			value1, value2 := bytesTouint64uint64(rawValue)
			return fn(key, value1, value2)
		},
	)
}

// GetUint32ToUint64Uint64 is a get with key of type uint32 and value of type pair(uint64,uint64).
func (b *Bucket) GetUint32ToUint64Uint64(key uint32) (uint64, uint64) {
	return bytesTouint64uint64(b.Get(uint32Tobytes(key)))
}

// GetRequiredUint32ToUint64Uint64 is a get with key of type uint32 and value of type pair(uint64,uint64) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint32ToUint64Uint64(key uint32) (uint64, uint64, error) {
	value := b.Get(uint32Tobytes(key))
	if value == nil {
		return 0, 0, NewKeyNotFoundError(key)
	}
	value1, value2 := bytesTouint64uint64(value)
	return value1, value2, nil
}

// PutUint32ToUint64Uint64 is a put with key of type uint32 and value of type pair(uint64,uint64).
func (b *Bucket) PutUint32ToUint64Uint64(key uint32, value1 uint64, value2 uint64) error {
	return b.Put(uint32Tobytes(key), uint64uint64Tobytes(value1, value2))
}

// FirstUint32ToUint64Uint64 is first with key of type uint32 and value of type pair(uint64,uint64).
func (c *Cursor) FirstUint32ToUint64Uint64() (uint32, uint64, uint64) {
	rawKey, rawValue := c.First()
	key := bytesTouint32(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key, value1, value2
}

// LastUint32ToUint64Uint64 is first with key of type uint32 and value of type pair(uint64,uint64).
func (c *Cursor) LastUint32ToUint64Uint64() (uint32, uint64, uint64) {
	rawKey, rawValue := c.Last()
	key := bytesTouint32(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key, value1, value2
}

// NextUint32ToUint64Uint64 is first with key of type uint32 and value of type pair(uint64,uint64).
func (c *Cursor) NextUint32ToUint64Uint64() (uint32, uint64, uint64) {
	rawKey, rawValue := c.Next()
	key := bytesTouint32(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key, value1, value2
}

// PrevUint32ToUint64Uint64 is first with key of type uint32 and value of type pair(uint64,uint64).
func (c *Cursor) PrevUint32ToUint64Uint64() (uint32, uint64, uint64) {
	rawKey, rawValue := c.Prev()
	key := bytesTouint32(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key, value1, value2
}

// SeekUint32ToUint64Uint64 is first with key of type uint32 and value of type pair(uint64,uint64).
func (c *Cursor) SeekUint32ToUint64Uint64(key uint32) (uint32, uint64, uint64) {
	rawKey, rawValue := c.Seek(uint32Tobytes(key))
	key = bytesTouint32(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key, value1, value2
}

// ForEachUint32ToUint16Bytes is a for each with key of type uint32 and value of type pair(uint16,[]byte).
func (b *Bucket) ForEachUint32ToUint16Bytes(fn func(key uint32, value1 uint16, value2 []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := bytesTouint32(rawKey)
			value1, value2 := bytesTouint16bytes(rawValue)
			return fn(key, value1, value2)
		},
	)
}

// GetUint32ToUint16Bytes is a get with key of type uint32 and value of type pair(uint16,[]byte).
func (b *Bucket) GetUint32ToUint16Bytes(key uint32) (uint16, []byte) {
	return bytesTouint16bytes(b.Get(uint32Tobytes(key)))
}

// GetRequiredUint32ToUint16Bytes is a get with key of type uint32 and value of type pair(uint16,[]byte) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint32ToUint16Bytes(key uint32) (uint16, []byte, error) {
	value := b.Get(uint32Tobytes(key))
	if value == nil {
		return 0, nil, NewKeyNotFoundError(key)
	}
	value1, value2 := bytesTouint16bytes(value)
	return value1, value2, nil
}

// PutUint32ToUint16Bytes is a put with key of type uint32 and value of type pair(uint16,[]byte).
func (b *Bucket) PutUint32ToUint16Bytes(key uint32, value1 uint16, value2 []byte) error {
	return b.Put(uint32Tobytes(key), uint16bytesTobytes(value1, value2))
}

// FirstUint32ToUint16Bytes is first with key of type uint32 and value of type pair(uint16,[]byte).
func (c *Cursor) FirstUint32ToUint16Bytes() (uint32, uint16, []byte) {
	rawKey, rawValue := c.First()
	key := bytesTouint32(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key, value1, value2
}

// LastUint32ToUint16Bytes is first with key of type uint32 and value of type pair(uint16,[]byte).
func (c *Cursor) LastUint32ToUint16Bytes() (uint32, uint16, []byte) {
	rawKey, rawValue := c.Last()
	key := bytesTouint32(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key, value1, value2
}

// NextUint32ToUint16Bytes is first with key of type uint32 and value of type pair(uint16,[]byte).
func (c *Cursor) NextUint32ToUint16Bytes() (uint32, uint16, []byte) {
	rawKey, rawValue := c.Next()
	key := bytesTouint32(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key, value1, value2
}

// PrevUint32ToUint16Bytes is first with key of type uint32 and value of type pair(uint16,[]byte).
func (c *Cursor) PrevUint32ToUint16Bytes() (uint32, uint16, []byte) {
	rawKey, rawValue := c.Prev()
	key := bytesTouint32(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key, value1, value2
}

// SeekUint32ToUint16Bytes is first with key of type uint32 and value of type pair(uint16,[]byte).
func (c *Cursor) SeekUint32ToUint16Bytes(key uint32) (uint32, uint16, []byte) {
	rawKey, rawValue := c.Seek(uint32Tobytes(key))
	key = bytesTouint32(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key, value1, value2
}

// ForEachUint32ToUint32Bytes is a for each with key of type uint32 and value of type pair(uint32,[]byte).
func (b *Bucket) ForEachUint32ToUint32Bytes(fn func(key uint32, value1 uint32, value2 []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := bytesTouint32(rawKey)
			value1, value2 := bytesTouint32bytes(rawValue)
			return fn(key, value1, value2)
		},
	)
}

// GetUint32ToUint32Bytes is a get with key of type uint32 and value of type pair(uint32,[]byte).
func (b *Bucket) GetUint32ToUint32Bytes(key uint32) (uint32, []byte) {
	return bytesTouint32bytes(b.Get(uint32Tobytes(key)))
}

// GetRequiredUint32ToUint32Bytes is a get with key of type uint32 and value of type pair(uint32,[]byte) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint32ToUint32Bytes(key uint32) (uint32, []byte, error) {
	value := b.Get(uint32Tobytes(key))
	if value == nil {
		return 0, nil, NewKeyNotFoundError(key)
	}
	value1, value2 := bytesTouint32bytes(value)
	return value1, value2, nil
}

// PutUint32ToUint32Bytes is a put with key of type uint32 and value of type pair(uint32,[]byte).
func (b *Bucket) PutUint32ToUint32Bytes(key uint32, value1 uint32, value2 []byte) error {
	return b.Put(uint32Tobytes(key), uint32bytesTobytes(value1, value2))
}

// FirstUint32ToUint32Bytes is first with key of type uint32 and value of type pair(uint32,[]byte).
func (c *Cursor) FirstUint32ToUint32Bytes() (uint32, uint32, []byte) {
	rawKey, rawValue := c.First()
	key := bytesTouint32(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key, value1, value2
}

// LastUint32ToUint32Bytes is first with key of type uint32 and value of type pair(uint32,[]byte).
func (c *Cursor) LastUint32ToUint32Bytes() (uint32, uint32, []byte) {
	rawKey, rawValue := c.Last()
	key := bytesTouint32(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key, value1, value2
}

// NextUint32ToUint32Bytes is first with key of type uint32 and value of type pair(uint32,[]byte).
func (c *Cursor) NextUint32ToUint32Bytes() (uint32, uint32, []byte) {
	rawKey, rawValue := c.Next()
	key := bytesTouint32(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key, value1, value2
}

// PrevUint32ToUint32Bytes is first with key of type uint32 and value of type pair(uint32,[]byte).
func (c *Cursor) PrevUint32ToUint32Bytes() (uint32, uint32, []byte) {
	rawKey, rawValue := c.Prev()
	key := bytesTouint32(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key, value1, value2
}

// SeekUint32ToUint32Bytes is first with key of type uint32 and value of type pair(uint32,[]byte).
func (c *Cursor) SeekUint32ToUint32Bytes(key uint32) (uint32, uint32, []byte) {
	rawKey, rawValue := c.Seek(uint32Tobytes(key))
	key = bytesTouint32(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key, value1, value2
}

// ForEachUint32ToUint64Bytes is a for each with key of type uint32 and value of type pair(uint64,[]byte).
func (b *Bucket) ForEachUint32ToUint64Bytes(fn func(key uint32, value1 uint64, value2 []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := bytesTouint32(rawKey)
			value1, value2 := bytesTouint64bytes(rawValue)
			return fn(key, value1, value2)
		},
	)
}

// GetUint32ToUint64Bytes is a get with key of type uint32 and value of type pair(uint64,[]byte).
func (b *Bucket) GetUint32ToUint64Bytes(key uint32) (uint64, []byte) {
	return bytesTouint64bytes(b.Get(uint32Tobytes(key)))
}

// GetRequiredUint32ToUint64Bytes is a get with key of type uint32 and value of type pair(uint64,[]byte) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint32ToUint64Bytes(key uint32) (uint64, []byte, error) {
	value := b.Get(uint32Tobytes(key))
	if value == nil {
		return 0, nil, NewKeyNotFoundError(key)
	}
	value1, value2 := bytesTouint64bytes(value)
	return value1, value2, nil
}

// PutUint32ToUint64Bytes is a put with key of type uint32 and value of type pair(uint64,[]byte).
func (b *Bucket) PutUint32ToUint64Bytes(key uint32, value1 uint64, value2 []byte) error {
	return b.Put(uint32Tobytes(key), uint64bytesTobytes(value1, value2))
}

// FirstUint32ToUint64Bytes is first with key of type uint32 and value of type pair(uint64,[]byte).
func (c *Cursor) FirstUint32ToUint64Bytes() (uint32, uint64, []byte) {
	rawKey, rawValue := c.First()
	key := bytesTouint32(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key, value1, value2
}

// LastUint32ToUint64Bytes is first with key of type uint32 and value of type pair(uint64,[]byte).
func (c *Cursor) LastUint32ToUint64Bytes() (uint32, uint64, []byte) {
	rawKey, rawValue := c.Last()
	key := bytesTouint32(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key, value1, value2
}

// NextUint32ToUint64Bytes is first with key of type uint32 and value of type pair(uint64,[]byte).
func (c *Cursor) NextUint32ToUint64Bytes() (uint32, uint64, []byte) {
	rawKey, rawValue := c.Next()
	key := bytesTouint32(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key, value1, value2
}

// PrevUint32ToUint64Bytes is first with key of type uint32 and value of type pair(uint64,[]byte).
func (c *Cursor) PrevUint32ToUint64Bytes() (uint32, uint64, []byte) {
	rawKey, rawValue := c.Prev()
	key := bytesTouint32(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key, value1, value2
}

// SeekUint32ToUint64Bytes is first with key of type uint32 and value of type pair(uint64,[]byte).
func (c *Cursor) SeekUint32ToUint64Bytes(key uint32) (uint32, uint64, []byte) {
	rawKey, rawValue := c.Seek(uint32Tobytes(key))
	key = bytesTouint32(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key, value1, value2
}

// ForEachUint32ToBytesUint16 is a for each with key of type uint32 and value of type pair([]byte,uint16).
func (b *Bucket) ForEachUint32ToBytesUint16(fn func(key uint32, value1 []byte, value2 uint16) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := bytesTouint32(rawKey)
			value1, value2 := bytesTobytesuint16(rawValue)
			return fn(key, value1, value2)
		},
	)
}

// GetUint32ToBytesUint16 is a get with key of type uint32 and value of type pair([]byte,uint16).
func (b *Bucket) GetUint32ToBytesUint16(key uint32) ([]byte, uint16) {
	return bytesTobytesuint16(b.Get(uint32Tobytes(key)))
}

// GetRequiredUint32ToBytesUint16 is a get with key of type uint32 and value of type pair([]byte,uint16) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint32ToBytesUint16(key uint32) ([]byte, uint16, error) {
	value := b.Get(uint32Tobytes(key))
	if value == nil {
		return nil, 0, NewKeyNotFoundError(key)
	}
	value1, value2 := bytesTobytesuint16(value)
	return value1, value2, nil
}

// PutUint32ToBytesUint16 is a put with key of type uint32 and value of type pair([]byte,uint16).
func (b *Bucket) PutUint32ToBytesUint16(key uint32, value1 []byte, value2 uint16) error {
	return b.Put(uint32Tobytes(key), bytesuint16Tobytes(value1, value2))
}

// FirstUint32ToBytesUint16 is first with key of type uint32 and value of type pair([]byte,uint16).
func (c *Cursor) FirstUint32ToBytesUint16() (uint32, []byte, uint16) {
	rawKey, rawValue := c.First()
	key := bytesTouint32(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key, value1, value2
}

// LastUint32ToBytesUint16 is first with key of type uint32 and value of type pair([]byte,uint16).
func (c *Cursor) LastUint32ToBytesUint16() (uint32, []byte, uint16) {
	rawKey, rawValue := c.Last()
	key := bytesTouint32(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key, value1, value2
}

// NextUint32ToBytesUint16 is first with key of type uint32 and value of type pair([]byte,uint16).
func (c *Cursor) NextUint32ToBytesUint16() (uint32, []byte, uint16) {
	rawKey, rawValue := c.Next()
	key := bytesTouint32(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key, value1, value2
}

// PrevUint32ToBytesUint16 is first with key of type uint32 and value of type pair([]byte,uint16).
func (c *Cursor) PrevUint32ToBytesUint16() (uint32, []byte, uint16) {
	rawKey, rawValue := c.Prev()
	key := bytesTouint32(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key, value1, value2
}

// SeekUint32ToBytesUint16 is first with key of type uint32 and value of type pair([]byte,uint16).
func (c *Cursor) SeekUint32ToBytesUint16(key uint32) (uint32, []byte, uint16) {
	rawKey, rawValue := c.Seek(uint32Tobytes(key))
	key = bytesTouint32(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key, value1, value2
}

// ForEachUint32ToBytesUint32 is a for each with key of type uint32 and value of type pair([]byte,uint32).
func (b *Bucket) ForEachUint32ToBytesUint32(fn func(key uint32, value1 []byte, value2 uint32) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := bytesTouint32(rawKey)
			value1, value2 := bytesTobytesuint32(rawValue)
			return fn(key, value1, value2)
		},
	)
}

// GetUint32ToBytesUint32 is a get with key of type uint32 and value of type pair([]byte,uint32).
func (b *Bucket) GetUint32ToBytesUint32(key uint32) ([]byte, uint32) {
	return bytesTobytesuint32(b.Get(uint32Tobytes(key)))
}

// GetRequiredUint32ToBytesUint32 is a get with key of type uint32 and value of type pair([]byte,uint32) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint32ToBytesUint32(key uint32) ([]byte, uint32, error) {
	value := b.Get(uint32Tobytes(key))
	if value == nil {
		return nil, 0, NewKeyNotFoundError(key)
	}
	value1, value2 := bytesTobytesuint32(value)
	return value1, value2, nil
}

// PutUint32ToBytesUint32 is a put with key of type uint32 and value of type pair([]byte,uint32).
func (b *Bucket) PutUint32ToBytesUint32(key uint32, value1 []byte, value2 uint32) error {
	return b.Put(uint32Tobytes(key), bytesuint32Tobytes(value1, value2))
}

// FirstUint32ToBytesUint32 is first with key of type uint32 and value of type pair([]byte,uint32).
func (c *Cursor) FirstUint32ToBytesUint32() (uint32, []byte, uint32) {
	rawKey, rawValue := c.First()
	key := bytesTouint32(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key, value1, value2
}

// LastUint32ToBytesUint32 is first with key of type uint32 and value of type pair([]byte,uint32).
func (c *Cursor) LastUint32ToBytesUint32() (uint32, []byte, uint32) {
	rawKey, rawValue := c.Last()
	key := bytesTouint32(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key, value1, value2
}

// NextUint32ToBytesUint32 is first with key of type uint32 and value of type pair([]byte,uint32).
func (c *Cursor) NextUint32ToBytesUint32() (uint32, []byte, uint32) {
	rawKey, rawValue := c.Next()
	key := bytesTouint32(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key, value1, value2
}

// PrevUint32ToBytesUint32 is first with key of type uint32 and value of type pair([]byte,uint32).
func (c *Cursor) PrevUint32ToBytesUint32() (uint32, []byte, uint32) {
	rawKey, rawValue := c.Prev()
	key := bytesTouint32(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key, value1, value2
}

// SeekUint32ToBytesUint32 is first with key of type uint32 and value of type pair([]byte,uint32).
func (c *Cursor) SeekUint32ToBytesUint32(key uint32) (uint32, []byte, uint32) {
	rawKey, rawValue := c.Seek(uint32Tobytes(key))
	key = bytesTouint32(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key, value1, value2
}

// ForEachUint32ToBytesUint64 is a for each with key of type uint32 and value of type pair([]byte,uint64).
func (b *Bucket) ForEachUint32ToBytesUint64(fn func(key uint32, value1 []byte, value2 uint64) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := bytesTouint32(rawKey)
			value1, value2 := bytesTobytesuint64(rawValue)
			return fn(key, value1, value2)
		},
	)
}

// GetUint32ToBytesUint64 is a get with key of type uint32 and value of type pair([]byte,uint64).
func (b *Bucket) GetUint32ToBytesUint64(key uint32) ([]byte, uint64) {
	return bytesTobytesuint64(b.Get(uint32Tobytes(key)))
}

// GetRequiredUint32ToBytesUint64 is a get with key of type uint32 and value of type pair([]byte,uint64) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint32ToBytesUint64(key uint32) ([]byte, uint64, error) {
	value := b.Get(uint32Tobytes(key))
	if value == nil {
		return nil, 0, NewKeyNotFoundError(key)
	}
	value1, value2 := bytesTobytesuint64(value)
	return value1, value2, nil
}

// PutUint32ToBytesUint64 is a put with key of type uint32 and value of type pair([]byte,uint64).
func (b *Bucket) PutUint32ToBytesUint64(key uint32, value1 []byte, value2 uint64) error {
	return b.Put(uint32Tobytes(key), bytesuint64Tobytes(value1, value2))
}

// FirstUint32ToBytesUint64 is first with key of type uint32 and value of type pair([]byte,uint64).
func (c *Cursor) FirstUint32ToBytesUint64() (uint32, []byte, uint64) {
	rawKey, rawValue := c.First()
	key := bytesTouint32(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key, value1, value2
}

// LastUint32ToBytesUint64 is first with key of type uint32 and value of type pair([]byte,uint64).
func (c *Cursor) LastUint32ToBytesUint64() (uint32, []byte, uint64) {
	rawKey, rawValue := c.Last()
	key := bytesTouint32(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key, value1, value2
}

// NextUint32ToBytesUint64 is first with key of type uint32 and value of type pair([]byte,uint64).
func (c *Cursor) NextUint32ToBytesUint64() (uint32, []byte, uint64) {
	rawKey, rawValue := c.Next()
	key := bytesTouint32(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key, value1, value2
}

// PrevUint32ToBytesUint64 is first with key of type uint32 and value of type pair([]byte,uint64).
func (c *Cursor) PrevUint32ToBytesUint64() (uint32, []byte, uint64) {
	rawKey, rawValue := c.Prev()
	key := bytesTouint32(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key, value1, value2
}

// SeekUint32ToBytesUint64 is first with key of type uint32 and value of type pair([]byte,uint64).
func (c *Cursor) SeekUint32ToBytesUint64(key uint32) (uint32, []byte, uint64) {
	rawKey, rawValue := c.Seek(uint32Tobytes(key))
	key = bytesTouint32(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key, value1, value2
}

// DeleteUint32 is a delete with key of type uint32.
func (b *Bucket) DeleteUint32(key uint32) error {
	return b.Delete(uint32Tobytes(key))
}

// ForEachUint64ToBytes is a for each with key of type uint64 and value of type []byte.
func (b *Bucket) ForEachUint64ToBytes(fn func(key uint64, value []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := bytesTouint64(rawKey)
			value := (rawValue)
			return fn(key, value)
		},
	)
}

// GetUint64ToBytes is a get with key of type uint64 and value of type []byte.
func (b *Bucket) GetUint64ToBytes(key uint64) []byte {
	return (b.Get(uint64Tobytes(key)))
}

// GetRequiredUint64ToBytes is a get with key of type uint64 and value of type []byte that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint64ToBytes(key uint64) ([]byte, error) {
	value := b.Get(uint64Tobytes(key))
	if value == nil {
		return nil, NewKeyNotFoundError(key)
	}
	return (value), nil
}

// PutUint64ToBytes is a put with key of type uint64 and value of type []byte.
func (b *Bucket) PutUint64ToBytes(key uint64, value []byte) error {
	return b.Put(uint64Tobytes(key), (value))
}

// FirstUint64ToBytes is first with key of type uint64 and value of type []byte.
func (c *Cursor) FirstUint64ToBytes() (uint64, []byte) {
	rawKey, rawValue := c.First()
	key := bytesTouint64(rawKey)
	value := (rawValue)
	return key, value
}

// LastUint64ToBytes is first with key of type uint64 and value of type []byte.
func (c *Cursor) LastUint64ToBytes() (uint64, []byte) {
	rawKey, rawValue := c.Last()
	key := bytesTouint64(rawKey)
	value := (rawValue)
	return key, value
}

// NextUint64ToBytes is first with key of type uint64 and value of type []byte.
func (c *Cursor) NextUint64ToBytes() (uint64, []byte) {
	rawKey, rawValue := c.Next()
	key := bytesTouint64(rawKey)
	value := (rawValue)
	return key, value
}

// PrevUint64ToBytes is first with key of type uint64 and value of type []byte.
func (c *Cursor) PrevUint64ToBytes() (uint64, []byte) {
	rawKey, rawValue := c.Prev()
	key := bytesTouint64(rawKey)
	value := (rawValue)
	return key, value
}

// SeekUint64ToBytes is first with key of type uint64 and value of type []byte.
func (c *Cursor) SeekUint64ToBytes(key uint64) (uint64, []byte) {
	rawKey, rawValue := c.Seek(uint64Tobytes(key))
	key = bytesTouint64(rawKey)
	value := (rawValue)
	return key, value
}

// ForEachUint64ToString is a for each with key of type uint64 and value of type string.
func (b *Bucket) ForEachUint64ToString(fn func(key uint64, value string) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := bytesTouint64(rawKey)
			value := string(rawValue)
			return fn(key, value)
		},
	)
}

// GetUint64ToString is a get with key of type uint64 and value of type string.
func (b *Bucket) GetUint64ToString(key uint64) string {
	return string(b.Get(uint64Tobytes(key)))
}

// GetRequiredUint64ToString is a get with key of type uint64 and value of type string that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint64ToString(key uint64) (string, error) {
	value := b.Get(uint64Tobytes(key))
	if value == nil {
		return "", NewKeyNotFoundError(key)
	}
	return string(value), nil
}

// PutUint64ToString is a put with key of type uint64 and value of type string.
func (b *Bucket) PutUint64ToString(key uint64, value string) error {
	return b.Put(uint64Tobytes(key), []byte(value))
}

// FirstUint64ToString is first with key of type uint64 and value of type string.
func (c *Cursor) FirstUint64ToString() (uint64, string) {
	rawKey, rawValue := c.First()
	key := bytesTouint64(rawKey)
	value := string(rawValue)
	return key, value
}

// LastUint64ToString is first with key of type uint64 and value of type string.
func (c *Cursor) LastUint64ToString() (uint64, string) {
	rawKey, rawValue := c.Last()
	key := bytesTouint64(rawKey)
	value := string(rawValue)
	return key, value
}

// NextUint64ToString is first with key of type uint64 and value of type string.
func (c *Cursor) NextUint64ToString() (uint64, string) {
	rawKey, rawValue := c.Next()
	key := bytesTouint64(rawKey)
	value := string(rawValue)
	return key, value
}

// PrevUint64ToString is first with key of type uint64 and value of type string.
func (c *Cursor) PrevUint64ToString() (uint64, string) {
	rawKey, rawValue := c.Prev()
	key := bytesTouint64(rawKey)
	value := string(rawValue)
	return key, value
}

// SeekUint64ToString is first with key of type uint64 and value of type string.
func (c *Cursor) SeekUint64ToString(key uint64) (uint64, string) {
	rawKey, rawValue := c.Seek(uint64Tobytes(key))
	key = bytesTouint64(rawKey)
	value := string(rawValue)
	return key, value
}

// ForEachUint64ToUint16 is a for each with key of type uint64 and value of type uint16.
func (b *Bucket) ForEachUint64ToUint16(fn func(key uint64, value uint16) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := bytesTouint64(rawKey)
			value := bytesTouint16(rawValue)
			return fn(key, value)
		},
	)
}

// GetUint64ToUint16 is a get with key of type uint64 and value of type uint16.
func (b *Bucket) GetUint64ToUint16(key uint64) uint16 {
	return bytesTouint16(b.Get(uint64Tobytes(key)))
}

// GetRequiredUint64ToUint16 is a get with key of type uint64 and value of type uint16 that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint64ToUint16(key uint64) (uint16, error) {
	value := b.Get(uint64Tobytes(key))
	if value == nil {
		return 0, NewKeyNotFoundError(key)
	}
	return bytesTouint16(value), nil
}

// PutUint64ToUint16 is a put with key of type uint64 and value of type uint16.
func (b *Bucket) PutUint64ToUint16(key uint64, value uint16) error {
	return b.Put(uint64Tobytes(key), uint16Tobytes(value))
}

// FirstUint64ToUint16 is first with key of type uint64 and value of type uint16.
func (c *Cursor) FirstUint64ToUint16() (uint64, uint16) {
	rawKey, rawValue := c.First()
	key := bytesTouint64(rawKey)
	value := bytesTouint16(rawValue)
	return key, value
}

// LastUint64ToUint16 is first with key of type uint64 and value of type uint16.
func (c *Cursor) LastUint64ToUint16() (uint64, uint16) {
	rawKey, rawValue := c.Last()
	key := bytesTouint64(rawKey)
	value := bytesTouint16(rawValue)
	return key, value
}

// NextUint64ToUint16 is first with key of type uint64 and value of type uint16.
func (c *Cursor) NextUint64ToUint16() (uint64, uint16) {
	rawKey, rawValue := c.Next()
	key := bytesTouint64(rawKey)
	value := bytesTouint16(rawValue)
	return key, value
}

// PrevUint64ToUint16 is first with key of type uint64 and value of type uint16.
func (c *Cursor) PrevUint64ToUint16() (uint64, uint16) {
	rawKey, rawValue := c.Prev()
	key := bytesTouint64(rawKey)
	value := bytesTouint16(rawValue)
	return key, value
}

// SeekUint64ToUint16 is first with key of type uint64 and value of type uint16.
func (c *Cursor) SeekUint64ToUint16(key uint64) (uint64, uint16) {
	rawKey, rawValue := c.Seek(uint64Tobytes(key))
	key = bytesTouint64(rawKey)
	value := bytesTouint16(rawValue)
	return key, value
}

// ForEachUint64ToUint32 is a for each with key of type uint64 and value of type uint32.
func (b *Bucket) ForEachUint64ToUint32(fn func(key uint64, value uint32) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := bytesTouint64(rawKey)
			value := bytesTouint32(rawValue)
			return fn(key, value)
		},
	)
}

// GetUint64ToUint32 is a get with key of type uint64 and value of type uint32.
func (b *Bucket) GetUint64ToUint32(key uint64) uint32 {
	return bytesTouint32(b.Get(uint64Tobytes(key)))
}

// GetRequiredUint64ToUint32 is a get with key of type uint64 and value of type uint32 that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint64ToUint32(key uint64) (uint32, error) {
	value := b.Get(uint64Tobytes(key))
	if value == nil {
		return 0, NewKeyNotFoundError(key)
	}
	return bytesTouint32(value), nil
}

// PutUint64ToUint32 is a put with key of type uint64 and value of type uint32.
func (b *Bucket) PutUint64ToUint32(key uint64, value uint32) error {
	return b.Put(uint64Tobytes(key), uint32Tobytes(value))
}

// FirstUint64ToUint32 is first with key of type uint64 and value of type uint32.
func (c *Cursor) FirstUint64ToUint32() (uint64, uint32) {
	rawKey, rawValue := c.First()
	key := bytesTouint64(rawKey)
	value := bytesTouint32(rawValue)
	return key, value
}

// LastUint64ToUint32 is first with key of type uint64 and value of type uint32.
func (c *Cursor) LastUint64ToUint32() (uint64, uint32) {
	rawKey, rawValue := c.Last()
	key := bytesTouint64(rawKey)
	value := bytesTouint32(rawValue)
	return key, value
}

// NextUint64ToUint32 is first with key of type uint64 and value of type uint32.
func (c *Cursor) NextUint64ToUint32() (uint64, uint32) {
	rawKey, rawValue := c.Next()
	key := bytesTouint64(rawKey)
	value := bytesTouint32(rawValue)
	return key, value
}

// PrevUint64ToUint32 is first with key of type uint64 and value of type uint32.
func (c *Cursor) PrevUint64ToUint32() (uint64, uint32) {
	rawKey, rawValue := c.Prev()
	key := bytesTouint64(rawKey)
	value := bytesTouint32(rawValue)
	return key, value
}

// SeekUint64ToUint32 is first with key of type uint64 and value of type uint32.
func (c *Cursor) SeekUint64ToUint32(key uint64) (uint64, uint32) {
	rawKey, rawValue := c.Seek(uint64Tobytes(key))
	key = bytesTouint64(rawKey)
	value := bytesTouint32(rawValue)
	return key, value
}

// ForEachUint64ToUint64 is a for each with key of type uint64 and value of type uint64.
func (b *Bucket) ForEachUint64ToUint64(fn func(key uint64, value uint64) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := bytesTouint64(rawKey)
			value := bytesTouint64(rawValue)
			return fn(key, value)
		},
	)
}

// GetUint64ToUint64 is a get with key of type uint64 and value of type uint64.
func (b *Bucket) GetUint64ToUint64(key uint64) uint64 {
	return bytesTouint64(b.Get(uint64Tobytes(key)))
}

// GetRequiredUint64ToUint64 is a get with key of type uint64 and value of type uint64 that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint64ToUint64(key uint64) (uint64, error) {
	value := b.Get(uint64Tobytes(key))
	if value == nil {
		return 0, NewKeyNotFoundError(key)
	}
	return bytesTouint64(value), nil
}

// PutUint64ToUint64 is a put with key of type uint64 and value of type uint64.
func (b *Bucket) PutUint64ToUint64(key uint64, value uint64) error {
	return b.Put(uint64Tobytes(key), uint64Tobytes(value))
}

// FirstUint64ToUint64 is first with key of type uint64 and value of type uint64.
func (c *Cursor) FirstUint64ToUint64() (uint64, uint64) {
	rawKey, rawValue := c.First()
	key := bytesTouint64(rawKey)
	value := bytesTouint64(rawValue)
	return key, value
}

// LastUint64ToUint64 is first with key of type uint64 and value of type uint64.
func (c *Cursor) LastUint64ToUint64() (uint64, uint64) {
	rawKey, rawValue := c.Last()
	key := bytesTouint64(rawKey)
	value := bytesTouint64(rawValue)
	return key, value
}

// NextUint64ToUint64 is first with key of type uint64 and value of type uint64.
func (c *Cursor) NextUint64ToUint64() (uint64, uint64) {
	rawKey, rawValue := c.Next()
	key := bytesTouint64(rawKey)
	value := bytesTouint64(rawValue)
	return key, value
}

// PrevUint64ToUint64 is first with key of type uint64 and value of type uint64.
func (c *Cursor) PrevUint64ToUint64() (uint64, uint64) {
	rawKey, rawValue := c.Prev()
	key := bytesTouint64(rawKey)
	value := bytesTouint64(rawValue)
	return key, value
}

// SeekUint64ToUint64 is first with key of type uint64 and value of type uint64.
func (c *Cursor) SeekUint64ToUint64(key uint64) (uint64, uint64) {
	rawKey, rawValue := c.Seek(uint64Tobytes(key))
	key = bytesTouint64(rawKey)
	value := bytesTouint64(rawValue)
	return key, value
}

// ForEachUint64ToUint16Uint16 is a for each with key of type uint64 and value of type pair(uint16,uint16).
func (b *Bucket) ForEachUint64ToUint16Uint16(fn func(key uint64, value1 uint16, value2 uint16) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := bytesTouint64(rawKey)
			value1, value2 := bytesTouint16uint16(rawValue)
			return fn(key, value1, value2)
		},
	)
}

// GetUint64ToUint16Uint16 is a get with key of type uint64 and value of type pair(uint16,uint16).
func (b *Bucket) GetUint64ToUint16Uint16(key uint64) (uint16, uint16) {
	return bytesTouint16uint16(b.Get(uint64Tobytes(key)))
}

// GetRequiredUint64ToUint16Uint16 is a get with key of type uint64 and value of type pair(uint16,uint16) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint64ToUint16Uint16(key uint64) (uint16, uint16, error) {
	value := b.Get(uint64Tobytes(key))
	if value == nil {
		return 0, 0, NewKeyNotFoundError(key)
	}
	value1, value2 := bytesTouint16uint16(value)
	return value1, value2, nil
}

// PutUint64ToUint16Uint16 is a put with key of type uint64 and value of type pair(uint16,uint16).
func (b *Bucket) PutUint64ToUint16Uint16(key uint64, value1 uint16, value2 uint16) error {
	return b.Put(uint64Tobytes(key), uint16uint16Tobytes(value1, value2))
}

// FirstUint64ToUint16Uint16 is first with key of type uint64 and value of type pair(uint16,uint16).
func (c *Cursor) FirstUint64ToUint16Uint16() (uint64, uint16, uint16) {
	rawKey, rawValue := c.First()
	key := bytesTouint64(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key, value1, value2
}

// LastUint64ToUint16Uint16 is first with key of type uint64 and value of type pair(uint16,uint16).
func (c *Cursor) LastUint64ToUint16Uint16() (uint64, uint16, uint16) {
	rawKey, rawValue := c.Last()
	key := bytesTouint64(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key, value1, value2
}

// NextUint64ToUint16Uint16 is first with key of type uint64 and value of type pair(uint16,uint16).
func (c *Cursor) NextUint64ToUint16Uint16() (uint64, uint16, uint16) {
	rawKey, rawValue := c.Next()
	key := bytesTouint64(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key, value1, value2
}

// PrevUint64ToUint16Uint16 is first with key of type uint64 and value of type pair(uint16,uint16).
func (c *Cursor) PrevUint64ToUint16Uint16() (uint64, uint16, uint16) {
	rawKey, rawValue := c.Prev()
	key := bytesTouint64(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key, value1, value2
}

// SeekUint64ToUint16Uint16 is first with key of type uint64 and value of type pair(uint16,uint16).
func (c *Cursor) SeekUint64ToUint16Uint16(key uint64) (uint64, uint16, uint16) {
	rawKey, rawValue := c.Seek(uint64Tobytes(key))
	key = bytesTouint64(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key, value1, value2
}

// ForEachUint64ToUint32Uint32 is a for each with key of type uint64 and value of type pair(uint32,uint32).
func (b *Bucket) ForEachUint64ToUint32Uint32(fn func(key uint64, value1 uint32, value2 uint32) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := bytesTouint64(rawKey)
			value1, value2 := bytesTouint32uint32(rawValue)
			return fn(key, value1, value2)
		},
	)
}

// GetUint64ToUint32Uint32 is a get with key of type uint64 and value of type pair(uint32,uint32).
func (b *Bucket) GetUint64ToUint32Uint32(key uint64) (uint32, uint32) {
	return bytesTouint32uint32(b.Get(uint64Tobytes(key)))
}

// GetRequiredUint64ToUint32Uint32 is a get with key of type uint64 and value of type pair(uint32,uint32) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint64ToUint32Uint32(key uint64) (uint32, uint32, error) {
	value := b.Get(uint64Tobytes(key))
	if value == nil {
		return 0, 0, NewKeyNotFoundError(key)
	}
	value1, value2 := bytesTouint32uint32(value)
	return value1, value2, nil
}

// PutUint64ToUint32Uint32 is a put with key of type uint64 and value of type pair(uint32,uint32).
func (b *Bucket) PutUint64ToUint32Uint32(key uint64, value1 uint32, value2 uint32) error {
	return b.Put(uint64Tobytes(key), uint32uint32Tobytes(value1, value2))
}

// FirstUint64ToUint32Uint32 is first with key of type uint64 and value of type pair(uint32,uint32).
func (c *Cursor) FirstUint64ToUint32Uint32() (uint64, uint32, uint32) {
	rawKey, rawValue := c.First()
	key := bytesTouint64(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key, value1, value2
}

// LastUint64ToUint32Uint32 is first with key of type uint64 and value of type pair(uint32,uint32).
func (c *Cursor) LastUint64ToUint32Uint32() (uint64, uint32, uint32) {
	rawKey, rawValue := c.Last()
	key := bytesTouint64(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key, value1, value2
}

// NextUint64ToUint32Uint32 is first with key of type uint64 and value of type pair(uint32,uint32).
func (c *Cursor) NextUint64ToUint32Uint32() (uint64, uint32, uint32) {
	rawKey, rawValue := c.Next()
	key := bytesTouint64(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key, value1, value2
}

// PrevUint64ToUint32Uint32 is first with key of type uint64 and value of type pair(uint32,uint32).
func (c *Cursor) PrevUint64ToUint32Uint32() (uint64, uint32, uint32) {
	rawKey, rawValue := c.Prev()
	key := bytesTouint64(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key, value1, value2
}

// SeekUint64ToUint32Uint32 is first with key of type uint64 and value of type pair(uint32,uint32).
func (c *Cursor) SeekUint64ToUint32Uint32(key uint64) (uint64, uint32, uint32) {
	rawKey, rawValue := c.Seek(uint64Tobytes(key))
	key = bytesTouint64(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key, value1, value2
}

// ForEachUint64ToUint64Uint64 is a for each with key of type uint64 and value of type pair(uint64,uint64).
func (b *Bucket) ForEachUint64ToUint64Uint64(fn func(key uint64, value1 uint64, value2 uint64) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := bytesTouint64(rawKey)
			value1, value2 := bytesTouint64uint64(rawValue)
			return fn(key, value1, value2)
		},
	)
}

// GetUint64ToUint64Uint64 is a get with key of type uint64 and value of type pair(uint64,uint64).
func (b *Bucket) GetUint64ToUint64Uint64(key uint64) (uint64, uint64) {
	return bytesTouint64uint64(b.Get(uint64Tobytes(key)))
}

// GetRequiredUint64ToUint64Uint64 is a get with key of type uint64 and value of type pair(uint64,uint64) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint64ToUint64Uint64(key uint64) (uint64, uint64, error) {
	value := b.Get(uint64Tobytes(key))
	if value == nil {
		return 0, 0, NewKeyNotFoundError(key)
	}
	value1, value2 := bytesTouint64uint64(value)
	return value1, value2, nil
}

// PutUint64ToUint64Uint64 is a put with key of type uint64 and value of type pair(uint64,uint64).
func (b *Bucket) PutUint64ToUint64Uint64(key uint64, value1 uint64, value2 uint64) error {
	return b.Put(uint64Tobytes(key), uint64uint64Tobytes(value1, value2))
}

// FirstUint64ToUint64Uint64 is first with key of type uint64 and value of type pair(uint64,uint64).
func (c *Cursor) FirstUint64ToUint64Uint64() (uint64, uint64, uint64) {
	rawKey, rawValue := c.First()
	key := bytesTouint64(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key, value1, value2
}

// LastUint64ToUint64Uint64 is first with key of type uint64 and value of type pair(uint64,uint64).
func (c *Cursor) LastUint64ToUint64Uint64() (uint64, uint64, uint64) {
	rawKey, rawValue := c.Last()
	key := bytesTouint64(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key, value1, value2
}

// NextUint64ToUint64Uint64 is first with key of type uint64 and value of type pair(uint64,uint64).
func (c *Cursor) NextUint64ToUint64Uint64() (uint64, uint64, uint64) {
	rawKey, rawValue := c.Next()
	key := bytesTouint64(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key, value1, value2
}

// PrevUint64ToUint64Uint64 is first with key of type uint64 and value of type pair(uint64,uint64).
func (c *Cursor) PrevUint64ToUint64Uint64() (uint64, uint64, uint64) {
	rawKey, rawValue := c.Prev()
	key := bytesTouint64(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key, value1, value2
}

// SeekUint64ToUint64Uint64 is first with key of type uint64 and value of type pair(uint64,uint64).
func (c *Cursor) SeekUint64ToUint64Uint64(key uint64) (uint64, uint64, uint64) {
	rawKey, rawValue := c.Seek(uint64Tobytes(key))
	key = bytesTouint64(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key, value1, value2
}

// ForEachUint64ToUint16Bytes is a for each with key of type uint64 and value of type pair(uint16,[]byte).
func (b *Bucket) ForEachUint64ToUint16Bytes(fn func(key uint64, value1 uint16, value2 []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := bytesTouint64(rawKey)
			value1, value2 := bytesTouint16bytes(rawValue)
			return fn(key, value1, value2)
		},
	)
}

// GetUint64ToUint16Bytes is a get with key of type uint64 and value of type pair(uint16,[]byte).
func (b *Bucket) GetUint64ToUint16Bytes(key uint64) (uint16, []byte) {
	return bytesTouint16bytes(b.Get(uint64Tobytes(key)))
}

// GetRequiredUint64ToUint16Bytes is a get with key of type uint64 and value of type pair(uint16,[]byte) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint64ToUint16Bytes(key uint64) (uint16, []byte, error) {
	value := b.Get(uint64Tobytes(key))
	if value == nil {
		return 0, nil, NewKeyNotFoundError(key)
	}
	value1, value2 := bytesTouint16bytes(value)
	return value1, value2, nil
}

// PutUint64ToUint16Bytes is a put with key of type uint64 and value of type pair(uint16,[]byte).
func (b *Bucket) PutUint64ToUint16Bytes(key uint64, value1 uint16, value2 []byte) error {
	return b.Put(uint64Tobytes(key), uint16bytesTobytes(value1, value2))
}

// FirstUint64ToUint16Bytes is first with key of type uint64 and value of type pair(uint16,[]byte).
func (c *Cursor) FirstUint64ToUint16Bytes() (uint64, uint16, []byte) {
	rawKey, rawValue := c.First()
	key := bytesTouint64(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key, value1, value2
}

// LastUint64ToUint16Bytes is first with key of type uint64 and value of type pair(uint16,[]byte).
func (c *Cursor) LastUint64ToUint16Bytes() (uint64, uint16, []byte) {
	rawKey, rawValue := c.Last()
	key := bytesTouint64(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key, value1, value2
}

// NextUint64ToUint16Bytes is first with key of type uint64 and value of type pair(uint16,[]byte).
func (c *Cursor) NextUint64ToUint16Bytes() (uint64, uint16, []byte) {
	rawKey, rawValue := c.Next()
	key := bytesTouint64(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key, value1, value2
}

// PrevUint64ToUint16Bytes is first with key of type uint64 and value of type pair(uint16,[]byte).
func (c *Cursor) PrevUint64ToUint16Bytes() (uint64, uint16, []byte) {
	rawKey, rawValue := c.Prev()
	key := bytesTouint64(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key, value1, value2
}

// SeekUint64ToUint16Bytes is first with key of type uint64 and value of type pair(uint16,[]byte).
func (c *Cursor) SeekUint64ToUint16Bytes(key uint64) (uint64, uint16, []byte) {
	rawKey, rawValue := c.Seek(uint64Tobytes(key))
	key = bytesTouint64(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key, value1, value2
}

// ForEachUint64ToUint32Bytes is a for each with key of type uint64 and value of type pair(uint32,[]byte).
func (b *Bucket) ForEachUint64ToUint32Bytes(fn func(key uint64, value1 uint32, value2 []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := bytesTouint64(rawKey)
			value1, value2 := bytesTouint32bytes(rawValue)
			return fn(key, value1, value2)
		},
	)
}

// GetUint64ToUint32Bytes is a get with key of type uint64 and value of type pair(uint32,[]byte).
func (b *Bucket) GetUint64ToUint32Bytes(key uint64) (uint32, []byte) {
	return bytesTouint32bytes(b.Get(uint64Tobytes(key)))
}

// GetRequiredUint64ToUint32Bytes is a get with key of type uint64 and value of type pair(uint32,[]byte) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint64ToUint32Bytes(key uint64) (uint32, []byte, error) {
	value := b.Get(uint64Tobytes(key))
	if value == nil {
		return 0, nil, NewKeyNotFoundError(key)
	}
	value1, value2 := bytesTouint32bytes(value)
	return value1, value2, nil
}

// PutUint64ToUint32Bytes is a put with key of type uint64 and value of type pair(uint32,[]byte).
func (b *Bucket) PutUint64ToUint32Bytes(key uint64, value1 uint32, value2 []byte) error {
	return b.Put(uint64Tobytes(key), uint32bytesTobytes(value1, value2))
}

// FirstUint64ToUint32Bytes is first with key of type uint64 and value of type pair(uint32,[]byte).
func (c *Cursor) FirstUint64ToUint32Bytes() (uint64, uint32, []byte) {
	rawKey, rawValue := c.First()
	key := bytesTouint64(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key, value1, value2
}

// LastUint64ToUint32Bytes is first with key of type uint64 and value of type pair(uint32,[]byte).
func (c *Cursor) LastUint64ToUint32Bytes() (uint64, uint32, []byte) {
	rawKey, rawValue := c.Last()
	key := bytesTouint64(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key, value1, value2
}

// NextUint64ToUint32Bytes is first with key of type uint64 and value of type pair(uint32,[]byte).
func (c *Cursor) NextUint64ToUint32Bytes() (uint64, uint32, []byte) {
	rawKey, rawValue := c.Next()
	key := bytesTouint64(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key, value1, value2
}

// PrevUint64ToUint32Bytes is first with key of type uint64 and value of type pair(uint32,[]byte).
func (c *Cursor) PrevUint64ToUint32Bytes() (uint64, uint32, []byte) {
	rawKey, rawValue := c.Prev()
	key := bytesTouint64(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key, value1, value2
}

// SeekUint64ToUint32Bytes is first with key of type uint64 and value of type pair(uint32,[]byte).
func (c *Cursor) SeekUint64ToUint32Bytes(key uint64) (uint64, uint32, []byte) {
	rawKey, rawValue := c.Seek(uint64Tobytes(key))
	key = bytesTouint64(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key, value1, value2
}

// ForEachUint64ToUint64Bytes is a for each with key of type uint64 and value of type pair(uint64,[]byte).
func (b *Bucket) ForEachUint64ToUint64Bytes(fn func(key uint64, value1 uint64, value2 []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := bytesTouint64(rawKey)
			value1, value2 := bytesTouint64bytes(rawValue)
			return fn(key, value1, value2)
		},
	)
}

// GetUint64ToUint64Bytes is a get with key of type uint64 and value of type pair(uint64,[]byte).
func (b *Bucket) GetUint64ToUint64Bytes(key uint64) (uint64, []byte) {
	return bytesTouint64bytes(b.Get(uint64Tobytes(key)))
}

// GetRequiredUint64ToUint64Bytes is a get with key of type uint64 and value of type pair(uint64,[]byte) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint64ToUint64Bytes(key uint64) (uint64, []byte, error) {
	value := b.Get(uint64Tobytes(key))
	if value == nil {
		return 0, nil, NewKeyNotFoundError(key)
	}
	value1, value2 := bytesTouint64bytes(value)
	return value1, value2, nil
}

// PutUint64ToUint64Bytes is a put with key of type uint64 and value of type pair(uint64,[]byte).
func (b *Bucket) PutUint64ToUint64Bytes(key uint64, value1 uint64, value2 []byte) error {
	return b.Put(uint64Tobytes(key), uint64bytesTobytes(value1, value2))
}

// FirstUint64ToUint64Bytes is first with key of type uint64 and value of type pair(uint64,[]byte).
func (c *Cursor) FirstUint64ToUint64Bytes() (uint64, uint64, []byte) {
	rawKey, rawValue := c.First()
	key := bytesTouint64(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key, value1, value2
}

// LastUint64ToUint64Bytes is first with key of type uint64 and value of type pair(uint64,[]byte).
func (c *Cursor) LastUint64ToUint64Bytes() (uint64, uint64, []byte) {
	rawKey, rawValue := c.Last()
	key := bytesTouint64(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key, value1, value2
}

// NextUint64ToUint64Bytes is first with key of type uint64 and value of type pair(uint64,[]byte).
func (c *Cursor) NextUint64ToUint64Bytes() (uint64, uint64, []byte) {
	rawKey, rawValue := c.Next()
	key := bytesTouint64(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key, value1, value2
}

// PrevUint64ToUint64Bytes is first with key of type uint64 and value of type pair(uint64,[]byte).
func (c *Cursor) PrevUint64ToUint64Bytes() (uint64, uint64, []byte) {
	rawKey, rawValue := c.Prev()
	key := bytesTouint64(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key, value1, value2
}

// SeekUint64ToUint64Bytes is first with key of type uint64 and value of type pair(uint64,[]byte).
func (c *Cursor) SeekUint64ToUint64Bytes(key uint64) (uint64, uint64, []byte) {
	rawKey, rawValue := c.Seek(uint64Tobytes(key))
	key = bytesTouint64(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key, value1, value2
}

// ForEachUint64ToBytesUint16 is a for each with key of type uint64 and value of type pair([]byte,uint16).
func (b *Bucket) ForEachUint64ToBytesUint16(fn func(key uint64, value1 []byte, value2 uint16) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := bytesTouint64(rawKey)
			value1, value2 := bytesTobytesuint16(rawValue)
			return fn(key, value1, value2)
		},
	)
}

// GetUint64ToBytesUint16 is a get with key of type uint64 and value of type pair([]byte,uint16).
func (b *Bucket) GetUint64ToBytesUint16(key uint64) ([]byte, uint16) {
	return bytesTobytesuint16(b.Get(uint64Tobytes(key)))
}

// GetRequiredUint64ToBytesUint16 is a get with key of type uint64 and value of type pair([]byte,uint16) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint64ToBytesUint16(key uint64) ([]byte, uint16, error) {
	value := b.Get(uint64Tobytes(key))
	if value == nil {
		return nil, 0, NewKeyNotFoundError(key)
	}
	value1, value2 := bytesTobytesuint16(value)
	return value1, value2, nil
}

// PutUint64ToBytesUint16 is a put with key of type uint64 and value of type pair([]byte,uint16).
func (b *Bucket) PutUint64ToBytesUint16(key uint64, value1 []byte, value2 uint16) error {
	return b.Put(uint64Tobytes(key), bytesuint16Tobytes(value1, value2))
}

// FirstUint64ToBytesUint16 is first with key of type uint64 and value of type pair([]byte,uint16).
func (c *Cursor) FirstUint64ToBytesUint16() (uint64, []byte, uint16) {
	rawKey, rawValue := c.First()
	key := bytesTouint64(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key, value1, value2
}

// LastUint64ToBytesUint16 is first with key of type uint64 and value of type pair([]byte,uint16).
func (c *Cursor) LastUint64ToBytesUint16() (uint64, []byte, uint16) {
	rawKey, rawValue := c.Last()
	key := bytesTouint64(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key, value1, value2
}

// NextUint64ToBytesUint16 is first with key of type uint64 and value of type pair([]byte,uint16).
func (c *Cursor) NextUint64ToBytesUint16() (uint64, []byte, uint16) {
	rawKey, rawValue := c.Next()
	key := bytesTouint64(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key, value1, value2
}

// PrevUint64ToBytesUint16 is first with key of type uint64 and value of type pair([]byte,uint16).
func (c *Cursor) PrevUint64ToBytesUint16() (uint64, []byte, uint16) {
	rawKey, rawValue := c.Prev()
	key := bytesTouint64(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key, value1, value2
}

// SeekUint64ToBytesUint16 is first with key of type uint64 and value of type pair([]byte,uint16).
func (c *Cursor) SeekUint64ToBytesUint16(key uint64) (uint64, []byte, uint16) {
	rawKey, rawValue := c.Seek(uint64Tobytes(key))
	key = bytesTouint64(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key, value1, value2
}

// ForEachUint64ToBytesUint32 is a for each with key of type uint64 and value of type pair([]byte,uint32).
func (b *Bucket) ForEachUint64ToBytesUint32(fn func(key uint64, value1 []byte, value2 uint32) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := bytesTouint64(rawKey)
			value1, value2 := bytesTobytesuint32(rawValue)
			return fn(key, value1, value2)
		},
	)
}

// GetUint64ToBytesUint32 is a get with key of type uint64 and value of type pair([]byte,uint32).
func (b *Bucket) GetUint64ToBytesUint32(key uint64) ([]byte, uint32) {
	return bytesTobytesuint32(b.Get(uint64Tobytes(key)))
}

// GetRequiredUint64ToBytesUint32 is a get with key of type uint64 and value of type pair([]byte,uint32) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint64ToBytesUint32(key uint64) ([]byte, uint32, error) {
	value := b.Get(uint64Tobytes(key))
	if value == nil {
		return nil, 0, NewKeyNotFoundError(key)
	}
	value1, value2 := bytesTobytesuint32(value)
	return value1, value2, nil
}

// PutUint64ToBytesUint32 is a put with key of type uint64 and value of type pair([]byte,uint32).
func (b *Bucket) PutUint64ToBytesUint32(key uint64, value1 []byte, value2 uint32) error {
	return b.Put(uint64Tobytes(key), bytesuint32Tobytes(value1, value2))
}

// FirstUint64ToBytesUint32 is first with key of type uint64 and value of type pair([]byte,uint32).
func (c *Cursor) FirstUint64ToBytesUint32() (uint64, []byte, uint32) {
	rawKey, rawValue := c.First()
	key := bytesTouint64(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key, value1, value2
}

// LastUint64ToBytesUint32 is first with key of type uint64 and value of type pair([]byte,uint32).
func (c *Cursor) LastUint64ToBytesUint32() (uint64, []byte, uint32) {
	rawKey, rawValue := c.Last()
	key := bytesTouint64(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key, value1, value2
}

// NextUint64ToBytesUint32 is first with key of type uint64 and value of type pair([]byte,uint32).
func (c *Cursor) NextUint64ToBytesUint32() (uint64, []byte, uint32) {
	rawKey, rawValue := c.Next()
	key := bytesTouint64(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key, value1, value2
}

// PrevUint64ToBytesUint32 is first with key of type uint64 and value of type pair([]byte,uint32).
func (c *Cursor) PrevUint64ToBytesUint32() (uint64, []byte, uint32) {
	rawKey, rawValue := c.Prev()
	key := bytesTouint64(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key, value1, value2
}

// SeekUint64ToBytesUint32 is first with key of type uint64 and value of type pair([]byte,uint32).
func (c *Cursor) SeekUint64ToBytesUint32(key uint64) (uint64, []byte, uint32) {
	rawKey, rawValue := c.Seek(uint64Tobytes(key))
	key = bytesTouint64(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key, value1, value2
}

// ForEachUint64ToBytesUint64 is a for each with key of type uint64 and value of type pair([]byte,uint64).
func (b *Bucket) ForEachUint64ToBytesUint64(fn func(key uint64, value1 []byte, value2 uint64) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key := bytesTouint64(rawKey)
			value1, value2 := bytesTobytesuint64(rawValue)
			return fn(key, value1, value2)
		},
	)
}

// GetUint64ToBytesUint64 is a get with key of type uint64 and value of type pair([]byte,uint64).
func (b *Bucket) GetUint64ToBytesUint64(key uint64) ([]byte, uint64) {
	return bytesTobytesuint64(b.Get(uint64Tobytes(key)))
}

// GetRequiredUint64ToBytesUint64 is a get with key of type uint64 and value of type pair([]byte,uint64) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint64ToBytesUint64(key uint64) ([]byte, uint64, error) {
	value := b.Get(uint64Tobytes(key))
	if value == nil {
		return nil, 0, NewKeyNotFoundError(key)
	}
	value1, value2 := bytesTobytesuint64(value)
	return value1, value2, nil
}

// PutUint64ToBytesUint64 is a put with key of type uint64 and value of type pair([]byte,uint64).
func (b *Bucket) PutUint64ToBytesUint64(key uint64, value1 []byte, value2 uint64) error {
	return b.Put(uint64Tobytes(key), bytesuint64Tobytes(value1, value2))
}

// FirstUint64ToBytesUint64 is first with key of type uint64 and value of type pair([]byte,uint64).
func (c *Cursor) FirstUint64ToBytesUint64() (uint64, []byte, uint64) {
	rawKey, rawValue := c.First()
	key := bytesTouint64(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key, value1, value2
}

// LastUint64ToBytesUint64 is first with key of type uint64 and value of type pair([]byte,uint64).
func (c *Cursor) LastUint64ToBytesUint64() (uint64, []byte, uint64) {
	rawKey, rawValue := c.Last()
	key := bytesTouint64(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key, value1, value2
}

// NextUint64ToBytesUint64 is first with key of type uint64 and value of type pair([]byte,uint64).
func (c *Cursor) NextUint64ToBytesUint64() (uint64, []byte, uint64) {
	rawKey, rawValue := c.Next()
	key := bytesTouint64(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key, value1, value2
}

// PrevUint64ToBytesUint64 is first with key of type uint64 and value of type pair([]byte,uint64).
func (c *Cursor) PrevUint64ToBytesUint64() (uint64, []byte, uint64) {
	rawKey, rawValue := c.Prev()
	key := bytesTouint64(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key, value1, value2
}

// SeekUint64ToBytesUint64 is first with key of type uint64 and value of type pair([]byte,uint64).
func (c *Cursor) SeekUint64ToBytesUint64(key uint64) (uint64, []byte, uint64) {
	rawKey, rawValue := c.Seek(uint64Tobytes(key))
	key = bytesTouint64(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key, value1, value2
}

// DeleteUint64 is a delete with key of type uint64.
func (b *Bucket) DeleteUint64(key uint64) error {
	return b.Delete(uint64Tobytes(key))
}

// ForEachUint16Uint16ToBytes is a for each with key of type pair(uint16,uint16) and value of type []byte.
func (b *Bucket) ForEachUint16Uint16ToBytes(fn func(key1 uint16, key2 uint16, value []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint16uint16(rawKey)
			value := (rawValue)
			return fn(key1, key2, value)
		},
	)
}

// GetUint16Uint16ToBytes is a get with key of type pair(uint16,uint16) and value of type []byte.
func (b *Bucket) GetUint16Uint16ToBytes(key1 uint16, key2 uint16) []byte {
	return (b.Get(uint16uint16Tobytes(key1, key2)))
}

// GetRequiredUint16Uint16ToBytes is a get with key of type pair(uint16,uint16) and value of type []byte that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint16Uint16ToBytes(key1 uint16, key2 uint16) ([]byte, error) {
	value := b.Get(uint16uint16Tobytes(key1, key2))
	if value == nil {
		return nil, NewKeyNotFoundError(key1, key2)
	}
	return (value), nil
}

// PutUint16Uint16ToBytes is a put with key of type pair(uint16,uint16) and value of type []byte.
func (b *Bucket) PutUint16Uint16ToBytes(key1 uint16, key2 uint16, value []byte) error {
	return b.Put(uint16uint16Tobytes(key1, key2), (value))
}

// FirstUint16Uint16ToBytes is first with key of type pair(uint16,uint16) and value of type []byte.
func (c *Cursor) FirstUint16Uint16ToBytes() (uint16, uint16, []byte) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint16uint16(rawKey)
	value := (rawValue)
	return key1, key2, value
}

// LastUint16Uint16ToBytes is first with key of type pair(uint16,uint16) and value of type []byte.
func (c *Cursor) LastUint16Uint16ToBytes() (uint16, uint16, []byte) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint16uint16(rawKey)
	value := (rawValue)
	return key1, key2, value
}

// NextUint16Uint16ToBytes is first with key of type pair(uint16,uint16) and value of type []byte.
func (c *Cursor) NextUint16Uint16ToBytes() (uint16, uint16, []byte) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint16uint16(rawKey)
	value := (rawValue)
	return key1, key2, value
}

// PrevUint16Uint16ToBytes is first with key of type pair(uint16,uint16) and value of type []byte.
func (c *Cursor) PrevUint16Uint16ToBytes() (uint16, uint16, []byte) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint16uint16(rawKey)
	value := (rawValue)
	return key1, key2, value
}

// SeekUint16Uint16ToBytes is first with key of type pair(uint16,uint16) and value of type []byte.
func (c *Cursor) SeekUint16Uint16ToBytes(key1 uint16, key2 uint16) (uint16, uint16, []byte) {
	rawKey, rawValue := c.Seek(uint16uint16Tobytes(key1, key2))
	key1, key2 = bytesTouint16uint16(rawKey)
	value := (rawValue)
	return key1, key2, value
}

// ForEachUint16Uint16ToString is a for each with key of type pair(uint16,uint16) and value of type string.
func (b *Bucket) ForEachUint16Uint16ToString(fn func(key1 uint16, key2 uint16, value string) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint16uint16(rawKey)
			value := string(rawValue)
			return fn(key1, key2, value)
		},
	)
}

// GetUint16Uint16ToString is a get with key of type pair(uint16,uint16) and value of type string.
func (b *Bucket) GetUint16Uint16ToString(key1 uint16, key2 uint16) string {
	return string(b.Get(uint16uint16Tobytes(key1, key2)))
}

// GetRequiredUint16Uint16ToString is a get with key of type pair(uint16,uint16) and value of type string that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint16Uint16ToString(key1 uint16, key2 uint16) (string, error) {
	value := b.Get(uint16uint16Tobytes(key1, key2))
	if value == nil {
		return "", NewKeyNotFoundError(key1, key2)
	}
	return string(value), nil
}

// PutUint16Uint16ToString is a put with key of type pair(uint16,uint16) and value of type string.
func (b *Bucket) PutUint16Uint16ToString(key1 uint16, key2 uint16, value string) error {
	return b.Put(uint16uint16Tobytes(key1, key2), []byte(value))
}

// FirstUint16Uint16ToString is first with key of type pair(uint16,uint16) and value of type string.
func (c *Cursor) FirstUint16Uint16ToString() (uint16, uint16, string) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint16uint16(rawKey)
	value := string(rawValue)
	return key1, key2, value
}

// LastUint16Uint16ToString is first with key of type pair(uint16,uint16) and value of type string.
func (c *Cursor) LastUint16Uint16ToString() (uint16, uint16, string) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint16uint16(rawKey)
	value := string(rawValue)
	return key1, key2, value
}

// NextUint16Uint16ToString is first with key of type pair(uint16,uint16) and value of type string.
func (c *Cursor) NextUint16Uint16ToString() (uint16, uint16, string) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint16uint16(rawKey)
	value := string(rawValue)
	return key1, key2, value
}

// PrevUint16Uint16ToString is first with key of type pair(uint16,uint16) and value of type string.
func (c *Cursor) PrevUint16Uint16ToString() (uint16, uint16, string) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint16uint16(rawKey)
	value := string(rawValue)
	return key1, key2, value
}

// SeekUint16Uint16ToString is first with key of type pair(uint16,uint16) and value of type string.
func (c *Cursor) SeekUint16Uint16ToString(key1 uint16, key2 uint16) (uint16, uint16, string) {
	rawKey, rawValue := c.Seek(uint16uint16Tobytes(key1, key2))
	key1, key2 = bytesTouint16uint16(rawKey)
	value := string(rawValue)
	return key1, key2, value
}

// ForEachUint16Uint16ToUint16 is a for each with key of type pair(uint16,uint16) and value of type uint16.
func (b *Bucket) ForEachUint16Uint16ToUint16(fn func(key1 uint16, key2 uint16, value uint16) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint16uint16(rawKey)
			value := bytesTouint16(rawValue)
			return fn(key1, key2, value)
		},
	)
}

// GetUint16Uint16ToUint16 is a get with key of type pair(uint16,uint16) and value of type uint16.
func (b *Bucket) GetUint16Uint16ToUint16(key1 uint16, key2 uint16) uint16 {
	return bytesTouint16(b.Get(uint16uint16Tobytes(key1, key2)))
}

// GetRequiredUint16Uint16ToUint16 is a get with key of type pair(uint16,uint16) and value of type uint16 that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint16Uint16ToUint16(key1 uint16, key2 uint16) (uint16, error) {
	value := b.Get(uint16uint16Tobytes(key1, key2))
	if value == nil {
		return 0, NewKeyNotFoundError(key1, key2)
	}
	return bytesTouint16(value), nil
}

// PutUint16Uint16ToUint16 is a put with key of type pair(uint16,uint16) and value of type uint16.
func (b *Bucket) PutUint16Uint16ToUint16(key1 uint16, key2 uint16, value uint16) error {
	return b.Put(uint16uint16Tobytes(key1, key2), uint16Tobytes(value))
}

// FirstUint16Uint16ToUint16 is first with key of type pair(uint16,uint16) and value of type uint16.
func (c *Cursor) FirstUint16Uint16ToUint16() (uint16, uint16, uint16) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint16uint16(rawKey)
	value := bytesTouint16(rawValue)
	return key1, key2, value
}

// LastUint16Uint16ToUint16 is first with key of type pair(uint16,uint16) and value of type uint16.
func (c *Cursor) LastUint16Uint16ToUint16() (uint16, uint16, uint16) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint16uint16(rawKey)
	value := bytesTouint16(rawValue)
	return key1, key2, value
}

// NextUint16Uint16ToUint16 is first with key of type pair(uint16,uint16) and value of type uint16.
func (c *Cursor) NextUint16Uint16ToUint16() (uint16, uint16, uint16) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint16uint16(rawKey)
	value := bytesTouint16(rawValue)
	return key1, key2, value
}

// PrevUint16Uint16ToUint16 is first with key of type pair(uint16,uint16) and value of type uint16.
func (c *Cursor) PrevUint16Uint16ToUint16() (uint16, uint16, uint16) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint16uint16(rawKey)
	value := bytesTouint16(rawValue)
	return key1, key2, value
}

// SeekUint16Uint16ToUint16 is first with key of type pair(uint16,uint16) and value of type uint16.
func (c *Cursor) SeekUint16Uint16ToUint16(key1 uint16, key2 uint16) (uint16, uint16, uint16) {
	rawKey, rawValue := c.Seek(uint16uint16Tobytes(key1, key2))
	key1, key2 = bytesTouint16uint16(rawKey)
	value := bytesTouint16(rawValue)
	return key1, key2, value
}

// ForEachUint16Uint16ToUint32 is a for each with key of type pair(uint16,uint16) and value of type uint32.
func (b *Bucket) ForEachUint16Uint16ToUint32(fn func(key1 uint16, key2 uint16, value uint32) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint16uint16(rawKey)
			value := bytesTouint32(rawValue)
			return fn(key1, key2, value)
		},
	)
}

// GetUint16Uint16ToUint32 is a get with key of type pair(uint16,uint16) and value of type uint32.
func (b *Bucket) GetUint16Uint16ToUint32(key1 uint16, key2 uint16) uint32 {
	return bytesTouint32(b.Get(uint16uint16Tobytes(key1, key2)))
}

// GetRequiredUint16Uint16ToUint32 is a get with key of type pair(uint16,uint16) and value of type uint32 that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint16Uint16ToUint32(key1 uint16, key2 uint16) (uint32, error) {
	value := b.Get(uint16uint16Tobytes(key1, key2))
	if value == nil {
		return 0, NewKeyNotFoundError(key1, key2)
	}
	return bytesTouint32(value), nil
}

// PutUint16Uint16ToUint32 is a put with key of type pair(uint16,uint16) and value of type uint32.
func (b *Bucket) PutUint16Uint16ToUint32(key1 uint16, key2 uint16, value uint32) error {
	return b.Put(uint16uint16Tobytes(key1, key2), uint32Tobytes(value))
}

// FirstUint16Uint16ToUint32 is first with key of type pair(uint16,uint16) and value of type uint32.
func (c *Cursor) FirstUint16Uint16ToUint32() (uint16, uint16, uint32) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint16uint16(rawKey)
	value := bytesTouint32(rawValue)
	return key1, key2, value
}

// LastUint16Uint16ToUint32 is first with key of type pair(uint16,uint16) and value of type uint32.
func (c *Cursor) LastUint16Uint16ToUint32() (uint16, uint16, uint32) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint16uint16(rawKey)
	value := bytesTouint32(rawValue)
	return key1, key2, value
}

// NextUint16Uint16ToUint32 is first with key of type pair(uint16,uint16) and value of type uint32.
func (c *Cursor) NextUint16Uint16ToUint32() (uint16, uint16, uint32) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint16uint16(rawKey)
	value := bytesTouint32(rawValue)
	return key1, key2, value
}

// PrevUint16Uint16ToUint32 is first with key of type pair(uint16,uint16) and value of type uint32.
func (c *Cursor) PrevUint16Uint16ToUint32() (uint16, uint16, uint32) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint16uint16(rawKey)
	value := bytesTouint32(rawValue)
	return key1, key2, value
}

// SeekUint16Uint16ToUint32 is first with key of type pair(uint16,uint16) and value of type uint32.
func (c *Cursor) SeekUint16Uint16ToUint32(key1 uint16, key2 uint16) (uint16, uint16, uint32) {
	rawKey, rawValue := c.Seek(uint16uint16Tobytes(key1, key2))
	key1, key2 = bytesTouint16uint16(rawKey)
	value := bytesTouint32(rawValue)
	return key1, key2, value
}

// ForEachUint16Uint16ToUint64 is a for each with key of type pair(uint16,uint16) and value of type uint64.
func (b *Bucket) ForEachUint16Uint16ToUint64(fn func(key1 uint16, key2 uint16, value uint64) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint16uint16(rawKey)
			value := bytesTouint64(rawValue)
			return fn(key1, key2, value)
		},
	)
}

// GetUint16Uint16ToUint64 is a get with key of type pair(uint16,uint16) and value of type uint64.
func (b *Bucket) GetUint16Uint16ToUint64(key1 uint16, key2 uint16) uint64 {
	return bytesTouint64(b.Get(uint16uint16Tobytes(key1, key2)))
}

// GetRequiredUint16Uint16ToUint64 is a get with key of type pair(uint16,uint16) and value of type uint64 that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint16Uint16ToUint64(key1 uint16, key2 uint16) (uint64, error) {
	value := b.Get(uint16uint16Tobytes(key1, key2))
	if value == nil {
		return 0, NewKeyNotFoundError(key1, key2)
	}
	return bytesTouint64(value), nil
}

// PutUint16Uint16ToUint64 is a put with key of type pair(uint16,uint16) and value of type uint64.
func (b *Bucket) PutUint16Uint16ToUint64(key1 uint16, key2 uint16, value uint64) error {
	return b.Put(uint16uint16Tobytes(key1, key2), uint64Tobytes(value))
}

// FirstUint16Uint16ToUint64 is first with key of type pair(uint16,uint16) and value of type uint64.
func (c *Cursor) FirstUint16Uint16ToUint64() (uint16, uint16, uint64) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint16uint16(rawKey)
	value := bytesTouint64(rawValue)
	return key1, key2, value
}

// LastUint16Uint16ToUint64 is first with key of type pair(uint16,uint16) and value of type uint64.
func (c *Cursor) LastUint16Uint16ToUint64() (uint16, uint16, uint64) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint16uint16(rawKey)
	value := bytesTouint64(rawValue)
	return key1, key2, value
}

// NextUint16Uint16ToUint64 is first with key of type pair(uint16,uint16) and value of type uint64.
func (c *Cursor) NextUint16Uint16ToUint64() (uint16, uint16, uint64) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint16uint16(rawKey)
	value := bytesTouint64(rawValue)
	return key1, key2, value
}

// PrevUint16Uint16ToUint64 is first with key of type pair(uint16,uint16) and value of type uint64.
func (c *Cursor) PrevUint16Uint16ToUint64() (uint16, uint16, uint64) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint16uint16(rawKey)
	value := bytesTouint64(rawValue)
	return key1, key2, value
}

// SeekUint16Uint16ToUint64 is first with key of type pair(uint16,uint16) and value of type uint64.
func (c *Cursor) SeekUint16Uint16ToUint64(key1 uint16, key2 uint16) (uint16, uint16, uint64) {
	rawKey, rawValue := c.Seek(uint16uint16Tobytes(key1, key2))
	key1, key2 = bytesTouint16uint16(rawKey)
	value := bytesTouint64(rawValue)
	return key1, key2, value
}

// ForEachUint16Uint16ToUint16Uint16 is a for each with key of type pair(uint16,uint16) and value of type pair(uint16,uint16).
func (b *Bucket) ForEachUint16Uint16ToUint16Uint16(fn func(key1 uint16, key2 uint16, value1 uint16, value2 uint16) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint16uint16(rawKey)
			value1, value2 := bytesTouint16uint16(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetUint16Uint16ToUint16Uint16 is a get with key of type pair(uint16,uint16) and value of type pair(uint16,uint16).
func (b *Bucket) GetUint16Uint16ToUint16Uint16(key1 uint16, key2 uint16) (uint16, uint16) {
	return bytesTouint16uint16(b.Get(uint16uint16Tobytes(key1, key2)))
}

// GetRequiredUint16Uint16ToUint16Uint16 is a get with key of type pair(uint16,uint16) and value of type pair(uint16,uint16) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint16Uint16ToUint16Uint16(key1 uint16, key2 uint16) (uint16, uint16, error) {
	value := b.Get(uint16uint16Tobytes(key1, key2))
	if value == nil {
		return 0, 0, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTouint16uint16(value)
	return value1, value2, nil
}

// PutUint16Uint16ToUint16Uint16 is a put with key of type pair(uint16,uint16) and value of type pair(uint16,uint16).
func (b *Bucket) PutUint16Uint16ToUint16Uint16(key1 uint16, key2 uint16, value1 uint16, value2 uint16) error {
	return b.Put(uint16uint16Tobytes(key1, key2), uint16uint16Tobytes(value1, value2))
}

// FirstUint16Uint16ToUint16Uint16 is first with key of type pair(uint16,uint16) and value of type pair(uint16,uint16).
func (c *Cursor) FirstUint16Uint16ToUint16Uint16() (uint16, uint16, uint16, uint16) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint16uint16(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key1, key2, value1, value2
}

// LastUint16Uint16ToUint16Uint16 is first with key of type pair(uint16,uint16) and value of type pair(uint16,uint16).
func (c *Cursor) LastUint16Uint16ToUint16Uint16() (uint16, uint16, uint16, uint16) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint16uint16(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key1, key2, value1, value2
}

// NextUint16Uint16ToUint16Uint16 is first with key of type pair(uint16,uint16) and value of type pair(uint16,uint16).
func (c *Cursor) NextUint16Uint16ToUint16Uint16() (uint16, uint16, uint16, uint16) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint16uint16(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key1, key2, value1, value2
}

// PrevUint16Uint16ToUint16Uint16 is first with key of type pair(uint16,uint16) and value of type pair(uint16,uint16).
func (c *Cursor) PrevUint16Uint16ToUint16Uint16() (uint16, uint16, uint16, uint16) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint16uint16(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key1, key2, value1, value2
}

// SeekUint16Uint16ToUint16Uint16 is first with key of type pair(uint16,uint16) and value of type pair(uint16,uint16).
func (c *Cursor) SeekUint16Uint16ToUint16Uint16(key1 uint16, key2 uint16) (uint16, uint16, uint16, uint16) {
	rawKey, rawValue := c.Seek(uint16uint16Tobytes(key1, key2))
	key1, key2 = bytesTouint16uint16(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key1, key2, value1, value2
}

// ForEachUint16Uint16ToUint32Uint32 is a for each with key of type pair(uint16,uint16) and value of type pair(uint32,uint32).
func (b *Bucket) ForEachUint16Uint16ToUint32Uint32(fn func(key1 uint16, key2 uint16, value1 uint32, value2 uint32) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint16uint16(rawKey)
			value1, value2 := bytesTouint32uint32(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetUint16Uint16ToUint32Uint32 is a get with key of type pair(uint16,uint16) and value of type pair(uint32,uint32).
func (b *Bucket) GetUint16Uint16ToUint32Uint32(key1 uint16, key2 uint16) (uint32, uint32) {
	return bytesTouint32uint32(b.Get(uint16uint16Tobytes(key1, key2)))
}

// GetRequiredUint16Uint16ToUint32Uint32 is a get with key of type pair(uint16,uint16) and value of type pair(uint32,uint32) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint16Uint16ToUint32Uint32(key1 uint16, key2 uint16) (uint32, uint32, error) {
	value := b.Get(uint16uint16Tobytes(key1, key2))
	if value == nil {
		return 0, 0, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTouint32uint32(value)
	return value1, value2, nil
}

// PutUint16Uint16ToUint32Uint32 is a put with key of type pair(uint16,uint16) and value of type pair(uint32,uint32).
func (b *Bucket) PutUint16Uint16ToUint32Uint32(key1 uint16, key2 uint16, value1 uint32, value2 uint32) error {
	return b.Put(uint16uint16Tobytes(key1, key2), uint32uint32Tobytes(value1, value2))
}

// FirstUint16Uint16ToUint32Uint32 is first with key of type pair(uint16,uint16) and value of type pair(uint32,uint32).
func (c *Cursor) FirstUint16Uint16ToUint32Uint32() (uint16, uint16, uint32, uint32) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint16uint16(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key1, key2, value1, value2
}

// LastUint16Uint16ToUint32Uint32 is first with key of type pair(uint16,uint16) and value of type pair(uint32,uint32).
func (c *Cursor) LastUint16Uint16ToUint32Uint32() (uint16, uint16, uint32, uint32) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint16uint16(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key1, key2, value1, value2
}

// NextUint16Uint16ToUint32Uint32 is first with key of type pair(uint16,uint16) and value of type pair(uint32,uint32).
func (c *Cursor) NextUint16Uint16ToUint32Uint32() (uint16, uint16, uint32, uint32) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint16uint16(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key1, key2, value1, value2
}

// PrevUint16Uint16ToUint32Uint32 is first with key of type pair(uint16,uint16) and value of type pair(uint32,uint32).
func (c *Cursor) PrevUint16Uint16ToUint32Uint32() (uint16, uint16, uint32, uint32) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint16uint16(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key1, key2, value1, value2
}

// SeekUint16Uint16ToUint32Uint32 is first with key of type pair(uint16,uint16) and value of type pair(uint32,uint32).
func (c *Cursor) SeekUint16Uint16ToUint32Uint32(key1 uint16, key2 uint16) (uint16, uint16, uint32, uint32) {
	rawKey, rawValue := c.Seek(uint16uint16Tobytes(key1, key2))
	key1, key2 = bytesTouint16uint16(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key1, key2, value1, value2
}

// ForEachUint16Uint16ToUint64Uint64 is a for each with key of type pair(uint16,uint16) and value of type pair(uint64,uint64).
func (b *Bucket) ForEachUint16Uint16ToUint64Uint64(fn func(key1 uint16, key2 uint16, value1 uint64, value2 uint64) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint16uint16(rawKey)
			value1, value2 := bytesTouint64uint64(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetUint16Uint16ToUint64Uint64 is a get with key of type pair(uint16,uint16) and value of type pair(uint64,uint64).
func (b *Bucket) GetUint16Uint16ToUint64Uint64(key1 uint16, key2 uint16) (uint64, uint64) {
	return bytesTouint64uint64(b.Get(uint16uint16Tobytes(key1, key2)))
}

// GetRequiredUint16Uint16ToUint64Uint64 is a get with key of type pair(uint16,uint16) and value of type pair(uint64,uint64) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint16Uint16ToUint64Uint64(key1 uint16, key2 uint16) (uint64, uint64, error) {
	value := b.Get(uint16uint16Tobytes(key1, key2))
	if value == nil {
		return 0, 0, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTouint64uint64(value)
	return value1, value2, nil
}

// PutUint16Uint16ToUint64Uint64 is a put with key of type pair(uint16,uint16) and value of type pair(uint64,uint64).
func (b *Bucket) PutUint16Uint16ToUint64Uint64(key1 uint16, key2 uint16, value1 uint64, value2 uint64) error {
	return b.Put(uint16uint16Tobytes(key1, key2), uint64uint64Tobytes(value1, value2))
}

// FirstUint16Uint16ToUint64Uint64 is first with key of type pair(uint16,uint16) and value of type pair(uint64,uint64).
func (c *Cursor) FirstUint16Uint16ToUint64Uint64() (uint16, uint16, uint64, uint64) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint16uint16(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key1, key2, value1, value2
}

// LastUint16Uint16ToUint64Uint64 is first with key of type pair(uint16,uint16) and value of type pair(uint64,uint64).
func (c *Cursor) LastUint16Uint16ToUint64Uint64() (uint16, uint16, uint64, uint64) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint16uint16(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key1, key2, value1, value2
}

// NextUint16Uint16ToUint64Uint64 is first with key of type pair(uint16,uint16) and value of type pair(uint64,uint64).
func (c *Cursor) NextUint16Uint16ToUint64Uint64() (uint16, uint16, uint64, uint64) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint16uint16(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key1, key2, value1, value2
}

// PrevUint16Uint16ToUint64Uint64 is first with key of type pair(uint16,uint16) and value of type pair(uint64,uint64).
func (c *Cursor) PrevUint16Uint16ToUint64Uint64() (uint16, uint16, uint64, uint64) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint16uint16(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key1, key2, value1, value2
}

// SeekUint16Uint16ToUint64Uint64 is first with key of type pair(uint16,uint16) and value of type pair(uint64,uint64).
func (c *Cursor) SeekUint16Uint16ToUint64Uint64(key1 uint16, key2 uint16) (uint16, uint16, uint64, uint64) {
	rawKey, rawValue := c.Seek(uint16uint16Tobytes(key1, key2))
	key1, key2 = bytesTouint16uint16(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key1, key2, value1, value2
}

// ForEachUint16Uint16ToUint16Bytes is a for each with key of type pair(uint16,uint16) and value of type pair(uint16,[]byte).
func (b *Bucket) ForEachUint16Uint16ToUint16Bytes(fn func(key1 uint16, key2 uint16, value1 uint16, value2 []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint16uint16(rawKey)
			value1, value2 := bytesTouint16bytes(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetUint16Uint16ToUint16Bytes is a get with key of type pair(uint16,uint16) and value of type pair(uint16,[]byte).
func (b *Bucket) GetUint16Uint16ToUint16Bytes(key1 uint16, key2 uint16) (uint16, []byte) {
	return bytesTouint16bytes(b.Get(uint16uint16Tobytes(key1, key2)))
}

// GetRequiredUint16Uint16ToUint16Bytes is a get with key of type pair(uint16,uint16) and value of type pair(uint16,[]byte) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint16Uint16ToUint16Bytes(key1 uint16, key2 uint16) (uint16, []byte, error) {
	value := b.Get(uint16uint16Tobytes(key1, key2))
	if value == nil {
		return 0, nil, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTouint16bytes(value)
	return value1, value2, nil
}

// PutUint16Uint16ToUint16Bytes is a put with key of type pair(uint16,uint16) and value of type pair(uint16,[]byte).
func (b *Bucket) PutUint16Uint16ToUint16Bytes(key1 uint16, key2 uint16, value1 uint16, value2 []byte) error {
	return b.Put(uint16uint16Tobytes(key1, key2), uint16bytesTobytes(value1, value2))
}

// FirstUint16Uint16ToUint16Bytes is first with key of type pair(uint16,uint16) and value of type pair(uint16,[]byte).
func (c *Cursor) FirstUint16Uint16ToUint16Bytes() (uint16, uint16, uint16, []byte) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint16uint16(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key1, key2, value1, value2
}

// LastUint16Uint16ToUint16Bytes is first with key of type pair(uint16,uint16) and value of type pair(uint16,[]byte).
func (c *Cursor) LastUint16Uint16ToUint16Bytes() (uint16, uint16, uint16, []byte) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint16uint16(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key1, key2, value1, value2
}

// NextUint16Uint16ToUint16Bytes is first with key of type pair(uint16,uint16) and value of type pair(uint16,[]byte).
func (c *Cursor) NextUint16Uint16ToUint16Bytes() (uint16, uint16, uint16, []byte) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint16uint16(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key1, key2, value1, value2
}

// PrevUint16Uint16ToUint16Bytes is first with key of type pair(uint16,uint16) and value of type pair(uint16,[]byte).
func (c *Cursor) PrevUint16Uint16ToUint16Bytes() (uint16, uint16, uint16, []byte) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint16uint16(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key1, key2, value1, value2
}

// SeekUint16Uint16ToUint16Bytes is first with key of type pair(uint16,uint16) and value of type pair(uint16,[]byte).
func (c *Cursor) SeekUint16Uint16ToUint16Bytes(key1 uint16, key2 uint16) (uint16, uint16, uint16, []byte) {
	rawKey, rawValue := c.Seek(uint16uint16Tobytes(key1, key2))
	key1, key2 = bytesTouint16uint16(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key1, key2, value1, value2
}

// ForEachUint16Uint16ToUint32Bytes is a for each with key of type pair(uint16,uint16) and value of type pair(uint32,[]byte).
func (b *Bucket) ForEachUint16Uint16ToUint32Bytes(fn func(key1 uint16, key2 uint16, value1 uint32, value2 []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint16uint16(rawKey)
			value1, value2 := bytesTouint32bytes(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetUint16Uint16ToUint32Bytes is a get with key of type pair(uint16,uint16) and value of type pair(uint32,[]byte).
func (b *Bucket) GetUint16Uint16ToUint32Bytes(key1 uint16, key2 uint16) (uint32, []byte) {
	return bytesTouint32bytes(b.Get(uint16uint16Tobytes(key1, key2)))
}

// GetRequiredUint16Uint16ToUint32Bytes is a get with key of type pair(uint16,uint16) and value of type pair(uint32,[]byte) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint16Uint16ToUint32Bytes(key1 uint16, key2 uint16) (uint32, []byte, error) {
	value := b.Get(uint16uint16Tobytes(key1, key2))
	if value == nil {
		return 0, nil, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTouint32bytes(value)
	return value1, value2, nil
}

// PutUint16Uint16ToUint32Bytes is a put with key of type pair(uint16,uint16) and value of type pair(uint32,[]byte).
func (b *Bucket) PutUint16Uint16ToUint32Bytes(key1 uint16, key2 uint16, value1 uint32, value2 []byte) error {
	return b.Put(uint16uint16Tobytes(key1, key2), uint32bytesTobytes(value1, value2))
}

// FirstUint16Uint16ToUint32Bytes is first with key of type pair(uint16,uint16) and value of type pair(uint32,[]byte).
func (c *Cursor) FirstUint16Uint16ToUint32Bytes() (uint16, uint16, uint32, []byte) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint16uint16(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key1, key2, value1, value2
}

// LastUint16Uint16ToUint32Bytes is first with key of type pair(uint16,uint16) and value of type pair(uint32,[]byte).
func (c *Cursor) LastUint16Uint16ToUint32Bytes() (uint16, uint16, uint32, []byte) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint16uint16(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key1, key2, value1, value2
}

// NextUint16Uint16ToUint32Bytes is first with key of type pair(uint16,uint16) and value of type pair(uint32,[]byte).
func (c *Cursor) NextUint16Uint16ToUint32Bytes() (uint16, uint16, uint32, []byte) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint16uint16(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key1, key2, value1, value2
}

// PrevUint16Uint16ToUint32Bytes is first with key of type pair(uint16,uint16) and value of type pair(uint32,[]byte).
func (c *Cursor) PrevUint16Uint16ToUint32Bytes() (uint16, uint16, uint32, []byte) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint16uint16(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key1, key2, value1, value2
}

// SeekUint16Uint16ToUint32Bytes is first with key of type pair(uint16,uint16) and value of type pair(uint32,[]byte).
func (c *Cursor) SeekUint16Uint16ToUint32Bytes(key1 uint16, key2 uint16) (uint16, uint16, uint32, []byte) {
	rawKey, rawValue := c.Seek(uint16uint16Tobytes(key1, key2))
	key1, key2 = bytesTouint16uint16(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key1, key2, value1, value2
}

// ForEachUint16Uint16ToUint64Bytes is a for each with key of type pair(uint16,uint16) and value of type pair(uint64,[]byte).
func (b *Bucket) ForEachUint16Uint16ToUint64Bytes(fn func(key1 uint16, key2 uint16, value1 uint64, value2 []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint16uint16(rawKey)
			value1, value2 := bytesTouint64bytes(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetUint16Uint16ToUint64Bytes is a get with key of type pair(uint16,uint16) and value of type pair(uint64,[]byte).
func (b *Bucket) GetUint16Uint16ToUint64Bytes(key1 uint16, key2 uint16) (uint64, []byte) {
	return bytesTouint64bytes(b.Get(uint16uint16Tobytes(key1, key2)))
}

// GetRequiredUint16Uint16ToUint64Bytes is a get with key of type pair(uint16,uint16) and value of type pair(uint64,[]byte) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint16Uint16ToUint64Bytes(key1 uint16, key2 uint16) (uint64, []byte, error) {
	value := b.Get(uint16uint16Tobytes(key1, key2))
	if value == nil {
		return 0, nil, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTouint64bytes(value)
	return value1, value2, nil
}

// PutUint16Uint16ToUint64Bytes is a put with key of type pair(uint16,uint16) and value of type pair(uint64,[]byte).
func (b *Bucket) PutUint16Uint16ToUint64Bytes(key1 uint16, key2 uint16, value1 uint64, value2 []byte) error {
	return b.Put(uint16uint16Tobytes(key1, key2), uint64bytesTobytes(value1, value2))
}

// FirstUint16Uint16ToUint64Bytes is first with key of type pair(uint16,uint16) and value of type pair(uint64,[]byte).
func (c *Cursor) FirstUint16Uint16ToUint64Bytes() (uint16, uint16, uint64, []byte) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint16uint16(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key1, key2, value1, value2
}

// LastUint16Uint16ToUint64Bytes is first with key of type pair(uint16,uint16) and value of type pair(uint64,[]byte).
func (c *Cursor) LastUint16Uint16ToUint64Bytes() (uint16, uint16, uint64, []byte) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint16uint16(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key1, key2, value1, value2
}

// NextUint16Uint16ToUint64Bytes is first with key of type pair(uint16,uint16) and value of type pair(uint64,[]byte).
func (c *Cursor) NextUint16Uint16ToUint64Bytes() (uint16, uint16, uint64, []byte) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint16uint16(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key1, key2, value1, value2
}

// PrevUint16Uint16ToUint64Bytes is first with key of type pair(uint16,uint16) and value of type pair(uint64,[]byte).
func (c *Cursor) PrevUint16Uint16ToUint64Bytes() (uint16, uint16, uint64, []byte) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint16uint16(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key1, key2, value1, value2
}

// SeekUint16Uint16ToUint64Bytes is first with key of type pair(uint16,uint16) and value of type pair(uint64,[]byte).
func (c *Cursor) SeekUint16Uint16ToUint64Bytes(key1 uint16, key2 uint16) (uint16, uint16, uint64, []byte) {
	rawKey, rawValue := c.Seek(uint16uint16Tobytes(key1, key2))
	key1, key2 = bytesTouint16uint16(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key1, key2, value1, value2
}

// ForEachUint16Uint16ToBytesUint16 is a for each with key of type pair(uint16,uint16) and value of type pair([]byte,uint16).
func (b *Bucket) ForEachUint16Uint16ToBytesUint16(fn func(key1 uint16, key2 uint16, value1 []byte, value2 uint16) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint16uint16(rawKey)
			value1, value2 := bytesTobytesuint16(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetUint16Uint16ToBytesUint16 is a get with key of type pair(uint16,uint16) and value of type pair([]byte,uint16).
func (b *Bucket) GetUint16Uint16ToBytesUint16(key1 uint16, key2 uint16) ([]byte, uint16) {
	return bytesTobytesuint16(b.Get(uint16uint16Tobytes(key1, key2)))
}

// GetRequiredUint16Uint16ToBytesUint16 is a get with key of type pair(uint16,uint16) and value of type pair([]byte,uint16) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint16Uint16ToBytesUint16(key1 uint16, key2 uint16) ([]byte, uint16, error) {
	value := b.Get(uint16uint16Tobytes(key1, key2))
	if value == nil {
		return nil, 0, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTobytesuint16(value)
	return value1, value2, nil
}

// PutUint16Uint16ToBytesUint16 is a put with key of type pair(uint16,uint16) and value of type pair([]byte,uint16).
func (b *Bucket) PutUint16Uint16ToBytesUint16(key1 uint16, key2 uint16, value1 []byte, value2 uint16) error {
	return b.Put(uint16uint16Tobytes(key1, key2), bytesuint16Tobytes(value1, value2))
}

// FirstUint16Uint16ToBytesUint16 is first with key of type pair(uint16,uint16) and value of type pair([]byte,uint16).
func (c *Cursor) FirstUint16Uint16ToBytesUint16() (uint16, uint16, []byte, uint16) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint16uint16(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key1, key2, value1, value2
}

// LastUint16Uint16ToBytesUint16 is first with key of type pair(uint16,uint16) and value of type pair([]byte,uint16).
func (c *Cursor) LastUint16Uint16ToBytesUint16() (uint16, uint16, []byte, uint16) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint16uint16(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key1, key2, value1, value2
}

// NextUint16Uint16ToBytesUint16 is first with key of type pair(uint16,uint16) and value of type pair([]byte,uint16).
func (c *Cursor) NextUint16Uint16ToBytesUint16() (uint16, uint16, []byte, uint16) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint16uint16(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key1, key2, value1, value2
}

// PrevUint16Uint16ToBytesUint16 is first with key of type pair(uint16,uint16) and value of type pair([]byte,uint16).
func (c *Cursor) PrevUint16Uint16ToBytesUint16() (uint16, uint16, []byte, uint16) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint16uint16(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key1, key2, value1, value2
}

// SeekUint16Uint16ToBytesUint16 is first with key of type pair(uint16,uint16) and value of type pair([]byte,uint16).
func (c *Cursor) SeekUint16Uint16ToBytesUint16(key1 uint16, key2 uint16) (uint16, uint16, []byte, uint16) {
	rawKey, rawValue := c.Seek(uint16uint16Tobytes(key1, key2))
	key1, key2 = bytesTouint16uint16(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key1, key2, value1, value2
}

// ForEachUint16Uint16ToBytesUint32 is a for each with key of type pair(uint16,uint16) and value of type pair([]byte,uint32).
func (b *Bucket) ForEachUint16Uint16ToBytesUint32(fn func(key1 uint16, key2 uint16, value1 []byte, value2 uint32) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint16uint16(rawKey)
			value1, value2 := bytesTobytesuint32(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetUint16Uint16ToBytesUint32 is a get with key of type pair(uint16,uint16) and value of type pair([]byte,uint32).
func (b *Bucket) GetUint16Uint16ToBytesUint32(key1 uint16, key2 uint16) ([]byte, uint32) {
	return bytesTobytesuint32(b.Get(uint16uint16Tobytes(key1, key2)))
}

// GetRequiredUint16Uint16ToBytesUint32 is a get with key of type pair(uint16,uint16) and value of type pair([]byte,uint32) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint16Uint16ToBytesUint32(key1 uint16, key2 uint16) ([]byte, uint32, error) {
	value := b.Get(uint16uint16Tobytes(key1, key2))
	if value == nil {
		return nil, 0, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTobytesuint32(value)
	return value1, value2, nil
}

// PutUint16Uint16ToBytesUint32 is a put with key of type pair(uint16,uint16) and value of type pair([]byte,uint32).
func (b *Bucket) PutUint16Uint16ToBytesUint32(key1 uint16, key2 uint16, value1 []byte, value2 uint32) error {
	return b.Put(uint16uint16Tobytes(key1, key2), bytesuint32Tobytes(value1, value2))
}

// FirstUint16Uint16ToBytesUint32 is first with key of type pair(uint16,uint16) and value of type pair([]byte,uint32).
func (c *Cursor) FirstUint16Uint16ToBytesUint32() (uint16, uint16, []byte, uint32) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint16uint16(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key1, key2, value1, value2
}

// LastUint16Uint16ToBytesUint32 is first with key of type pair(uint16,uint16) and value of type pair([]byte,uint32).
func (c *Cursor) LastUint16Uint16ToBytesUint32() (uint16, uint16, []byte, uint32) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint16uint16(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key1, key2, value1, value2
}

// NextUint16Uint16ToBytesUint32 is first with key of type pair(uint16,uint16) and value of type pair([]byte,uint32).
func (c *Cursor) NextUint16Uint16ToBytesUint32() (uint16, uint16, []byte, uint32) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint16uint16(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key1, key2, value1, value2
}

// PrevUint16Uint16ToBytesUint32 is first with key of type pair(uint16,uint16) and value of type pair([]byte,uint32).
func (c *Cursor) PrevUint16Uint16ToBytesUint32() (uint16, uint16, []byte, uint32) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint16uint16(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key1, key2, value1, value2
}

// SeekUint16Uint16ToBytesUint32 is first with key of type pair(uint16,uint16) and value of type pair([]byte,uint32).
func (c *Cursor) SeekUint16Uint16ToBytesUint32(key1 uint16, key2 uint16) (uint16, uint16, []byte, uint32) {
	rawKey, rawValue := c.Seek(uint16uint16Tobytes(key1, key2))
	key1, key2 = bytesTouint16uint16(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key1, key2, value1, value2
}

// ForEachUint16Uint16ToBytesUint64 is a for each with key of type pair(uint16,uint16) and value of type pair([]byte,uint64).
func (b *Bucket) ForEachUint16Uint16ToBytesUint64(fn func(key1 uint16, key2 uint16, value1 []byte, value2 uint64) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint16uint16(rawKey)
			value1, value2 := bytesTobytesuint64(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetUint16Uint16ToBytesUint64 is a get with key of type pair(uint16,uint16) and value of type pair([]byte,uint64).
func (b *Bucket) GetUint16Uint16ToBytesUint64(key1 uint16, key2 uint16) ([]byte, uint64) {
	return bytesTobytesuint64(b.Get(uint16uint16Tobytes(key1, key2)))
}

// GetRequiredUint16Uint16ToBytesUint64 is a get with key of type pair(uint16,uint16) and value of type pair([]byte,uint64) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint16Uint16ToBytesUint64(key1 uint16, key2 uint16) ([]byte, uint64, error) {
	value := b.Get(uint16uint16Tobytes(key1, key2))
	if value == nil {
		return nil, 0, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTobytesuint64(value)
	return value1, value2, nil
}

// PutUint16Uint16ToBytesUint64 is a put with key of type pair(uint16,uint16) and value of type pair([]byte,uint64).
func (b *Bucket) PutUint16Uint16ToBytesUint64(key1 uint16, key2 uint16, value1 []byte, value2 uint64) error {
	return b.Put(uint16uint16Tobytes(key1, key2), bytesuint64Tobytes(value1, value2))
}

// FirstUint16Uint16ToBytesUint64 is first with key of type pair(uint16,uint16) and value of type pair([]byte,uint64).
func (c *Cursor) FirstUint16Uint16ToBytesUint64() (uint16, uint16, []byte, uint64) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint16uint16(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key1, key2, value1, value2
}

// LastUint16Uint16ToBytesUint64 is first with key of type pair(uint16,uint16) and value of type pair([]byte,uint64).
func (c *Cursor) LastUint16Uint16ToBytesUint64() (uint16, uint16, []byte, uint64) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint16uint16(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key1, key2, value1, value2
}

// NextUint16Uint16ToBytesUint64 is first with key of type pair(uint16,uint16) and value of type pair([]byte,uint64).
func (c *Cursor) NextUint16Uint16ToBytesUint64() (uint16, uint16, []byte, uint64) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint16uint16(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key1, key2, value1, value2
}

// PrevUint16Uint16ToBytesUint64 is first with key of type pair(uint16,uint16) and value of type pair([]byte,uint64).
func (c *Cursor) PrevUint16Uint16ToBytesUint64() (uint16, uint16, []byte, uint64) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint16uint16(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key1, key2, value1, value2
}

// SeekUint16Uint16ToBytesUint64 is first with key of type pair(uint16,uint16) and value of type pair([]byte,uint64).
func (c *Cursor) SeekUint16Uint16ToBytesUint64(key1 uint16, key2 uint16) (uint16, uint16, []byte, uint64) {
	rawKey, rawValue := c.Seek(uint16uint16Tobytes(key1, key2))
	key1, key2 = bytesTouint16uint16(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key1, key2, value1, value2
}

// DeleteUint16Uint16 is a delete with key of type pair(uint16,uint16).
func (b *Bucket) DeleteUint16Uint16(key1 uint16, key2 uint16) error {
	return b.Delete(uint16uint16Tobytes(key1, key2))
}

// ForEachUint32Uint32ToBytes is a for each with key of type pair(uint32,uint32) and value of type []byte.
func (b *Bucket) ForEachUint32Uint32ToBytes(fn func(key1 uint32, key2 uint32, value []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint32uint32(rawKey)
			value := (rawValue)
			return fn(key1, key2, value)
		},
	)
}

// GetUint32Uint32ToBytes is a get with key of type pair(uint32,uint32) and value of type []byte.
func (b *Bucket) GetUint32Uint32ToBytes(key1 uint32, key2 uint32) []byte {
	return (b.Get(uint32uint32Tobytes(key1, key2)))
}

// GetRequiredUint32Uint32ToBytes is a get with key of type pair(uint32,uint32) and value of type []byte that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint32Uint32ToBytes(key1 uint32, key2 uint32) ([]byte, error) {
	value := b.Get(uint32uint32Tobytes(key1, key2))
	if value == nil {
		return nil, NewKeyNotFoundError(key1, key2)
	}
	return (value), nil
}

// PutUint32Uint32ToBytes is a put with key of type pair(uint32,uint32) and value of type []byte.
func (b *Bucket) PutUint32Uint32ToBytes(key1 uint32, key2 uint32, value []byte) error {
	return b.Put(uint32uint32Tobytes(key1, key2), (value))
}

// FirstUint32Uint32ToBytes is first with key of type pair(uint32,uint32) and value of type []byte.
func (c *Cursor) FirstUint32Uint32ToBytes() (uint32, uint32, []byte) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint32uint32(rawKey)
	value := (rawValue)
	return key1, key2, value
}

// LastUint32Uint32ToBytes is first with key of type pair(uint32,uint32) and value of type []byte.
func (c *Cursor) LastUint32Uint32ToBytes() (uint32, uint32, []byte) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint32uint32(rawKey)
	value := (rawValue)
	return key1, key2, value
}

// NextUint32Uint32ToBytes is first with key of type pair(uint32,uint32) and value of type []byte.
func (c *Cursor) NextUint32Uint32ToBytes() (uint32, uint32, []byte) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint32uint32(rawKey)
	value := (rawValue)
	return key1, key2, value
}

// PrevUint32Uint32ToBytes is first with key of type pair(uint32,uint32) and value of type []byte.
func (c *Cursor) PrevUint32Uint32ToBytes() (uint32, uint32, []byte) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint32uint32(rawKey)
	value := (rawValue)
	return key1, key2, value
}

// SeekUint32Uint32ToBytes is first with key of type pair(uint32,uint32) and value of type []byte.
func (c *Cursor) SeekUint32Uint32ToBytes(key1 uint32, key2 uint32) (uint32, uint32, []byte) {
	rawKey, rawValue := c.Seek(uint32uint32Tobytes(key1, key2))
	key1, key2 = bytesTouint32uint32(rawKey)
	value := (rawValue)
	return key1, key2, value
}

// ForEachUint32Uint32ToString is a for each with key of type pair(uint32,uint32) and value of type string.
func (b *Bucket) ForEachUint32Uint32ToString(fn func(key1 uint32, key2 uint32, value string) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint32uint32(rawKey)
			value := string(rawValue)
			return fn(key1, key2, value)
		},
	)
}

// GetUint32Uint32ToString is a get with key of type pair(uint32,uint32) and value of type string.
func (b *Bucket) GetUint32Uint32ToString(key1 uint32, key2 uint32) string {
	return string(b.Get(uint32uint32Tobytes(key1, key2)))
}

// GetRequiredUint32Uint32ToString is a get with key of type pair(uint32,uint32) and value of type string that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint32Uint32ToString(key1 uint32, key2 uint32) (string, error) {
	value := b.Get(uint32uint32Tobytes(key1, key2))
	if value == nil {
		return "", NewKeyNotFoundError(key1, key2)
	}
	return string(value), nil
}

// PutUint32Uint32ToString is a put with key of type pair(uint32,uint32) and value of type string.
func (b *Bucket) PutUint32Uint32ToString(key1 uint32, key2 uint32, value string) error {
	return b.Put(uint32uint32Tobytes(key1, key2), []byte(value))
}

// FirstUint32Uint32ToString is first with key of type pair(uint32,uint32) and value of type string.
func (c *Cursor) FirstUint32Uint32ToString() (uint32, uint32, string) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint32uint32(rawKey)
	value := string(rawValue)
	return key1, key2, value
}

// LastUint32Uint32ToString is first with key of type pair(uint32,uint32) and value of type string.
func (c *Cursor) LastUint32Uint32ToString() (uint32, uint32, string) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint32uint32(rawKey)
	value := string(rawValue)
	return key1, key2, value
}

// NextUint32Uint32ToString is first with key of type pair(uint32,uint32) and value of type string.
func (c *Cursor) NextUint32Uint32ToString() (uint32, uint32, string) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint32uint32(rawKey)
	value := string(rawValue)
	return key1, key2, value
}

// PrevUint32Uint32ToString is first with key of type pair(uint32,uint32) and value of type string.
func (c *Cursor) PrevUint32Uint32ToString() (uint32, uint32, string) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint32uint32(rawKey)
	value := string(rawValue)
	return key1, key2, value
}

// SeekUint32Uint32ToString is first with key of type pair(uint32,uint32) and value of type string.
func (c *Cursor) SeekUint32Uint32ToString(key1 uint32, key2 uint32) (uint32, uint32, string) {
	rawKey, rawValue := c.Seek(uint32uint32Tobytes(key1, key2))
	key1, key2 = bytesTouint32uint32(rawKey)
	value := string(rawValue)
	return key1, key2, value
}

// ForEachUint32Uint32ToUint16 is a for each with key of type pair(uint32,uint32) and value of type uint16.
func (b *Bucket) ForEachUint32Uint32ToUint16(fn func(key1 uint32, key2 uint32, value uint16) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint32uint32(rawKey)
			value := bytesTouint16(rawValue)
			return fn(key1, key2, value)
		},
	)
}

// GetUint32Uint32ToUint16 is a get with key of type pair(uint32,uint32) and value of type uint16.
func (b *Bucket) GetUint32Uint32ToUint16(key1 uint32, key2 uint32) uint16 {
	return bytesTouint16(b.Get(uint32uint32Tobytes(key1, key2)))
}

// GetRequiredUint32Uint32ToUint16 is a get with key of type pair(uint32,uint32) and value of type uint16 that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint32Uint32ToUint16(key1 uint32, key2 uint32) (uint16, error) {
	value := b.Get(uint32uint32Tobytes(key1, key2))
	if value == nil {
		return 0, NewKeyNotFoundError(key1, key2)
	}
	return bytesTouint16(value), nil
}

// PutUint32Uint32ToUint16 is a put with key of type pair(uint32,uint32) and value of type uint16.
func (b *Bucket) PutUint32Uint32ToUint16(key1 uint32, key2 uint32, value uint16) error {
	return b.Put(uint32uint32Tobytes(key1, key2), uint16Tobytes(value))
}

// FirstUint32Uint32ToUint16 is first with key of type pair(uint32,uint32) and value of type uint16.
func (c *Cursor) FirstUint32Uint32ToUint16() (uint32, uint32, uint16) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint32uint32(rawKey)
	value := bytesTouint16(rawValue)
	return key1, key2, value
}

// LastUint32Uint32ToUint16 is first with key of type pair(uint32,uint32) and value of type uint16.
func (c *Cursor) LastUint32Uint32ToUint16() (uint32, uint32, uint16) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint32uint32(rawKey)
	value := bytesTouint16(rawValue)
	return key1, key2, value
}

// NextUint32Uint32ToUint16 is first with key of type pair(uint32,uint32) and value of type uint16.
func (c *Cursor) NextUint32Uint32ToUint16() (uint32, uint32, uint16) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint32uint32(rawKey)
	value := bytesTouint16(rawValue)
	return key1, key2, value
}

// PrevUint32Uint32ToUint16 is first with key of type pair(uint32,uint32) and value of type uint16.
func (c *Cursor) PrevUint32Uint32ToUint16() (uint32, uint32, uint16) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint32uint32(rawKey)
	value := bytesTouint16(rawValue)
	return key1, key2, value
}

// SeekUint32Uint32ToUint16 is first with key of type pair(uint32,uint32) and value of type uint16.
func (c *Cursor) SeekUint32Uint32ToUint16(key1 uint32, key2 uint32) (uint32, uint32, uint16) {
	rawKey, rawValue := c.Seek(uint32uint32Tobytes(key1, key2))
	key1, key2 = bytesTouint32uint32(rawKey)
	value := bytesTouint16(rawValue)
	return key1, key2, value
}

// ForEachUint32Uint32ToUint32 is a for each with key of type pair(uint32,uint32) and value of type uint32.
func (b *Bucket) ForEachUint32Uint32ToUint32(fn func(key1 uint32, key2 uint32, value uint32) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint32uint32(rawKey)
			value := bytesTouint32(rawValue)
			return fn(key1, key2, value)
		},
	)
}

// GetUint32Uint32ToUint32 is a get with key of type pair(uint32,uint32) and value of type uint32.
func (b *Bucket) GetUint32Uint32ToUint32(key1 uint32, key2 uint32) uint32 {
	return bytesTouint32(b.Get(uint32uint32Tobytes(key1, key2)))
}

// GetRequiredUint32Uint32ToUint32 is a get with key of type pair(uint32,uint32) and value of type uint32 that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint32Uint32ToUint32(key1 uint32, key2 uint32) (uint32, error) {
	value := b.Get(uint32uint32Tobytes(key1, key2))
	if value == nil {
		return 0, NewKeyNotFoundError(key1, key2)
	}
	return bytesTouint32(value), nil
}

// PutUint32Uint32ToUint32 is a put with key of type pair(uint32,uint32) and value of type uint32.
func (b *Bucket) PutUint32Uint32ToUint32(key1 uint32, key2 uint32, value uint32) error {
	return b.Put(uint32uint32Tobytes(key1, key2), uint32Tobytes(value))
}

// FirstUint32Uint32ToUint32 is first with key of type pair(uint32,uint32) and value of type uint32.
func (c *Cursor) FirstUint32Uint32ToUint32() (uint32, uint32, uint32) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint32uint32(rawKey)
	value := bytesTouint32(rawValue)
	return key1, key2, value
}

// LastUint32Uint32ToUint32 is first with key of type pair(uint32,uint32) and value of type uint32.
func (c *Cursor) LastUint32Uint32ToUint32() (uint32, uint32, uint32) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint32uint32(rawKey)
	value := bytesTouint32(rawValue)
	return key1, key2, value
}

// NextUint32Uint32ToUint32 is first with key of type pair(uint32,uint32) and value of type uint32.
func (c *Cursor) NextUint32Uint32ToUint32() (uint32, uint32, uint32) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint32uint32(rawKey)
	value := bytesTouint32(rawValue)
	return key1, key2, value
}

// PrevUint32Uint32ToUint32 is first with key of type pair(uint32,uint32) and value of type uint32.
func (c *Cursor) PrevUint32Uint32ToUint32() (uint32, uint32, uint32) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint32uint32(rawKey)
	value := bytesTouint32(rawValue)
	return key1, key2, value
}

// SeekUint32Uint32ToUint32 is first with key of type pair(uint32,uint32) and value of type uint32.
func (c *Cursor) SeekUint32Uint32ToUint32(key1 uint32, key2 uint32) (uint32, uint32, uint32) {
	rawKey, rawValue := c.Seek(uint32uint32Tobytes(key1, key2))
	key1, key2 = bytesTouint32uint32(rawKey)
	value := bytesTouint32(rawValue)
	return key1, key2, value
}

// ForEachUint32Uint32ToUint64 is a for each with key of type pair(uint32,uint32) and value of type uint64.
func (b *Bucket) ForEachUint32Uint32ToUint64(fn func(key1 uint32, key2 uint32, value uint64) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint32uint32(rawKey)
			value := bytesTouint64(rawValue)
			return fn(key1, key2, value)
		},
	)
}

// GetUint32Uint32ToUint64 is a get with key of type pair(uint32,uint32) and value of type uint64.
func (b *Bucket) GetUint32Uint32ToUint64(key1 uint32, key2 uint32) uint64 {
	return bytesTouint64(b.Get(uint32uint32Tobytes(key1, key2)))
}

// GetRequiredUint32Uint32ToUint64 is a get with key of type pair(uint32,uint32) and value of type uint64 that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint32Uint32ToUint64(key1 uint32, key2 uint32) (uint64, error) {
	value := b.Get(uint32uint32Tobytes(key1, key2))
	if value == nil {
		return 0, NewKeyNotFoundError(key1, key2)
	}
	return bytesTouint64(value), nil
}

// PutUint32Uint32ToUint64 is a put with key of type pair(uint32,uint32) and value of type uint64.
func (b *Bucket) PutUint32Uint32ToUint64(key1 uint32, key2 uint32, value uint64) error {
	return b.Put(uint32uint32Tobytes(key1, key2), uint64Tobytes(value))
}

// FirstUint32Uint32ToUint64 is first with key of type pair(uint32,uint32) and value of type uint64.
func (c *Cursor) FirstUint32Uint32ToUint64() (uint32, uint32, uint64) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint32uint32(rawKey)
	value := bytesTouint64(rawValue)
	return key1, key2, value
}

// LastUint32Uint32ToUint64 is first with key of type pair(uint32,uint32) and value of type uint64.
func (c *Cursor) LastUint32Uint32ToUint64() (uint32, uint32, uint64) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint32uint32(rawKey)
	value := bytesTouint64(rawValue)
	return key1, key2, value
}

// NextUint32Uint32ToUint64 is first with key of type pair(uint32,uint32) and value of type uint64.
func (c *Cursor) NextUint32Uint32ToUint64() (uint32, uint32, uint64) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint32uint32(rawKey)
	value := bytesTouint64(rawValue)
	return key1, key2, value
}

// PrevUint32Uint32ToUint64 is first with key of type pair(uint32,uint32) and value of type uint64.
func (c *Cursor) PrevUint32Uint32ToUint64() (uint32, uint32, uint64) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint32uint32(rawKey)
	value := bytesTouint64(rawValue)
	return key1, key2, value
}

// SeekUint32Uint32ToUint64 is first with key of type pair(uint32,uint32) and value of type uint64.
func (c *Cursor) SeekUint32Uint32ToUint64(key1 uint32, key2 uint32) (uint32, uint32, uint64) {
	rawKey, rawValue := c.Seek(uint32uint32Tobytes(key1, key2))
	key1, key2 = bytesTouint32uint32(rawKey)
	value := bytesTouint64(rawValue)
	return key1, key2, value
}

// ForEachUint32Uint32ToUint16Uint16 is a for each with key of type pair(uint32,uint32) and value of type pair(uint16,uint16).
func (b *Bucket) ForEachUint32Uint32ToUint16Uint16(fn func(key1 uint32, key2 uint32, value1 uint16, value2 uint16) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint32uint32(rawKey)
			value1, value2 := bytesTouint16uint16(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetUint32Uint32ToUint16Uint16 is a get with key of type pair(uint32,uint32) and value of type pair(uint16,uint16).
func (b *Bucket) GetUint32Uint32ToUint16Uint16(key1 uint32, key2 uint32) (uint16, uint16) {
	return bytesTouint16uint16(b.Get(uint32uint32Tobytes(key1, key2)))
}

// GetRequiredUint32Uint32ToUint16Uint16 is a get with key of type pair(uint32,uint32) and value of type pair(uint16,uint16) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint32Uint32ToUint16Uint16(key1 uint32, key2 uint32) (uint16, uint16, error) {
	value := b.Get(uint32uint32Tobytes(key1, key2))
	if value == nil {
		return 0, 0, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTouint16uint16(value)
	return value1, value2, nil
}

// PutUint32Uint32ToUint16Uint16 is a put with key of type pair(uint32,uint32) and value of type pair(uint16,uint16).
func (b *Bucket) PutUint32Uint32ToUint16Uint16(key1 uint32, key2 uint32, value1 uint16, value2 uint16) error {
	return b.Put(uint32uint32Tobytes(key1, key2), uint16uint16Tobytes(value1, value2))
}

// FirstUint32Uint32ToUint16Uint16 is first with key of type pair(uint32,uint32) and value of type pair(uint16,uint16).
func (c *Cursor) FirstUint32Uint32ToUint16Uint16() (uint32, uint32, uint16, uint16) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint32uint32(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key1, key2, value1, value2
}

// LastUint32Uint32ToUint16Uint16 is first with key of type pair(uint32,uint32) and value of type pair(uint16,uint16).
func (c *Cursor) LastUint32Uint32ToUint16Uint16() (uint32, uint32, uint16, uint16) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint32uint32(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key1, key2, value1, value2
}

// NextUint32Uint32ToUint16Uint16 is first with key of type pair(uint32,uint32) and value of type pair(uint16,uint16).
func (c *Cursor) NextUint32Uint32ToUint16Uint16() (uint32, uint32, uint16, uint16) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint32uint32(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key1, key2, value1, value2
}

// PrevUint32Uint32ToUint16Uint16 is first with key of type pair(uint32,uint32) and value of type pair(uint16,uint16).
func (c *Cursor) PrevUint32Uint32ToUint16Uint16() (uint32, uint32, uint16, uint16) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint32uint32(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key1, key2, value1, value2
}

// SeekUint32Uint32ToUint16Uint16 is first with key of type pair(uint32,uint32) and value of type pair(uint16,uint16).
func (c *Cursor) SeekUint32Uint32ToUint16Uint16(key1 uint32, key2 uint32) (uint32, uint32, uint16, uint16) {
	rawKey, rawValue := c.Seek(uint32uint32Tobytes(key1, key2))
	key1, key2 = bytesTouint32uint32(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key1, key2, value1, value2
}

// ForEachUint32Uint32ToUint32Uint32 is a for each with key of type pair(uint32,uint32) and value of type pair(uint32,uint32).
func (b *Bucket) ForEachUint32Uint32ToUint32Uint32(fn func(key1 uint32, key2 uint32, value1 uint32, value2 uint32) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint32uint32(rawKey)
			value1, value2 := bytesTouint32uint32(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetUint32Uint32ToUint32Uint32 is a get with key of type pair(uint32,uint32) and value of type pair(uint32,uint32).
func (b *Bucket) GetUint32Uint32ToUint32Uint32(key1 uint32, key2 uint32) (uint32, uint32) {
	return bytesTouint32uint32(b.Get(uint32uint32Tobytes(key1, key2)))
}

// GetRequiredUint32Uint32ToUint32Uint32 is a get with key of type pair(uint32,uint32) and value of type pair(uint32,uint32) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint32Uint32ToUint32Uint32(key1 uint32, key2 uint32) (uint32, uint32, error) {
	value := b.Get(uint32uint32Tobytes(key1, key2))
	if value == nil {
		return 0, 0, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTouint32uint32(value)
	return value1, value2, nil
}

// PutUint32Uint32ToUint32Uint32 is a put with key of type pair(uint32,uint32) and value of type pair(uint32,uint32).
func (b *Bucket) PutUint32Uint32ToUint32Uint32(key1 uint32, key2 uint32, value1 uint32, value2 uint32) error {
	return b.Put(uint32uint32Tobytes(key1, key2), uint32uint32Tobytes(value1, value2))
}

// FirstUint32Uint32ToUint32Uint32 is first with key of type pair(uint32,uint32) and value of type pair(uint32,uint32).
func (c *Cursor) FirstUint32Uint32ToUint32Uint32() (uint32, uint32, uint32, uint32) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint32uint32(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key1, key2, value1, value2
}

// LastUint32Uint32ToUint32Uint32 is first with key of type pair(uint32,uint32) and value of type pair(uint32,uint32).
func (c *Cursor) LastUint32Uint32ToUint32Uint32() (uint32, uint32, uint32, uint32) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint32uint32(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key1, key2, value1, value2
}

// NextUint32Uint32ToUint32Uint32 is first with key of type pair(uint32,uint32) and value of type pair(uint32,uint32).
func (c *Cursor) NextUint32Uint32ToUint32Uint32() (uint32, uint32, uint32, uint32) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint32uint32(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key1, key2, value1, value2
}

// PrevUint32Uint32ToUint32Uint32 is first with key of type pair(uint32,uint32) and value of type pair(uint32,uint32).
func (c *Cursor) PrevUint32Uint32ToUint32Uint32() (uint32, uint32, uint32, uint32) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint32uint32(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key1, key2, value1, value2
}

// SeekUint32Uint32ToUint32Uint32 is first with key of type pair(uint32,uint32) and value of type pair(uint32,uint32).
func (c *Cursor) SeekUint32Uint32ToUint32Uint32(key1 uint32, key2 uint32) (uint32, uint32, uint32, uint32) {
	rawKey, rawValue := c.Seek(uint32uint32Tobytes(key1, key2))
	key1, key2 = bytesTouint32uint32(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key1, key2, value1, value2
}

// ForEachUint32Uint32ToUint64Uint64 is a for each with key of type pair(uint32,uint32) and value of type pair(uint64,uint64).
func (b *Bucket) ForEachUint32Uint32ToUint64Uint64(fn func(key1 uint32, key2 uint32, value1 uint64, value2 uint64) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint32uint32(rawKey)
			value1, value2 := bytesTouint64uint64(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetUint32Uint32ToUint64Uint64 is a get with key of type pair(uint32,uint32) and value of type pair(uint64,uint64).
func (b *Bucket) GetUint32Uint32ToUint64Uint64(key1 uint32, key2 uint32) (uint64, uint64) {
	return bytesTouint64uint64(b.Get(uint32uint32Tobytes(key1, key2)))
}

// GetRequiredUint32Uint32ToUint64Uint64 is a get with key of type pair(uint32,uint32) and value of type pair(uint64,uint64) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint32Uint32ToUint64Uint64(key1 uint32, key2 uint32) (uint64, uint64, error) {
	value := b.Get(uint32uint32Tobytes(key1, key2))
	if value == nil {
		return 0, 0, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTouint64uint64(value)
	return value1, value2, nil
}

// PutUint32Uint32ToUint64Uint64 is a put with key of type pair(uint32,uint32) and value of type pair(uint64,uint64).
func (b *Bucket) PutUint32Uint32ToUint64Uint64(key1 uint32, key2 uint32, value1 uint64, value2 uint64) error {
	return b.Put(uint32uint32Tobytes(key1, key2), uint64uint64Tobytes(value1, value2))
}

// FirstUint32Uint32ToUint64Uint64 is first with key of type pair(uint32,uint32) and value of type pair(uint64,uint64).
func (c *Cursor) FirstUint32Uint32ToUint64Uint64() (uint32, uint32, uint64, uint64) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint32uint32(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key1, key2, value1, value2
}

// LastUint32Uint32ToUint64Uint64 is first with key of type pair(uint32,uint32) and value of type pair(uint64,uint64).
func (c *Cursor) LastUint32Uint32ToUint64Uint64() (uint32, uint32, uint64, uint64) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint32uint32(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key1, key2, value1, value2
}

// NextUint32Uint32ToUint64Uint64 is first with key of type pair(uint32,uint32) and value of type pair(uint64,uint64).
func (c *Cursor) NextUint32Uint32ToUint64Uint64() (uint32, uint32, uint64, uint64) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint32uint32(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key1, key2, value1, value2
}

// PrevUint32Uint32ToUint64Uint64 is first with key of type pair(uint32,uint32) and value of type pair(uint64,uint64).
func (c *Cursor) PrevUint32Uint32ToUint64Uint64() (uint32, uint32, uint64, uint64) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint32uint32(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key1, key2, value1, value2
}

// SeekUint32Uint32ToUint64Uint64 is first with key of type pair(uint32,uint32) and value of type pair(uint64,uint64).
func (c *Cursor) SeekUint32Uint32ToUint64Uint64(key1 uint32, key2 uint32) (uint32, uint32, uint64, uint64) {
	rawKey, rawValue := c.Seek(uint32uint32Tobytes(key1, key2))
	key1, key2 = bytesTouint32uint32(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key1, key2, value1, value2
}

// ForEachUint32Uint32ToUint16Bytes is a for each with key of type pair(uint32,uint32) and value of type pair(uint16,[]byte).
func (b *Bucket) ForEachUint32Uint32ToUint16Bytes(fn func(key1 uint32, key2 uint32, value1 uint16, value2 []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint32uint32(rawKey)
			value1, value2 := bytesTouint16bytes(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetUint32Uint32ToUint16Bytes is a get with key of type pair(uint32,uint32) and value of type pair(uint16,[]byte).
func (b *Bucket) GetUint32Uint32ToUint16Bytes(key1 uint32, key2 uint32) (uint16, []byte) {
	return bytesTouint16bytes(b.Get(uint32uint32Tobytes(key1, key2)))
}

// GetRequiredUint32Uint32ToUint16Bytes is a get with key of type pair(uint32,uint32) and value of type pair(uint16,[]byte) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint32Uint32ToUint16Bytes(key1 uint32, key2 uint32) (uint16, []byte, error) {
	value := b.Get(uint32uint32Tobytes(key1, key2))
	if value == nil {
		return 0, nil, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTouint16bytes(value)
	return value1, value2, nil
}

// PutUint32Uint32ToUint16Bytes is a put with key of type pair(uint32,uint32) and value of type pair(uint16,[]byte).
func (b *Bucket) PutUint32Uint32ToUint16Bytes(key1 uint32, key2 uint32, value1 uint16, value2 []byte) error {
	return b.Put(uint32uint32Tobytes(key1, key2), uint16bytesTobytes(value1, value2))
}

// FirstUint32Uint32ToUint16Bytes is first with key of type pair(uint32,uint32) and value of type pair(uint16,[]byte).
func (c *Cursor) FirstUint32Uint32ToUint16Bytes() (uint32, uint32, uint16, []byte) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint32uint32(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key1, key2, value1, value2
}

// LastUint32Uint32ToUint16Bytes is first with key of type pair(uint32,uint32) and value of type pair(uint16,[]byte).
func (c *Cursor) LastUint32Uint32ToUint16Bytes() (uint32, uint32, uint16, []byte) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint32uint32(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key1, key2, value1, value2
}

// NextUint32Uint32ToUint16Bytes is first with key of type pair(uint32,uint32) and value of type pair(uint16,[]byte).
func (c *Cursor) NextUint32Uint32ToUint16Bytes() (uint32, uint32, uint16, []byte) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint32uint32(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key1, key2, value1, value2
}

// PrevUint32Uint32ToUint16Bytes is first with key of type pair(uint32,uint32) and value of type pair(uint16,[]byte).
func (c *Cursor) PrevUint32Uint32ToUint16Bytes() (uint32, uint32, uint16, []byte) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint32uint32(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key1, key2, value1, value2
}

// SeekUint32Uint32ToUint16Bytes is first with key of type pair(uint32,uint32) and value of type pair(uint16,[]byte).
func (c *Cursor) SeekUint32Uint32ToUint16Bytes(key1 uint32, key2 uint32) (uint32, uint32, uint16, []byte) {
	rawKey, rawValue := c.Seek(uint32uint32Tobytes(key1, key2))
	key1, key2 = bytesTouint32uint32(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key1, key2, value1, value2
}

// ForEachUint32Uint32ToUint32Bytes is a for each with key of type pair(uint32,uint32) and value of type pair(uint32,[]byte).
func (b *Bucket) ForEachUint32Uint32ToUint32Bytes(fn func(key1 uint32, key2 uint32, value1 uint32, value2 []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint32uint32(rawKey)
			value1, value2 := bytesTouint32bytes(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetUint32Uint32ToUint32Bytes is a get with key of type pair(uint32,uint32) and value of type pair(uint32,[]byte).
func (b *Bucket) GetUint32Uint32ToUint32Bytes(key1 uint32, key2 uint32) (uint32, []byte) {
	return bytesTouint32bytes(b.Get(uint32uint32Tobytes(key1, key2)))
}

// GetRequiredUint32Uint32ToUint32Bytes is a get with key of type pair(uint32,uint32) and value of type pair(uint32,[]byte) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint32Uint32ToUint32Bytes(key1 uint32, key2 uint32) (uint32, []byte, error) {
	value := b.Get(uint32uint32Tobytes(key1, key2))
	if value == nil {
		return 0, nil, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTouint32bytes(value)
	return value1, value2, nil
}

// PutUint32Uint32ToUint32Bytes is a put with key of type pair(uint32,uint32) and value of type pair(uint32,[]byte).
func (b *Bucket) PutUint32Uint32ToUint32Bytes(key1 uint32, key2 uint32, value1 uint32, value2 []byte) error {
	return b.Put(uint32uint32Tobytes(key1, key2), uint32bytesTobytes(value1, value2))
}

// FirstUint32Uint32ToUint32Bytes is first with key of type pair(uint32,uint32) and value of type pair(uint32,[]byte).
func (c *Cursor) FirstUint32Uint32ToUint32Bytes() (uint32, uint32, uint32, []byte) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint32uint32(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key1, key2, value1, value2
}

// LastUint32Uint32ToUint32Bytes is first with key of type pair(uint32,uint32) and value of type pair(uint32,[]byte).
func (c *Cursor) LastUint32Uint32ToUint32Bytes() (uint32, uint32, uint32, []byte) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint32uint32(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key1, key2, value1, value2
}

// NextUint32Uint32ToUint32Bytes is first with key of type pair(uint32,uint32) and value of type pair(uint32,[]byte).
func (c *Cursor) NextUint32Uint32ToUint32Bytes() (uint32, uint32, uint32, []byte) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint32uint32(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key1, key2, value1, value2
}

// PrevUint32Uint32ToUint32Bytes is first with key of type pair(uint32,uint32) and value of type pair(uint32,[]byte).
func (c *Cursor) PrevUint32Uint32ToUint32Bytes() (uint32, uint32, uint32, []byte) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint32uint32(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key1, key2, value1, value2
}

// SeekUint32Uint32ToUint32Bytes is first with key of type pair(uint32,uint32) and value of type pair(uint32,[]byte).
func (c *Cursor) SeekUint32Uint32ToUint32Bytes(key1 uint32, key2 uint32) (uint32, uint32, uint32, []byte) {
	rawKey, rawValue := c.Seek(uint32uint32Tobytes(key1, key2))
	key1, key2 = bytesTouint32uint32(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key1, key2, value1, value2
}

// ForEachUint32Uint32ToUint64Bytes is a for each with key of type pair(uint32,uint32) and value of type pair(uint64,[]byte).
func (b *Bucket) ForEachUint32Uint32ToUint64Bytes(fn func(key1 uint32, key2 uint32, value1 uint64, value2 []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint32uint32(rawKey)
			value1, value2 := bytesTouint64bytes(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetUint32Uint32ToUint64Bytes is a get with key of type pair(uint32,uint32) and value of type pair(uint64,[]byte).
func (b *Bucket) GetUint32Uint32ToUint64Bytes(key1 uint32, key2 uint32) (uint64, []byte) {
	return bytesTouint64bytes(b.Get(uint32uint32Tobytes(key1, key2)))
}

// GetRequiredUint32Uint32ToUint64Bytes is a get with key of type pair(uint32,uint32) and value of type pair(uint64,[]byte) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint32Uint32ToUint64Bytes(key1 uint32, key2 uint32) (uint64, []byte, error) {
	value := b.Get(uint32uint32Tobytes(key1, key2))
	if value == nil {
		return 0, nil, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTouint64bytes(value)
	return value1, value2, nil
}

// PutUint32Uint32ToUint64Bytes is a put with key of type pair(uint32,uint32) and value of type pair(uint64,[]byte).
func (b *Bucket) PutUint32Uint32ToUint64Bytes(key1 uint32, key2 uint32, value1 uint64, value2 []byte) error {
	return b.Put(uint32uint32Tobytes(key1, key2), uint64bytesTobytes(value1, value2))
}

// FirstUint32Uint32ToUint64Bytes is first with key of type pair(uint32,uint32) and value of type pair(uint64,[]byte).
func (c *Cursor) FirstUint32Uint32ToUint64Bytes() (uint32, uint32, uint64, []byte) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint32uint32(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key1, key2, value1, value2
}

// LastUint32Uint32ToUint64Bytes is first with key of type pair(uint32,uint32) and value of type pair(uint64,[]byte).
func (c *Cursor) LastUint32Uint32ToUint64Bytes() (uint32, uint32, uint64, []byte) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint32uint32(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key1, key2, value1, value2
}

// NextUint32Uint32ToUint64Bytes is first with key of type pair(uint32,uint32) and value of type pair(uint64,[]byte).
func (c *Cursor) NextUint32Uint32ToUint64Bytes() (uint32, uint32, uint64, []byte) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint32uint32(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key1, key2, value1, value2
}

// PrevUint32Uint32ToUint64Bytes is first with key of type pair(uint32,uint32) and value of type pair(uint64,[]byte).
func (c *Cursor) PrevUint32Uint32ToUint64Bytes() (uint32, uint32, uint64, []byte) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint32uint32(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key1, key2, value1, value2
}

// SeekUint32Uint32ToUint64Bytes is first with key of type pair(uint32,uint32) and value of type pair(uint64,[]byte).
func (c *Cursor) SeekUint32Uint32ToUint64Bytes(key1 uint32, key2 uint32) (uint32, uint32, uint64, []byte) {
	rawKey, rawValue := c.Seek(uint32uint32Tobytes(key1, key2))
	key1, key2 = bytesTouint32uint32(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key1, key2, value1, value2
}

// ForEachUint32Uint32ToBytesUint16 is a for each with key of type pair(uint32,uint32) and value of type pair([]byte,uint16).
func (b *Bucket) ForEachUint32Uint32ToBytesUint16(fn func(key1 uint32, key2 uint32, value1 []byte, value2 uint16) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint32uint32(rawKey)
			value1, value2 := bytesTobytesuint16(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetUint32Uint32ToBytesUint16 is a get with key of type pair(uint32,uint32) and value of type pair([]byte,uint16).
func (b *Bucket) GetUint32Uint32ToBytesUint16(key1 uint32, key2 uint32) ([]byte, uint16) {
	return bytesTobytesuint16(b.Get(uint32uint32Tobytes(key1, key2)))
}

// GetRequiredUint32Uint32ToBytesUint16 is a get with key of type pair(uint32,uint32) and value of type pair([]byte,uint16) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint32Uint32ToBytesUint16(key1 uint32, key2 uint32) ([]byte, uint16, error) {
	value := b.Get(uint32uint32Tobytes(key1, key2))
	if value == nil {
		return nil, 0, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTobytesuint16(value)
	return value1, value2, nil
}

// PutUint32Uint32ToBytesUint16 is a put with key of type pair(uint32,uint32) and value of type pair([]byte,uint16).
func (b *Bucket) PutUint32Uint32ToBytesUint16(key1 uint32, key2 uint32, value1 []byte, value2 uint16) error {
	return b.Put(uint32uint32Tobytes(key1, key2), bytesuint16Tobytes(value1, value2))
}

// FirstUint32Uint32ToBytesUint16 is first with key of type pair(uint32,uint32) and value of type pair([]byte,uint16).
func (c *Cursor) FirstUint32Uint32ToBytesUint16() (uint32, uint32, []byte, uint16) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint32uint32(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key1, key2, value1, value2
}

// LastUint32Uint32ToBytesUint16 is first with key of type pair(uint32,uint32) and value of type pair([]byte,uint16).
func (c *Cursor) LastUint32Uint32ToBytesUint16() (uint32, uint32, []byte, uint16) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint32uint32(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key1, key2, value1, value2
}

// NextUint32Uint32ToBytesUint16 is first with key of type pair(uint32,uint32) and value of type pair([]byte,uint16).
func (c *Cursor) NextUint32Uint32ToBytesUint16() (uint32, uint32, []byte, uint16) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint32uint32(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key1, key2, value1, value2
}

// PrevUint32Uint32ToBytesUint16 is first with key of type pair(uint32,uint32) and value of type pair([]byte,uint16).
func (c *Cursor) PrevUint32Uint32ToBytesUint16() (uint32, uint32, []byte, uint16) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint32uint32(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key1, key2, value1, value2
}

// SeekUint32Uint32ToBytesUint16 is first with key of type pair(uint32,uint32) and value of type pair([]byte,uint16).
func (c *Cursor) SeekUint32Uint32ToBytesUint16(key1 uint32, key2 uint32) (uint32, uint32, []byte, uint16) {
	rawKey, rawValue := c.Seek(uint32uint32Tobytes(key1, key2))
	key1, key2 = bytesTouint32uint32(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key1, key2, value1, value2
}

// ForEachUint32Uint32ToBytesUint32 is a for each with key of type pair(uint32,uint32) and value of type pair([]byte,uint32).
func (b *Bucket) ForEachUint32Uint32ToBytesUint32(fn func(key1 uint32, key2 uint32, value1 []byte, value2 uint32) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint32uint32(rawKey)
			value1, value2 := bytesTobytesuint32(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetUint32Uint32ToBytesUint32 is a get with key of type pair(uint32,uint32) and value of type pair([]byte,uint32).
func (b *Bucket) GetUint32Uint32ToBytesUint32(key1 uint32, key2 uint32) ([]byte, uint32) {
	return bytesTobytesuint32(b.Get(uint32uint32Tobytes(key1, key2)))
}

// GetRequiredUint32Uint32ToBytesUint32 is a get with key of type pair(uint32,uint32) and value of type pair([]byte,uint32) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint32Uint32ToBytesUint32(key1 uint32, key2 uint32) ([]byte, uint32, error) {
	value := b.Get(uint32uint32Tobytes(key1, key2))
	if value == nil {
		return nil, 0, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTobytesuint32(value)
	return value1, value2, nil
}

// PutUint32Uint32ToBytesUint32 is a put with key of type pair(uint32,uint32) and value of type pair([]byte,uint32).
func (b *Bucket) PutUint32Uint32ToBytesUint32(key1 uint32, key2 uint32, value1 []byte, value2 uint32) error {
	return b.Put(uint32uint32Tobytes(key1, key2), bytesuint32Tobytes(value1, value2))
}

// FirstUint32Uint32ToBytesUint32 is first with key of type pair(uint32,uint32) and value of type pair([]byte,uint32).
func (c *Cursor) FirstUint32Uint32ToBytesUint32() (uint32, uint32, []byte, uint32) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint32uint32(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key1, key2, value1, value2
}

// LastUint32Uint32ToBytesUint32 is first with key of type pair(uint32,uint32) and value of type pair([]byte,uint32).
func (c *Cursor) LastUint32Uint32ToBytesUint32() (uint32, uint32, []byte, uint32) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint32uint32(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key1, key2, value1, value2
}

// NextUint32Uint32ToBytesUint32 is first with key of type pair(uint32,uint32) and value of type pair([]byte,uint32).
func (c *Cursor) NextUint32Uint32ToBytesUint32() (uint32, uint32, []byte, uint32) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint32uint32(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key1, key2, value1, value2
}

// PrevUint32Uint32ToBytesUint32 is first with key of type pair(uint32,uint32) and value of type pair([]byte,uint32).
func (c *Cursor) PrevUint32Uint32ToBytesUint32() (uint32, uint32, []byte, uint32) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint32uint32(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key1, key2, value1, value2
}

// SeekUint32Uint32ToBytesUint32 is first with key of type pair(uint32,uint32) and value of type pair([]byte,uint32).
func (c *Cursor) SeekUint32Uint32ToBytesUint32(key1 uint32, key2 uint32) (uint32, uint32, []byte, uint32) {
	rawKey, rawValue := c.Seek(uint32uint32Tobytes(key1, key2))
	key1, key2 = bytesTouint32uint32(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key1, key2, value1, value2
}

// ForEachUint32Uint32ToBytesUint64 is a for each with key of type pair(uint32,uint32) and value of type pair([]byte,uint64).
func (b *Bucket) ForEachUint32Uint32ToBytesUint64(fn func(key1 uint32, key2 uint32, value1 []byte, value2 uint64) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint32uint32(rawKey)
			value1, value2 := bytesTobytesuint64(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetUint32Uint32ToBytesUint64 is a get with key of type pair(uint32,uint32) and value of type pair([]byte,uint64).
func (b *Bucket) GetUint32Uint32ToBytesUint64(key1 uint32, key2 uint32) ([]byte, uint64) {
	return bytesTobytesuint64(b.Get(uint32uint32Tobytes(key1, key2)))
}

// GetRequiredUint32Uint32ToBytesUint64 is a get with key of type pair(uint32,uint32) and value of type pair([]byte,uint64) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint32Uint32ToBytesUint64(key1 uint32, key2 uint32) ([]byte, uint64, error) {
	value := b.Get(uint32uint32Tobytes(key1, key2))
	if value == nil {
		return nil, 0, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTobytesuint64(value)
	return value1, value2, nil
}

// PutUint32Uint32ToBytesUint64 is a put with key of type pair(uint32,uint32) and value of type pair([]byte,uint64).
func (b *Bucket) PutUint32Uint32ToBytesUint64(key1 uint32, key2 uint32, value1 []byte, value2 uint64) error {
	return b.Put(uint32uint32Tobytes(key1, key2), bytesuint64Tobytes(value1, value2))
}

// FirstUint32Uint32ToBytesUint64 is first with key of type pair(uint32,uint32) and value of type pair([]byte,uint64).
func (c *Cursor) FirstUint32Uint32ToBytesUint64() (uint32, uint32, []byte, uint64) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint32uint32(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key1, key2, value1, value2
}

// LastUint32Uint32ToBytesUint64 is first with key of type pair(uint32,uint32) and value of type pair([]byte,uint64).
func (c *Cursor) LastUint32Uint32ToBytesUint64() (uint32, uint32, []byte, uint64) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint32uint32(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key1, key2, value1, value2
}

// NextUint32Uint32ToBytesUint64 is first with key of type pair(uint32,uint32) and value of type pair([]byte,uint64).
func (c *Cursor) NextUint32Uint32ToBytesUint64() (uint32, uint32, []byte, uint64) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint32uint32(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key1, key2, value1, value2
}

// PrevUint32Uint32ToBytesUint64 is first with key of type pair(uint32,uint32) and value of type pair([]byte,uint64).
func (c *Cursor) PrevUint32Uint32ToBytesUint64() (uint32, uint32, []byte, uint64) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint32uint32(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key1, key2, value1, value2
}

// SeekUint32Uint32ToBytesUint64 is first with key of type pair(uint32,uint32) and value of type pair([]byte,uint64).
func (c *Cursor) SeekUint32Uint32ToBytesUint64(key1 uint32, key2 uint32) (uint32, uint32, []byte, uint64) {
	rawKey, rawValue := c.Seek(uint32uint32Tobytes(key1, key2))
	key1, key2 = bytesTouint32uint32(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key1, key2, value1, value2
}

// DeleteUint32Uint32 is a delete with key of type pair(uint32,uint32).
func (b *Bucket) DeleteUint32Uint32(key1 uint32, key2 uint32) error {
	return b.Delete(uint32uint32Tobytes(key1, key2))
}

// ForEachUint64Uint64ToBytes is a for each with key of type pair(uint64,uint64) and value of type []byte.
func (b *Bucket) ForEachUint64Uint64ToBytes(fn func(key1 uint64, key2 uint64, value []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint64uint64(rawKey)
			value := (rawValue)
			return fn(key1, key2, value)
		},
	)
}

// GetUint64Uint64ToBytes is a get with key of type pair(uint64,uint64) and value of type []byte.
func (b *Bucket) GetUint64Uint64ToBytes(key1 uint64, key2 uint64) []byte {
	return (b.Get(uint64uint64Tobytes(key1, key2)))
}

// GetRequiredUint64Uint64ToBytes is a get with key of type pair(uint64,uint64) and value of type []byte that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint64Uint64ToBytes(key1 uint64, key2 uint64) ([]byte, error) {
	value := b.Get(uint64uint64Tobytes(key1, key2))
	if value == nil {
		return nil, NewKeyNotFoundError(key1, key2)
	}
	return (value), nil
}

// PutUint64Uint64ToBytes is a put with key of type pair(uint64,uint64) and value of type []byte.
func (b *Bucket) PutUint64Uint64ToBytes(key1 uint64, key2 uint64, value []byte) error {
	return b.Put(uint64uint64Tobytes(key1, key2), (value))
}

// FirstUint64Uint64ToBytes is first with key of type pair(uint64,uint64) and value of type []byte.
func (c *Cursor) FirstUint64Uint64ToBytes() (uint64, uint64, []byte) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint64uint64(rawKey)
	value := (rawValue)
	return key1, key2, value
}

// LastUint64Uint64ToBytes is first with key of type pair(uint64,uint64) and value of type []byte.
func (c *Cursor) LastUint64Uint64ToBytes() (uint64, uint64, []byte) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint64uint64(rawKey)
	value := (rawValue)
	return key1, key2, value
}

// NextUint64Uint64ToBytes is first with key of type pair(uint64,uint64) and value of type []byte.
func (c *Cursor) NextUint64Uint64ToBytes() (uint64, uint64, []byte) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint64uint64(rawKey)
	value := (rawValue)
	return key1, key2, value
}

// PrevUint64Uint64ToBytes is first with key of type pair(uint64,uint64) and value of type []byte.
func (c *Cursor) PrevUint64Uint64ToBytes() (uint64, uint64, []byte) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint64uint64(rawKey)
	value := (rawValue)
	return key1, key2, value
}

// SeekUint64Uint64ToBytes is first with key of type pair(uint64,uint64) and value of type []byte.
func (c *Cursor) SeekUint64Uint64ToBytes(key1 uint64, key2 uint64) (uint64, uint64, []byte) {
	rawKey, rawValue := c.Seek(uint64uint64Tobytes(key1, key2))
	key1, key2 = bytesTouint64uint64(rawKey)
	value := (rawValue)
	return key1, key2, value
}

// ForEachUint64Uint64ToString is a for each with key of type pair(uint64,uint64) and value of type string.
func (b *Bucket) ForEachUint64Uint64ToString(fn func(key1 uint64, key2 uint64, value string) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint64uint64(rawKey)
			value := string(rawValue)
			return fn(key1, key2, value)
		},
	)
}

// GetUint64Uint64ToString is a get with key of type pair(uint64,uint64) and value of type string.
func (b *Bucket) GetUint64Uint64ToString(key1 uint64, key2 uint64) string {
	return string(b.Get(uint64uint64Tobytes(key1, key2)))
}

// GetRequiredUint64Uint64ToString is a get with key of type pair(uint64,uint64) and value of type string that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint64Uint64ToString(key1 uint64, key2 uint64) (string, error) {
	value := b.Get(uint64uint64Tobytes(key1, key2))
	if value == nil {
		return "", NewKeyNotFoundError(key1, key2)
	}
	return string(value), nil
}

// PutUint64Uint64ToString is a put with key of type pair(uint64,uint64) and value of type string.
func (b *Bucket) PutUint64Uint64ToString(key1 uint64, key2 uint64, value string) error {
	return b.Put(uint64uint64Tobytes(key1, key2), []byte(value))
}

// FirstUint64Uint64ToString is first with key of type pair(uint64,uint64) and value of type string.
func (c *Cursor) FirstUint64Uint64ToString() (uint64, uint64, string) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint64uint64(rawKey)
	value := string(rawValue)
	return key1, key2, value
}

// LastUint64Uint64ToString is first with key of type pair(uint64,uint64) and value of type string.
func (c *Cursor) LastUint64Uint64ToString() (uint64, uint64, string) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint64uint64(rawKey)
	value := string(rawValue)
	return key1, key2, value
}

// NextUint64Uint64ToString is first with key of type pair(uint64,uint64) and value of type string.
func (c *Cursor) NextUint64Uint64ToString() (uint64, uint64, string) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint64uint64(rawKey)
	value := string(rawValue)
	return key1, key2, value
}

// PrevUint64Uint64ToString is first with key of type pair(uint64,uint64) and value of type string.
func (c *Cursor) PrevUint64Uint64ToString() (uint64, uint64, string) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint64uint64(rawKey)
	value := string(rawValue)
	return key1, key2, value
}

// SeekUint64Uint64ToString is first with key of type pair(uint64,uint64) and value of type string.
func (c *Cursor) SeekUint64Uint64ToString(key1 uint64, key2 uint64) (uint64, uint64, string) {
	rawKey, rawValue := c.Seek(uint64uint64Tobytes(key1, key2))
	key1, key2 = bytesTouint64uint64(rawKey)
	value := string(rawValue)
	return key1, key2, value
}

// ForEachUint64Uint64ToUint16 is a for each with key of type pair(uint64,uint64) and value of type uint16.
func (b *Bucket) ForEachUint64Uint64ToUint16(fn func(key1 uint64, key2 uint64, value uint16) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint64uint64(rawKey)
			value := bytesTouint16(rawValue)
			return fn(key1, key2, value)
		},
	)
}

// GetUint64Uint64ToUint16 is a get with key of type pair(uint64,uint64) and value of type uint16.
func (b *Bucket) GetUint64Uint64ToUint16(key1 uint64, key2 uint64) uint16 {
	return bytesTouint16(b.Get(uint64uint64Tobytes(key1, key2)))
}

// GetRequiredUint64Uint64ToUint16 is a get with key of type pair(uint64,uint64) and value of type uint16 that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint64Uint64ToUint16(key1 uint64, key2 uint64) (uint16, error) {
	value := b.Get(uint64uint64Tobytes(key1, key2))
	if value == nil {
		return 0, NewKeyNotFoundError(key1, key2)
	}
	return bytesTouint16(value), nil
}

// PutUint64Uint64ToUint16 is a put with key of type pair(uint64,uint64) and value of type uint16.
func (b *Bucket) PutUint64Uint64ToUint16(key1 uint64, key2 uint64, value uint16) error {
	return b.Put(uint64uint64Tobytes(key1, key2), uint16Tobytes(value))
}

// FirstUint64Uint64ToUint16 is first with key of type pair(uint64,uint64) and value of type uint16.
func (c *Cursor) FirstUint64Uint64ToUint16() (uint64, uint64, uint16) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint64uint64(rawKey)
	value := bytesTouint16(rawValue)
	return key1, key2, value
}

// LastUint64Uint64ToUint16 is first with key of type pair(uint64,uint64) and value of type uint16.
func (c *Cursor) LastUint64Uint64ToUint16() (uint64, uint64, uint16) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint64uint64(rawKey)
	value := bytesTouint16(rawValue)
	return key1, key2, value
}

// NextUint64Uint64ToUint16 is first with key of type pair(uint64,uint64) and value of type uint16.
func (c *Cursor) NextUint64Uint64ToUint16() (uint64, uint64, uint16) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint64uint64(rawKey)
	value := bytesTouint16(rawValue)
	return key1, key2, value
}

// PrevUint64Uint64ToUint16 is first with key of type pair(uint64,uint64) and value of type uint16.
func (c *Cursor) PrevUint64Uint64ToUint16() (uint64, uint64, uint16) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint64uint64(rawKey)
	value := bytesTouint16(rawValue)
	return key1, key2, value
}

// SeekUint64Uint64ToUint16 is first with key of type pair(uint64,uint64) and value of type uint16.
func (c *Cursor) SeekUint64Uint64ToUint16(key1 uint64, key2 uint64) (uint64, uint64, uint16) {
	rawKey, rawValue := c.Seek(uint64uint64Tobytes(key1, key2))
	key1, key2 = bytesTouint64uint64(rawKey)
	value := bytesTouint16(rawValue)
	return key1, key2, value
}

// ForEachUint64Uint64ToUint32 is a for each with key of type pair(uint64,uint64) and value of type uint32.
func (b *Bucket) ForEachUint64Uint64ToUint32(fn func(key1 uint64, key2 uint64, value uint32) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint64uint64(rawKey)
			value := bytesTouint32(rawValue)
			return fn(key1, key2, value)
		},
	)
}

// GetUint64Uint64ToUint32 is a get with key of type pair(uint64,uint64) and value of type uint32.
func (b *Bucket) GetUint64Uint64ToUint32(key1 uint64, key2 uint64) uint32 {
	return bytesTouint32(b.Get(uint64uint64Tobytes(key1, key2)))
}

// GetRequiredUint64Uint64ToUint32 is a get with key of type pair(uint64,uint64) and value of type uint32 that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint64Uint64ToUint32(key1 uint64, key2 uint64) (uint32, error) {
	value := b.Get(uint64uint64Tobytes(key1, key2))
	if value == nil {
		return 0, NewKeyNotFoundError(key1, key2)
	}
	return bytesTouint32(value), nil
}

// PutUint64Uint64ToUint32 is a put with key of type pair(uint64,uint64) and value of type uint32.
func (b *Bucket) PutUint64Uint64ToUint32(key1 uint64, key2 uint64, value uint32) error {
	return b.Put(uint64uint64Tobytes(key1, key2), uint32Tobytes(value))
}

// FirstUint64Uint64ToUint32 is first with key of type pair(uint64,uint64) and value of type uint32.
func (c *Cursor) FirstUint64Uint64ToUint32() (uint64, uint64, uint32) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint64uint64(rawKey)
	value := bytesTouint32(rawValue)
	return key1, key2, value
}

// LastUint64Uint64ToUint32 is first with key of type pair(uint64,uint64) and value of type uint32.
func (c *Cursor) LastUint64Uint64ToUint32() (uint64, uint64, uint32) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint64uint64(rawKey)
	value := bytesTouint32(rawValue)
	return key1, key2, value
}

// NextUint64Uint64ToUint32 is first with key of type pair(uint64,uint64) and value of type uint32.
func (c *Cursor) NextUint64Uint64ToUint32() (uint64, uint64, uint32) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint64uint64(rawKey)
	value := bytesTouint32(rawValue)
	return key1, key2, value
}

// PrevUint64Uint64ToUint32 is first with key of type pair(uint64,uint64) and value of type uint32.
func (c *Cursor) PrevUint64Uint64ToUint32() (uint64, uint64, uint32) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint64uint64(rawKey)
	value := bytesTouint32(rawValue)
	return key1, key2, value
}

// SeekUint64Uint64ToUint32 is first with key of type pair(uint64,uint64) and value of type uint32.
func (c *Cursor) SeekUint64Uint64ToUint32(key1 uint64, key2 uint64) (uint64, uint64, uint32) {
	rawKey, rawValue := c.Seek(uint64uint64Tobytes(key1, key2))
	key1, key2 = bytesTouint64uint64(rawKey)
	value := bytesTouint32(rawValue)
	return key1, key2, value
}

// ForEachUint64Uint64ToUint64 is a for each with key of type pair(uint64,uint64) and value of type uint64.
func (b *Bucket) ForEachUint64Uint64ToUint64(fn func(key1 uint64, key2 uint64, value uint64) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint64uint64(rawKey)
			value := bytesTouint64(rawValue)
			return fn(key1, key2, value)
		},
	)
}

// GetUint64Uint64ToUint64 is a get with key of type pair(uint64,uint64) and value of type uint64.
func (b *Bucket) GetUint64Uint64ToUint64(key1 uint64, key2 uint64) uint64 {
	return bytesTouint64(b.Get(uint64uint64Tobytes(key1, key2)))
}

// GetRequiredUint64Uint64ToUint64 is a get with key of type pair(uint64,uint64) and value of type uint64 that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint64Uint64ToUint64(key1 uint64, key2 uint64) (uint64, error) {
	value := b.Get(uint64uint64Tobytes(key1, key2))
	if value == nil {
		return 0, NewKeyNotFoundError(key1, key2)
	}
	return bytesTouint64(value), nil
}

// PutUint64Uint64ToUint64 is a put with key of type pair(uint64,uint64) and value of type uint64.
func (b *Bucket) PutUint64Uint64ToUint64(key1 uint64, key2 uint64, value uint64) error {
	return b.Put(uint64uint64Tobytes(key1, key2), uint64Tobytes(value))
}

// FirstUint64Uint64ToUint64 is first with key of type pair(uint64,uint64) and value of type uint64.
func (c *Cursor) FirstUint64Uint64ToUint64() (uint64, uint64, uint64) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint64uint64(rawKey)
	value := bytesTouint64(rawValue)
	return key1, key2, value
}

// LastUint64Uint64ToUint64 is first with key of type pair(uint64,uint64) and value of type uint64.
func (c *Cursor) LastUint64Uint64ToUint64() (uint64, uint64, uint64) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint64uint64(rawKey)
	value := bytesTouint64(rawValue)
	return key1, key2, value
}

// NextUint64Uint64ToUint64 is first with key of type pair(uint64,uint64) and value of type uint64.
func (c *Cursor) NextUint64Uint64ToUint64() (uint64, uint64, uint64) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint64uint64(rawKey)
	value := bytesTouint64(rawValue)
	return key1, key2, value
}

// PrevUint64Uint64ToUint64 is first with key of type pair(uint64,uint64) and value of type uint64.
func (c *Cursor) PrevUint64Uint64ToUint64() (uint64, uint64, uint64) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint64uint64(rawKey)
	value := bytesTouint64(rawValue)
	return key1, key2, value
}

// SeekUint64Uint64ToUint64 is first with key of type pair(uint64,uint64) and value of type uint64.
func (c *Cursor) SeekUint64Uint64ToUint64(key1 uint64, key2 uint64) (uint64, uint64, uint64) {
	rawKey, rawValue := c.Seek(uint64uint64Tobytes(key1, key2))
	key1, key2 = bytesTouint64uint64(rawKey)
	value := bytesTouint64(rawValue)
	return key1, key2, value
}

// ForEachUint64Uint64ToUint16Uint16 is a for each with key of type pair(uint64,uint64) and value of type pair(uint16,uint16).
func (b *Bucket) ForEachUint64Uint64ToUint16Uint16(fn func(key1 uint64, key2 uint64, value1 uint16, value2 uint16) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint64uint64(rawKey)
			value1, value2 := bytesTouint16uint16(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetUint64Uint64ToUint16Uint16 is a get with key of type pair(uint64,uint64) and value of type pair(uint16,uint16).
func (b *Bucket) GetUint64Uint64ToUint16Uint16(key1 uint64, key2 uint64) (uint16, uint16) {
	return bytesTouint16uint16(b.Get(uint64uint64Tobytes(key1, key2)))
}

// GetRequiredUint64Uint64ToUint16Uint16 is a get with key of type pair(uint64,uint64) and value of type pair(uint16,uint16) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint64Uint64ToUint16Uint16(key1 uint64, key2 uint64) (uint16, uint16, error) {
	value := b.Get(uint64uint64Tobytes(key1, key2))
	if value == nil {
		return 0, 0, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTouint16uint16(value)
	return value1, value2, nil
}

// PutUint64Uint64ToUint16Uint16 is a put with key of type pair(uint64,uint64) and value of type pair(uint16,uint16).
func (b *Bucket) PutUint64Uint64ToUint16Uint16(key1 uint64, key2 uint64, value1 uint16, value2 uint16) error {
	return b.Put(uint64uint64Tobytes(key1, key2), uint16uint16Tobytes(value1, value2))
}

// FirstUint64Uint64ToUint16Uint16 is first with key of type pair(uint64,uint64) and value of type pair(uint16,uint16).
func (c *Cursor) FirstUint64Uint64ToUint16Uint16() (uint64, uint64, uint16, uint16) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint64uint64(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key1, key2, value1, value2
}

// LastUint64Uint64ToUint16Uint16 is first with key of type pair(uint64,uint64) and value of type pair(uint16,uint16).
func (c *Cursor) LastUint64Uint64ToUint16Uint16() (uint64, uint64, uint16, uint16) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint64uint64(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key1, key2, value1, value2
}

// NextUint64Uint64ToUint16Uint16 is first with key of type pair(uint64,uint64) and value of type pair(uint16,uint16).
func (c *Cursor) NextUint64Uint64ToUint16Uint16() (uint64, uint64, uint16, uint16) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint64uint64(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key1, key2, value1, value2
}

// PrevUint64Uint64ToUint16Uint16 is first with key of type pair(uint64,uint64) and value of type pair(uint16,uint16).
func (c *Cursor) PrevUint64Uint64ToUint16Uint16() (uint64, uint64, uint16, uint16) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint64uint64(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key1, key2, value1, value2
}

// SeekUint64Uint64ToUint16Uint16 is first with key of type pair(uint64,uint64) and value of type pair(uint16,uint16).
func (c *Cursor) SeekUint64Uint64ToUint16Uint16(key1 uint64, key2 uint64) (uint64, uint64, uint16, uint16) {
	rawKey, rawValue := c.Seek(uint64uint64Tobytes(key1, key2))
	key1, key2 = bytesTouint64uint64(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key1, key2, value1, value2
}

// ForEachUint64Uint64ToUint32Uint32 is a for each with key of type pair(uint64,uint64) and value of type pair(uint32,uint32).
func (b *Bucket) ForEachUint64Uint64ToUint32Uint32(fn func(key1 uint64, key2 uint64, value1 uint32, value2 uint32) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint64uint64(rawKey)
			value1, value2 := bytesTouint32uint32(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetUint64Uint64ToUint32Uint32 is a get with key of type pair(uint64,uint64) and value of type pair(uint32,uint32).
func (b *Bucket) GetUint64Uint64ToUint32Uint32(key1 uint64, key2 uint64) (uint32, uint32) {
	return bytesTouint32uint32(b.Get(uint64uint64Tobytes(key1, key2)))
}

// GetRequiredUint64Uint64ToUint32Uint32 is a get with key of type pair(uint64,uint64) and value of type pair(uint32,uint32) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint64Uint64ToUint32Uint32(key1 uint64, key2 uint64) (uint32, uint32, error) {
	value := b.Get(uint64uint64Tobytes(key1, key2))
	if value == nil {
		return 0, 0, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTouint32uint32(value)
	return value1, value2, nil
}

// PutUint64Uint64ToUint32Uint32 is a put with key of type pair(uint64,uint64) and value of type pair(uint32,uint32).
func (b *Bucket) PutUint64Uint64ToUint32Uint32(key1 uint64, key2 uint64, value1 uint32, value2 uint32) error {
	return b.Put(uint64uint64Tobytes(key1, key2), uint32uint32Tobytes(value1, value2))
}

// FirstUint64Uint64ToUint32Uint32 is first with key of type pair(uint64,uint64) and value of type pair(uint32,uint32).
func (c *Cursor) FirstUint64Uint64ToUint32Uint32() (uint64, uint64, uint32, uint32) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint64uint64(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key1, key2, value1, value2
}

// LastUint64Uint64ToUint32Uint32 is first with key of type pair(uint64,uint64) and value of type pair(uint32,uint32).
func (c *Cursor) LastUint64Uint64ToUint32Uint32() (uint64, uint64, uint32, uint32) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint64uint64(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key1, key2, value1, value2
}

// NextUint64Uint64ToUint32Uint32 is first with key of type pair(uint64,uint64) and value of type pair(uint32,uint32).
func (c *Cursor) NextUint64Uint64ToUint32Uint32() (uint64, uint64, uint32, uint32) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint64uint64(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key1, key2, value1, value2
}

// PrevUint64Uint64ToUint32Uint32 is first with key of type pair(uint64,uint64) and value of type pair(uint32,uint32).
func (c *Cursor) PrevUint64Uint64ToUint32Uint32() (uint64, uint64, uint32, uint32) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint64uint64(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key1, key2, value1, value2
}

// SeekUint64Uint64ToUint32Uint32 is first with key of type pair(uint64,uint64) and value of type pair(uint32,uint32).
func (c *Cursor) SeekUint64Uint64ToUint32Uint32(key1 uint64, key2 uint64) (uint64, uint64, uint32, uint32) {
	rawKey, rawValue := c.Seek(uint64uint64Tobytes(key1, key2))
	key1, key2 = bytesTouint64uint64(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key1, key2, value1, value2
}

// ForEachUint64Uint64ToUint64Uint64 is a for each with key of type pair(uint64,uint64) and value of type pair(uint64,uint64).
func (b *Bucket) ForEachUint64Uint64ToUint64Uint64(fn func(key1 uint64, key2 uint64, value1 uint64, value2 uint64) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint64uint64(rawKey)
			value1, value2 := bytesTouint64uint64(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetUint64Uint64ToUint64Uint64 is a get with key of type pair(uint64,uint64) and value of type pair(uint64,uint64).
func (b *Bucket) GetUint64Uint64ToUint64Uint64(key1 uint64, key2 uint64) (uint64, uint64) {
	return bytesTouint64uint64(b.Get(uint64uint64Tobytes(key1, key2)))
}

// GetRequiredUint64Uint64ToUint64Uint64 is a get with key of type pair(uint64,uint64) and value of type pair(uint64,uint64) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint64Uint64ToUint64Uint64(key1 uint64, key2 uint64) (uint64, uint64, error) {
	value := b.Get(uint64uint64Tobytes(key1, key2))
	if value == nil {
		return 0, 0, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTouint64uint64(value)
	return value1, value2, nil
}

// PutUint64Uint64ToUint64Uint64 is a put with key of type pair(uint64,uint64) and value of type pair(uint64,uint64).
func (b *Bucket) PutUint64Uint64ToUint64Uint64(key1 uint64, key2 uint64, value1 uint64, value2 uint64) error {
	return b.Put(uint64uint64Tobytes(key1, key2), uint64uint64Tobytes(value1, value2))
}

// FirstUint64Uint64ToUint64Uint64 is first with key of type pair(uint64,uint64) and value of type pair(uint64,uint64).
func (c *Cursor) FirstUint64Uint64ToUint64Uint64() (uint64, uint64, uint64, uint64) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint64uint64(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key1, key2, value1, value2
}

// LastUint64Uint64ToUint64Uint64 is first with key of type pair(uint64,uint64) and value of type pair(uint64,uint64).
func (c *Cursor) LastUint64Uint64ToUint64Uint64() (uint64, uint64, uint64, uint64) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint64uint64(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key1, key2, value1, value2
}

// NextUint64Uint64ToUint64Uint64 is first with key of type pair(uint64,uint64) and value of type pair(uint64,uint64).
func (c *Cursor) NextUint64Uint64ToUint64Uint64() (uint64, uint64, uint64, uint64) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint64uint64(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key1, key2, value1, value2
}

// PrevUint64Uint64ToUint64Uint64 is first with key of type pair(uint64,uint64) and value of type pair(uint64,uint64).
func (c *Cursor) PrevUint64Uint64ToUint64Uint64() (uint64, uint64, uint64, uint64) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint64uint64(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key1, key2, value1, value2
}

// SeekUint64Uint64ToUint64Uint64 is first with key of type pair(uint64,uint64) and value of type pair(uint64,uint64).
func (c *Cursor) SeekUint64Uint64ToUint64Uint64(key1 uint64, key2 uint64) (uint64, uint64, uint64, uint64) {
	rawKey, rawValue := c.Seek(uint64uint64Tobytes(key1, key2))
	key1, key2 = bytesTouint64uint64(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key1, key2, value1, value2
}

// ForEachUint64Uint64ToUint16Bytes is a for each with key of type pair(uint64,uint64) and value of type pair(uint16,[]byte).
func (b *Bucket) ForEachUint64Uint64ToUint16Bytes(fn func(key1 uint64, key2 uint64, value1 uint16, value2 []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint64uint64(rawKey)
			value1, value2 := bytesTouint16bytes(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetUint64Uint64ToUint16Bytes is a get with key of type pair(uint64,uint64) and value of type pair(uint16,[]byte).
func (b *Bucket) GetUint64Uint64ToUint16Bytes(key1 uint64, key2 uint64) (uint16, []byte) {
	return bytesTouint16bytes(b.Get(uint64uint64Tobytes(key1, key2)))
}

// GetRequiredUint64Uint64ToUint16Bytes is a get with key of type pair(uint64,uint64) and value of type pair(uint16,[]byte) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint64Uint64ToUint16Bytes(key1 uint64, key2 uint64) (uint16, []byte, error) {
	value := b.Get(uint64uint64Tobytes(key1, key2))
	if value == nil {
		return 0, nil, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTouint16bytes(value)
	return value1, value2, nil
}

// PutUint64Uint64ToUint16Bytes is a put with key of type pair(uint64,uint64) and value of type pair(uint16,[]byte).
func (b *Bucket) PutUint64Uint64ToUint16Bytes(key1 uint64, key2 uint64, value1 uint16, value2 []byte) error {
	return b.Put(uint64uint64Tobytes(key1, key2), uint16bytesTobytes(value1, value2))
}

// FirstUint64Uint64ToUint16Bytes is first with key of type pair(uint64,uint64) and value of type pair(uint16,[]byte).
func (c *Cursor) FirstUint64Uint64ToUint16Bytes() (uint64, uint64, uint16, []byte) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint64uint64(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key1, key2, value1, value2
}

// LastUint64Uint64ToUint16Bytes is first with key of type pair(uint64,uint64) and value of type pair(uint16,[]byte).
func (c *Cursor) LastUint64Uint64ToUint16Bytes() (uint64, uint64, uint16, []byte) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint64uint64(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key1, key2, value1, value2
}

// NextUint64Uint64ToUint16Bytes is first with key of type pair(uint64,uint64) and value of type pair(uint16,[]byte).
func (c *Cursor) NextUint64Uint64ToUint16Bytes() (uint64, uint64, uint16, []byte) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint64uint64(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key1, key2, value1, value2
}

// PrevUint64Uint64ToUint16Bytes is first with key of type pair(uint64,uint64) and value of type pair(uint16,[]byte).
func (c *Cursor) PrevUint64Uint64ToUint16Bytes() (uint64, uint64, uint16, []byte) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint64uint64(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key1, key2, value1, value2
}

// SeekUint64Uint64ToUint16Bytes is first with key of type pair(uint64,uint64) and value of type pair(uint16,[]byte).
func (c *Cursor) SeekUint64Uint64ToUint16Bytes(key1 uint64, key2 uint64) (uint64, uint64, uint16, []byte) {
	rawKey, rawValue := c.Seek(uint64uint64Tobytes(key1, key2))
	key1, key2 = bytesTouint64uint64(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key1, key2, value1, value2
}

// ForEachUint64Uint64ToUint32Bytes is a for each with key of type pair(uint64,uint64) and value of type pair(uint32,[]byte).
func (b *Bucket) ForEachUint64Uint64ToUint32Bytes(fn func(key1 uint64, key2 uint64, value1 uint32, value2 []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint64uint64(rawKey)
			value1, value2 := bytesTouint32bytes(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetUint64Uint64ToUint32Bytes is a get with key of type pair(uint64,uint64) and value of type pair(uint32,[]byte).
func (b *Bucket) GetUint64Uint64ToUint32Bytes(key1 uint64, key2 uint64) (uint32, []byte) {
	return bytesTouint32bytes(b.Get(uint64uint64Tobytes(key1, key2)))
}

// GetRequiredUint64Uint64ToUint32Bytes is a get with key of type pair(uint64,uint64) and value of type pair(uint32,[]byte) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint64Uint64ToUint32Bytes(key1 uint64, key2 uint64) (uint32, []byte, error) {
	value := b.Get(uint64uint64Tobytes(key1, key2))
	if value == nil {
		return 0, nil, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTouint32bytes(value)
	return value1, value2, nil
}

// PutUint64Uint64ToUint32Bytes is a put with key of type pair(uint64,uint64) and value of type pair(uint32,[]byte).
func (b *Bucket) PutUint64Uint64ToUint32Bytes(key1 uint64, key2 uint64, value1 uint32, value2 []byte) error {
	return b.Put(uint64uint64Tobytes(key1, key2), uint32bytesTobytes(value1, value2))
}

// FirstUint64Uint64ToUint32Bytes is first with key of type pair(uint64,uint64) and value of type pair(uint32,[]byte).
func (c *Cursor) FirstUint64Uint64ToUint32Bytes() (uint64, uint64, uint32, []byte) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint64uint64(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key1, key2, value1, value2
}

// LastUint64Uint64ToUint32Bytes is first with key of type pair(uint64,uint64) and value of type pair(uint32,[]byte).
func (c *Cursor) LastUint64Uint64ToUint32Bytes() (uint64, uint64, uint32, []byte) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint64uint64(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key1, key2, value1, value2
}

// NextUint64Uint64ToUint32Bytes is first with key of type pair(uint64,uint64) and value of type pair(uint32,[]byte).
func (c *Cursor) NextUint64Uint64ToUint32Bytes() (uint64, uint64, uint32, []byte) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint64uint64(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key1, key2, value1, value2
}

// PrevUint64Uint64ToUint32Bytes is first with key of type pair(uint64,uint64) and value of type pair(uint32,[]byte).
func (c *Cursor) PrevUint64Uint64ToUint32Bytes() (uint64, uint64, uint32, []byte) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint64uint64(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key1, key2, value1, value2
}

// SeekUint64Uint64ToUint32Bytes is first with key of type pair(uint64,uint64) and value of type pair(uint32,[]byte).
func (c *Cursor) SeekUint64Uint64ToUint32Bytes(key1 uint64, key2 uint64) (uint64, uint64, uint32, []byte) {
	rawKey, rawValue := c.Seek(uint64uint64Tobytes(key1, key2))
	key1, key2 = bytesTouint64uint64(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key1, key2, value1, value2
}

// ForEachUint64Uint64ToUint64Bytes is a for each with key of type pair(uint64,uint64) and value of type pair(uint64,[]byte).
func (b *Bucket) ForEachUint64Uint64ToUint64Bytes(fn func(key1 uint64, key2 uint64, value1 uint64, value2 []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint64uint64(rawKey)
			value1, value2 := bytesTouint64bytes(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetUint64Uint64ToUint64Bytes is a get with key of type pair(uint64,uint64) and value of type pair(uint64,[]byte).
func (b *Bucket) GetUint64Uint64ToUint64Bytes(key1 uint64, key2 uint64) (uint64, []byte) {
	return bytesTouint64bytes(b.Get(uint64uint64Tobytes(key1, key2)))
}

// GetRequiredUint64Uint64ToUint64Bytes is a get with key of type pair(uint64,uint64) and value of type pair(uint64,[]byte) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint64Uint64ToUint64Bytes(key1 uint64, key2 uint64) (uint64, []byte, error) {
	value := b.Get(uint64uint64Tobytes(key1, key2))
	if value == nil {
		return 0, nil, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTouint64bytes(value)
	return value1, value2, nil
}

// PutUint64Uint64ToUint64Bytes is a put with key of type pair(uint64,uint64) and value of type pair(uint64,[]byte).
func (b *Bucket) PutUint64Uint64ToUint64Bytes(key1 uint64, key2 uint64, value1 uint64, value2 []byte) error {
	return b.Put(uint64uint64Tobytes(key1, key2), uint64bytesTobytes(value1, value2))
}

// FirstUint64Uint64ToUint64Bytes is first with key of type pair(uint64,uint64) and value of type pair(uint64,[]byte).
func (c *Cursor) FirstUint64Uint64ToUint64Bytes() (uint64, uint64, uint64, []byte) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint64uint64(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key1, key2, value1, value2
}

// LastUint64Uint64ToUint64Bytes is first with key of type pair(uint64,uint64) and value of type pair(uint64,[]byte).
func (c *Cursor) LastUint64Uint64ToUint64Bytes() (uint64, uint64, uint64, []byte) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint64uint64(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key1, key2, value1, value2
}

// NextUint64Uint64ToUint64Bytes is first with key of type pair(uint64,uint64) and value of type pair(uint64,[]byte).
func (c *Cursor) NextUint64Uint64ToUint64Bytes() (uint64, uint64, uint64, []byte) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint64uint64(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key1, key2, value1, value2
}

// PrevUint64Uint64ToUint64Bytes is first with key of type pair(uint64,uint64) and value of type pair(uint64,[]byte).
func (c *Cursor) PrevUint64Uint64ToUint64Bytes() (uint64, uint64, uint64, []byte) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint64uint64(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key1, key2, value1, value2
}

// SeekUint64Uint64ToUint64Bytes is first with key of type pair(uint64,uint64) and value of type pair(uint64,[]byte).
func (c *Cursor) SeekUint64Uint64ToUint64Bytes(key1 uint64, key2 uint64) (uint64, uint64, uint64, []byte) {
	rawKey, rawValue := c.Seek(uint64uint64Tobytes(key1, key2))
	key1, key2 = bytesTouint64uint64(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key1, key2, value1, value2
}

// ForEachUint64Uint64ToBytesUint16 is a for each with key of type pair(uint64,uint64) and value of type pair([]byte,uint16).
func (b *Bucket) ForEachUint64Uint64ToBytesUint16(fn func(key1 uint64, key2 uint64, value1 []byte, value2 uint16) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint64uint64(rawKey)
			value1, value2 := bytesTobytesuint16(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetUint64Uint64ToBytesUint16 is a get with key of type pair(uint64,uint64) and value of type pair([]byte,uint16).
func (b *Bucket) GetUint64Uint64ToBytesUint16(key1 uint64, key2 uint64) ([]byte, uint16) {
	return bytesTobytesuint16(b.Get(uint64uint64Tobytes(key1, key2)))
}

// GetRequiredUint64Uint64ToBytesUint16 is a get with key of type pair(uint64,uint64) and value of type pair([]byte,uint16) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint64Uint64ToBytesUint16(key1 uint64, key2 uint64) ([]byte, uint16, error) {
	value := b.Get(uint64uint64Tobytes(key1, key2))
	if value == nil {
		return nil, 0, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTobytesuint16(value)
	return value1, value2, nil
}

// PutUint64Uint64ToBytesUint16 is a put with key of type pair(uint64,uint64) and value of type pair([]byte,uint16).
func (b *Bucket) PutUint64Uint64ToBytesUint16(key1 uint64, key2 uint64, value1 []byte, value2 uint16) error {
	return b.Put(uint64uint64Tobytes(key1, key2), bytesuint16Tobytes(value1, value2))
}

// FirstUint64Uint64ToBytesUint16 is first with key of type pair(uint64,uint64) and value of type pair([]byte,uint16).
func (c *Cursor) FirstUint64Uint64ToBytesUint16() (uint64, uint64, []byte, uint16) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint64uint64(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key1, key2, value1, value2
}

// LastUint64Uint64ToBytesUint16 is first with key of type pair(uint64,uint64) and value of type pair([]byte,uint16).
func (c *Cursor) LastUint64Uint64ToBytesUint16() (uint64, uint64, []byte, uint16) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint64uint64(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key1, key2, value1, value2
}

// NextUint64Uint64ToBytesUint16 is first with key of type pair(uint64,uint64) and value of type pair([]byte,uint16).
func (c *Cursor) NextUint64Uint64ToBytesUint16() (uint64, uint64, []byte, uint16) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint64uint64(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key1, key2, value1, value2
}

// PrevUint64Uint64ToBytesUint16 is first with key of type pair(uint64,uint64) and value of type pair([]byte,uint16).
func (c *Cursor) PrevUint64Uint64ToBytesUint16() (uint64, uint64, []byte, uint16) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint64uint64(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key1, key2, value1, value2
}

// SeekUint64Uint64ToBytesUint16 is first with key of type pair(uint64,uint64) and value of type pair([]byte,uint16).
func (c *Cursor) SeekUint64Uint64ToBytesUint16(key1 uint64, key2 uint64) (uint64, uint64, []byte, uint16) {
	rawKey, rawValue := c.Seek(uint64uint64Tobytes(key1, key2))
	key1, key2 = bytesTouint64uint64(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key1, key2, value1, value2
}

// ForEachUint64Uint64ToBytesUint32 is a for each with key of type pair(uint64,uint64) and value of type pair([]byte,uint32).
func (b *Bucket) ForEachUint64Uint64ToBytesUint32(fn func(key1 uint64, key2 uint64, value1 []byte, value2 uint32) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint64uint64(rawKey)
			value1, value2 := bytesTobytesuint32(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetUint64Uint64ToBytesUint32 is a get with key of type pair(uint64,uint64) and value of type pair([]byte,uint32).
func (b *Bucket) GetUint64Uint64ToBytesUint32(key1 uint64, key2 uint64) ([]byte, uint32) {
	return bytesTobytesuint32(b.Get(uint64uint64Tobytes(key1, key2)))
}

// GetRequiredUint64Uint64ToBytesUint32 is a get with key of type pair(uint64,uint64) and value of type pair([]byte,uint32) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint64Uint64ToBytesUint32(key1 uint64, key2 uint64) ([]byte, uint32, error) {
	value := b.Get(uint64uint64Tobytes(key1, key2))
	if value == nil {
		return nil, 0, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTobytesuint32(value)
	return value1, value2, nil
}

// PutUint64Uint64ToBytesUint32 is a put with key of type pair(uint64,uint64) and value of type pair([]byte,uint32).
func (b *Bucket) PutUint64Uint64ToBytesUint32(key1 uint64, key2 uint64, value1 []byte, value2 uint32) error {
	return b.Put(uint64uint64Tobytes(key1, key2), bytesuint32Tobytes(value1, value2))
}

// FirstUint64Uint64ToBytesUint32 is first with key of type pair(uint64,uint64) and value of type pair([]byte,uint32).
func (c *Cursor) FirstUint64Uint64ToBytesUint32() (uint64, uint64, []byte, uint32) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint64uint64(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key1, key2, value1, value2
}

// LastUint64Uint64ToBytesUint32 is first with key of type pair(uint64,uint64) and value of type pair([]byte,uint32).
func (c *Cursor) LastUint64Uint64ToBytesUint32() (uint64, uint64, []byte, uint32) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint64uint64(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key1, key2, value1, value2
}

// NextUint64Uint64ToBytesUint32 is first with key of type pair(uint64,uint64) and value of type pair([]byte,uint32).
func (c *Cursor) NextUint64Uint64ToBytesUint32() (uint64, uint64, []byte, uint32) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint64uint64(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key1, key2, value1, value2
}

// PrevUint64Uint64ToBytesUint32 is first with key of type pair(uint64,uint64) and value of type pair([]byte,uint32).
func (c *Cursor) PrevUint64Uint64ToBytesUint32() (uint64, uint64, []byte, uint32) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint64uint64(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key1, key2, value1, value2
}

// SeekUint64Uint64ToBytesUint32 is first with key of type pair(uint64,uint64) and value of type pair([]byte,uint32).
func (c *Cursor) SeekUint64Uint64ToBytesUint32(key1 uint64, key2 uint64) (uint64, uint64, []byte, uint32) {
	rawKey, rawValue := c.Seek(uint64uint64Tobytes(key1, key2))
	key1, key2 = bytesTouint64uint64(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key1, key2, value1, value2
}

// ForEachUint64Uint64ToBytesUint64 is a for each with key of type pair(uint64,uint64) and value of type pair([]byte,uint64).
func (b *Bucket) ForEachUint64Uint64ToBytesUint64(fn func(key1 uint64, key2 uint64, value1 []byte, value2 uint64) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint64uint64(rawKey)
			value1, value2 := bytesTobytesuint64(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetUint64Uint64ToBytesUint64 is a get with key of type pair(uint64,uint64) and value of type pair([]byte,uint64).
func (b *Bucket) GetUint64Uint64ToBytesUint64(key1 uint64, key2 uint64) ([]byte, uint64) {
	return bytesTobytesuint64(b.Get(uint64uint64Tobytes(key1, key2)))
}

// GetRequiredUint64Uint64ToBytesUint64 is a get with key of type pair(uint64,uint64) and value of type pair([]byte,uint64) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint64Uint64ToBytesUint64(key1 uint64, key2 uint64) ([]byte, uint64, error) {
	value := b.Get(uint64uint64Tobytes(key1, key2))
	if value == nil {
		return nil, 0, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTobytesuint64(value)
	return value1, value2, nil
}

// PutUint64Uint64ToBytesUint64 is a put with key of type pair(uint64,uint64) and value of type pair([]byte,uint64).
func (b *Bucket) PutUint64Uint64ToBytesUint64(key1 uint64, key2 uint64, value1 []byte, value2 uint64) error {
	return b.Put(uint64uint64Tobytes(key1, key2), bytesuint64Tobytes(value1, value2))
}

// FirstUint64Uint64ToBytesUint64 is first with key of type pair(uint64,uint64) and value of type pair([]byte,uint64).
func (c *Cursor) FirstUint64Uint64ToBytesUint64() (uint64, uint64, []byte, uint64) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint64uint64(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key1, key2, value1, value2
}

// LastUint64Uint64ToBytesUint64 is first with key of type pair(uint64,uint64) and value of type pair([]byte,uint64).
func (c *Cursor) LastUint64Uint64ToBytesUint64() (uint64, uint64, []byte, uint64) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint64uint64(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key1, key2, value1, value2
}

// NextUint64Uint64ToBytesUint64 is first with key of type pair(uint64,uint64) and value of type pair([]byte,uint64).
func (c *Cursor) NextUint64Uint64ToBytesUint64() (uint64, uint64, []byte, uint64) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint64uint64(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key1, key2, value1, value2
}

// PrevUint64Uint64ToBytesUint64 is first with key of type pair(uint64,uint64) and value of type pair([]byte,uint64).
func (c *Cursor) PrevUint64Uint64ToBytesUint64() (uint64, uint64, []byte, uint64) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint64uint64(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key1, key2, value1, value2
}

// SeekUint64Uint64ToBytesUint64 is first with key of type pair(uint64,uint64) and value of type pair([]byte,uint64).
func (c *Cursor) SeekUint64Uint64ToBytesUint64(key1 uint64, key2 uint64) (uint64, uint64, []byte, uint64) {
	rawKey, rawValue := c.Seek(uint64uint64Tobytes(key1, key2))
	key1, key2 = bytesTouint64uint64(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key1, key2, value1, value2
}

// DeleteUint64Uint64 is a delete with key of type pair(uint64,uint64).
func (b *Bucket) DeleteUint64Uint64(key1 uint64, key2 uint64) error {
	return b.Delete(uint64uint64Tobytes(key1, key2))
}

// ForEachUint16BytesToBytes is a for each with key of type pair(uint16,[]byte) and value of type []byte.
func (b *Bucket) ForEachUint16BytesToBytes(fn func(key1 uint16, key2 []byte, value []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint16bytes(rawKey)
			value := (rawValue)
			return fn(key1, key2, value)
		},
	)
}

// GetUint16BytesToBytes is a get with key of type pair(uint16,[]byte) and value of type []byte.
func (b *Bucket) GetUint16BytesToBytes(key1 uint16, key2 []byte) []byte {
	return (b.Get(uint16bytesTobytes(key1, key2)))
}

// GetRequiredUint16BytesToBytes is a get with key of type pair(uint16,[]byte) and value of type []byte that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint16BytesToBytes(key1 uint16, key2 []byte) ([]byte, error) {
	value := b.Get(uint16bytesTobytes(key1, key2))
	if value == nil {
		return nil, NewKeyNotFoundError(key1, key2)
	}
	return (value), nil
}

// PutUint16BytesToBytes is a put with key of type pair(uint16,[]byte) and value of type []byte.
func (b *Bucket) PutUint16BytesToBytes(key1 uint16, key2 []byte, value []byte) error {
	return b.Put(uint16bytesTobytes(key1, key2), (value))
}

// FirstUint16BytesToBytes is first with key of type pair(uint16,[]byte) and value of type []byte.
func (c *Cursor) FirstUint16BytesToBytes() (uint16, []byte, []byte) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint16bytes(rawKey)
	value := (rawValue)
	return key1, key2, value
}

// LastUint16BytesToBytes is first with key of type pair(uint16,[]byte) and value of type []byte.
func (c *Cursor) LastUint16BytesToBytes() (uint16, []byte, []byte) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint16bytes(rawKey)
	value := (rawValue)
	return key1, key2, value
}

// NextUint16BytesToBytes is first with key of type pair(uint16,[]byte) and value of type []byte.
func (c *Cursor) NextUint16BytesToBytes() (uint16, []byte, []byte) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint16bytes(rawKey)
	value := (rawValue)
	return key1, key2, value
}

// PrevUint16BytesToBytes is first with key of type pair(uint16,[]byte) and value of type []byte.
func (c *Cursor) PrevUint16BytesToBytes() (uint16, []byte, []byte) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint16bytes(rawKey)
	value := (rawValue)
	return key1, key2, value
}

// SeekUint16BytesToBytes is first with key of type pair(uint16,[]byte) and value of type []byte.
func (c *Cursor) SeekUint16BytesToBytes(key1 uint16, key2 []byte) (uint16, []byte, []byte) {
	rawKey, rawValue := c.Seek(uint16bytesTobytes(key1, key2))
	key1, key2 = bytesTouint16bytes(rawKey)
	value := (rawValue)
	return key1, key2, value
}

// ForEachUint16BytesToString is a for each with key of type pair(uint16,[]byte) and value of type string.
func (b *Bucket) ForEachUint16BytesToString(fn func(key1 uint16, key2 []byte, value string) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint16bytes(rawKey)
			value := string(rawValue)
			return fn(key1, key2, value)
		},
	)
}

// GetUint16BytesToString is a get with key of type pair(uint16,[]byte) and value of type string.
func (b *Bucket) GetUint16BytesToString(key1 uint16, key2 []byte) string {
	return string(b.Get(uint16bytesTobytes(key1, key2)))
}

// GetRequiredUint16BytesToString is a get with key of type pair(uint16,[]byte) and value of type string that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint16BytesToString(key1 uint16, key2 []byte) (string, error) {
	value := b.Get(uint16bytesTobytes(key1, key2))
	if value == nil {
		return "", NewKeyNotFoundError(key1, key2)
	}
	return string(value), nil
}

// PutUint16BytesToString is a put with key of type pair(uint16,[]byte) and value of type string.
func (b *Bucket) PutUint16BytesToString(key1 uint16, key2 []byte, value string) error {
	return b.Put(uint16bytesTobytes(key1, key2), []byte(value))
}

// FirstUint16BytesToString is first with key of type pair(uint16,[]byte) and value of type string.
func (c *Cursor) FirstUint16BytesToString() (uint16, []byte, string) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint16bytes(rawKey)
	value := string(rawValue)
	return key1, key2, value
}

// LastUint16BytesToString is first with key of type pair(uint16,[]byte) and value of type string.
func (c *Cursor) LastUint16BytesToString() (uint16, []byte, string) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint16bytes(rawKey)
	value := string(rawValue)
	return key1, key2, value
}

// NextUint16BytesToString is first with key of type pair(uint16,[]byte) and value of type string.
func (c *Cursor) NextUint16BytesToString() (uint16, []byte, string) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint16bytes(rawKey)
	value := string(rawValue)
	return key1, key2, value
}

// PrevUint16BytesToString is first with key of type pair(uint16,[]byte) and value of type string.
func (c *Cursor) PrevUint16BytesToString() (uint16, []byte, string) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint16bytes(rawKey)
	value := string(rawValue)
	return key1, key2, value
}

// SeekUint16BytesToString is first with key of type pair(uint16,[]byte) and value of type string.
func (c *Cursor) SeekUint16BytesToString(key1 uint16, key2 []byte) (uint16, []byte, string) {
	rawKey, rawValue := c.Seek(uint16bytesTobytes(key1, key2))
	key1, key2 = bytesTouint16bytes(rawKey)
	value := string(rawValue)
	return key1, key2, value
}

// ForEachUint16BytesToUint16 is a for each with key of type pair(uint16,[]byte) and value of type uint16.
func (b *Bucket) ForEachUint16BytesToUint16(fn func(key1 uint16, key2 []byte, value uint16) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint16bytes(rawKey)
			value := bytesTouint16(rawValue)
			return fn(key1, key2, value)
		},
	)
}

// GetUint16BytesToUint16 is a get with key of type pair(uint16,[]byte) and value of type uint16.
func (b *Bucket) GetUint16BytesToUint16(key1 uint16, key2 []byte) uint16 {
	return bytesTouint16(b.Get(uint16bytesTobytes(key1, key2)))
}

// GetRequiredUint16BytesToUint16 is a get with key of type pair(uint16,[]byte) and value of type uint16 that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint16BytesToUint16(key1 uint16, key2 []byte) (uint16, error) {
	value := b.Get(uint16bytesTobytes(key1, key2))
	if value == nil {
		return 0, NewKeyNotFoundError(key1, key2)
	}
	return bytesTouint16(value), nil
}

// PutUint16BytesToUint16 is a put with key of type pair(uint16,[]byte) and value of type uint16.
func (b *Bucket) PutUint16BytesToUint16(key1 uint16, key2 []byte, value uint16) error {
	return b.Put(uint16bytesTobytes(key1, key2), uint16Tobytes(value))
}

// FirstUint16BytesToUint16 is first with key of type pair(uint16,[]byte) and value of type uint16.
func (c *Cursor) FirstUint16BytesToUint16() (uint16, []byte, uint16) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint16bytes(rawKey)
	value := bytesTouint16(rawValue)
	return key1, key2, value
}

// LastUint16BytesToUint16 is first with key of type pair(uint16,[]byte) and value of type uint16.
func (c *Cursor) LastUint16BytesToUint16() (uint16, []byte, uint16) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint16bytes(rawKey)
	value := bytesTouint16(rawValue)
	return key1, key2, value
}

// NextUint16BytesToUint16 is first with key of type pair(uint16,[]byte) and value of type uint16.
func (c *Cursor) NextUint16BytesToUint16() (uint16, []byte, uint16) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint16bytes(rawKey)
	value := bytesTouint16(rawValue)
	return key1, key2, value
}

// PrevUint16BytesToUint16 is first with key of type pair(uint16,[]byte) and value of type uint16.
func (c *Cursor) PrevUint16BytesToUint16() (uint16, []byte, uint16) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint16bytes(rawKey)
	value := bytesTouint16(rawValue)
	return key1, key2, value
}

// SeekUint16BytesToUint16 is first with key of type pair(uint16,[]byte) and value of type uint16.
func (c *Cursor) SeekUint16BytesToUint16(key1 uint16, key2 []byte) (uint16, []byte, uint16) {
	rawKey, rawValue := c.Seek(uint16bytesTobytes(key1, key2))
	key1, key2 = bytesTouint16bytes(rawKey)
	value := bytesTouint16(rawValue)
	return key1, key2, value
}

// ForEachUint16BytesToUint32 is a for each with key of type pair(uint16,[]byte) and value of type uint32.
func (b *Bucket) ForEachUint16BytesToUint32(fn func(key1 uint16, key2 []byte, value uint32) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint16bytes(rawKey)
			value := bytesTouint32(rawValue)
			return fn(key1, key2, value)
		},
	)
}

// GetUint16BytesToUint32 is a get with key of type pair(uint16,[]byte) and value of type uint32.
func (b *Bucket) GetUint16BytesToUint32(key1 uint16, key2 []byte) uint32 {
	return bytesTouint32(b.Get(uint16bytesTobytes(key1, key2)))
}

// GetRequiredUint16BytesToUint32 is a get with key of type pair(uint16,[]byte) and value of type uint32 that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint16BytesToUint32(key1 uint16, key2 []byte) (uint32, error) {
	value := b.Get(uint16bytesTobytes(key1, key2))
	if value == nil {
		return 0, NewKeyNotFoundError(key1, key2)
	}
	return bytesTouint32(value), nil
}

// PutUint16BytesToUint32 is a put with key of type pair(uint16,[]byte) and value of type uint32.
func (b *Bucket) PutUint16BytesToUint32(key1 uint16, key2 []byte, value uint32) error {
	return b.Put(uint16bytesTobytes(key1, key2), uint32Tobytes(value))
}

// FirstUint16BytesToUint32 is first with key of type pair(uint16,[]byte) and value of type uint32.
func (c *Cursor) FirstUint16BytesToUint32() (uint16, []byte, uint32) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint16bytes(rawKey)
	value := bytesTouint32(rawValue)
	return key1, key2, value
}

// LastUint16BytesToUint32 is first with key of type pair(uint16,[]byte) and value of type uint32.
func (c *Cursor) LastUint16BytesToUint32() (uint16, []byte, uint32) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint16bytes(rawKey)
	value := bytesTouint32(rawValue)
	return key1, key2, value
}

// NextUint16BytesToUint32 is first with key of type pair(uint16,[]byte) and value of type uint32.
func (c *Cursor) NextUint16BytesToUint32() (uint16, []byte, uint32) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint16bytes(rawKey)
	value := bytesTouint32(rawValue)
	return key1, key2, value
}

// PrevUint16BytesToUint32 is first with key of type pair(uint16,[]byte) and value of type uint32.
func (c *Cursor) PrevUint16BytesToUint32() (uint16, []byte, uint32) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint16bytes(rawKey)
	value := bytesTouint32(rawValue)
	return key1, key2, value
}

// SeekUint16BytesToUint32 is first with key of type pair(uint16,[]byte) and value of type uint32.
func (c *Cursor) SeekUint16BytesToUint32(key1 uint16, key2 []byte) (uint16, []byte, uint32) {
	rawKey, rawValue := c.Seek(uint16bytesTobytes(key1, key2))
	key1, key2 = bytesTouint16bytes(rawKey)
	value := bytesTouint32(rawValue)
	return key1, key2, value
}

// ForEachUint16BytesToUint64 is a for each with key of type pair(uint16,[]byte) and value of type uint64.
func (b *Bucket) ForEachUint16BytesToUint64(fn func(key1 uint16, key2 []byte, value uint64) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint16bytes(rawKey)
			value := bytesTouint64(rawValue)
			return fn(key1, key2, value)
		},
	)
}

// GetUint16BytesToUint64 is a get with key of type pair(uint16,[]byte) and value of type uint64.
func (b *Bucket) GetUint16BytesToUint64(key1 uint16, key2 []byte) uint64 {
	return bytesTouint64(b.Get(uint16bytesTobytes(key1, key2)))
}

// GetRequiredUint16BytesToUint64 is a get with key of type pair(uint16,[]byte) and value of type uint64 that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint16BytesToUint64(key1 uint16, key2 []byte) (uint64, error) {
	value := b.Get(uint16bytesTobytes(key1, key2))
	if value == nil {
		return 0, NewKeyNotFoundError(key1, key2)
	}
	return bytesTouint64(value), nil
}

// PutUint16BytesToUint64 is a put with key of type pair(uint16,[]byte) and value of type uint64.
func (b *Bucket) PutUint16BytesToUint64(key1 uint16, key2 []byte, value uint64) error {
	return b.Put(uint16bytesTobytes(key1, key2), uint64Tobytes(value))
}

// FirstUint16BytesToUint64 is first with key of type pair(uint16,[]byte) and value of type uint64.
func (c *Cursor) FirstUint16BytesToUint64() (uint16, []byte, uint64) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint16bytes(rawKey)
	value := bytesTouint64(rawValue)
	return key1, key2, value
}

// LastUint16BytesToUint64 is first with key of type pair(uint16,[]byte) and value of type uint64.
func (c *Cursor) LastUint16BytesToUint64() (uint16, []byte, uint64) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint16bytes(rawKey)
	value := bytesTouint64(rawValue)
	return key1, key2, value
}

// NextUint16BytesToUint64 is first with key of type pair(uint16,[]byte) and value of type uint64.
func (c *Cursor) NextUint16BytesToUint64() (uint16, []byte, uint64) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint16bytes(rawKey)
	value := bytesTouint64(rawValue)
	return key1, key2, value
}

// PrevUint16BytesToUint64 is first with key of type pair(uint16,[]byte) and value of type uint64.
func (c *Cursor) PrevUint16BytesToUint64() (uint16, []byte, uint64) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint16bytes(rawKey)
	value := bytesTouint64(rawValue)
	return key1, key2, value
}

// SeekUint16BytesToUint64 is first with key of type pair(uint16,[]byte) and value of type uint64.
func (c *Cursor) SeekUint16BytesToUint64(key1 uint16, key2 []byte) (uint16, []byte, uint64) {
	rawKey, rawValue := c.Seek(uint16bytesTobytes(key1, key2))
	key1, key2 = bytesTouint16bytes(rawKey)
	value := bytesTouint64(rawValue)
	return key1, key2, value
}

// ForEachUint16BytesToUint16Uint16 is a for each with key of type pair(uint16,[]byte) and value of type pair(uint16,uint16).
func (b *Bucket) ForEachUint16BytesToUint16Uint16(fn func(key1 uint16, key2 []byte, value1 uint16, value2 uint16) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint16bytes(rawKey)
			value1, value2 := bytesTouint16uint16(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetUint16BytesToUint16Uint16 is a get with key of type pair(uint16,[]byte) and value of type pair(uint16,uint16).
func (b *Bucket) GetUint16BytesToUint16Uint16(key1 uint16, key2 []byte) (uint16, uint16) {
	return bytesTouint16uint16(b.Get(uint16bytesTobytes(key1, key2)))
}

// GetRequiredUint16BytesToUint16Uint16 is a get with key of type pair(uint16,[]byte) and value of type pair(uint16,uint16) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint16BytesToUint16Uint16(key1 uint16, key2 []byte) (uint16, uint16, error) {
	value := b.Get(uint16bytesTobytes(key1, key2))
	if value == nil {
		return 0, 0, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTouint16uint16(value)
	return value1, value2, nil
}

// PutUint16BytesToUint16Uint16 is a put with key of type pair(uint16,[]byte) and value of type pair(uint16,uint16).
func (b *Bucket) PutUint16BytesToUint16Uint16(key1 uint16, key2 []byte, value1 uint16, value2 uint16) error {
	return b.Put(uint16bytesTobytes(key1, key2), uint16uint16Tobytes(value1, value2))
}

// FirstUint16BytesToUint16Uint16 is first with key of type pair(uint16,[]byte) and value of type pair(uint16,uint16).
func (c *Cursor) FirstUint16BytesToUint16Uint16() (uint16, []byte, uint16, uint16) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint16bytes(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key1, key2, value1, value2
}

// LastUint16BytesToUint16Uint16 is first with key of type pair(uint16,[]byte) and value of type pair(uint16,uint16).
func (c *Cursor) LastUint16BytesToUint16Uint16() (uint16, []byte, uint16, uint16) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint16bytes(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key1, key2, value1, value2
}

// NextUint16BytesToUint16Uint16 is first with key of type pair(uint16,[]byte) and value of type pair(uint16,uint16).
func (c *Cursor) NextUint16BytesToUint16Uint16() (uint16, []byte, uint16, uint16) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint16bytes(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key1, key2, value1, value2
}

// PrevUint16BytesToUint16Uint16 is first with key of type pair(uint16,[]byte) and value of type pair(uint16,uint16).
func (c *Cursor) PrevUint16BytesToUint16Uint16() (uint16, []byte, uint16, uint16) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint16bytes(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key1, key2, value1, value2
}

// SeekUint16BytesToUint16Uint16 is first with key of type pair(uint16,[]byte) and value of type pair(uint16,uint16).
func (c *Cursor) SeekUint16BytesToUint16Uint16(key1 uint16, key2 []byte) (uint16, []byte, uint16, uint16) {
	rawKey, rawValue := c.Seek(uint16bytesTobytes(key1, key2))
	key1, key2 = bytesTouint16bytes(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key1, key2, value1, value2
}

// ForEachUint16BytesToUint32Uint32 is a for each with key of type pair(uint16,[]byte) and value of type pair(uint32,uint32).
func (b *Bucket) ForEachUint16BytesToUint32Uint32(fn func(key1 uint16, key2 []byte, value1 uint32, value2 uint32) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint16bytes(rawKey)
			value1, value2 := bytesTouint32uint32(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetUint16BytesToUint32Uint32 is a get with key of type pair(uint16,[]byte) and value of type pair(uint32,uint32).
func (b *Bucket) GetUint16BytesToUint32Uint32(key1 uint16, key2 []byte) (uint32, uint32) {
	return bytesTouint32uint32(b.Get(uint16bytesTobytes(key1, key2)))
}

// GetRequiredUint16BytesToUint32Uint32 is a get with key of type pair(uint16,[]byte) and value of type pair(uint32,uint32) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint16BytesToUint32Uint32(key1 uint16, key2 []byte) (uint32, uint32, error) {
	value := b.Get(uint16bytesTobytes(key1, key2))
	if value == nil {
		return 0, 0, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTouint32uint32(value)
	return value1, value2, nil
}

// PutUint16BytesToUint32Uint32 is a put with key of type pair(uint16,[]byte) and value of type pair(uint32,uint32).
func (b *Bucket) PutUint16BytesToUint32Uint32(key1 uint16, key2 []byte, value1 uint32, value2 uint32) error {
	return b.Put(uint16bytesTobytes(key1, key2), uint32uint32Tobytes(value1, value2))
}

// FirstUint16BytesToUint32Uint32 is first with key of type pair(uint16,[]byte) and value of type pair(uint32,uint32).
func (c *Cursor) FirstUint16BytesToUint32Uint32() (uint16, []byte, uint32, uint32) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint16bytes(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key1, key2, value1, value2
}

// LastUint16BytesToUint32Uint32 is first with key of type pair(uint16,[]byte) and value of type pair(uint32,uint32).
func (c *Cursor) LastUint16BytesToUint32Uint32() (uint16, []byte, uint32, uint32) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint16bytes(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key1, key2, value1, value2
}

// NextUint16BytesToUint32Uint32 is first with key of type pair(uint16,[]byte) and value of type pair(uint32,uint32).
func (c *Cursor) NextUint16BytesToUint32Uint32() (uint16, []byte, uint32, uint32) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint16bytes(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key1, key2, value1, value2
}

// PrevUint16BytesToUint32Uint32 is first with key of type pair(uint16,[]byte) and value of type pair(uint32,uint32).
func (c *Cursor) PrevUint16BytesToUint32Uint32() (uint16, []byte, uint32, uint32) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint16bytes(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key1, key2, value1, value2
}

// SeekUint16BytesToUint32Uint32 is first with key of type pair(uint16,[]byte) and value of type pair(uint32,uint32).
func (c *Cursor) SeekUint16BytesToUint32Uint32(key1 uint16, key2 []byte) (uint16, []byte, uint32, uint32) {
	rawKey, rawValue := c.Seek(uint16bytesTobytes(key1, key2))
	key1, key2 = bytesTouint16bytes(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key1, key2, value1, value2
}

// ForEachUint16BytesToUint64Uint64 is a for each with key of type pair(uint16,[]byte) and value of type pair(uint64,uint64).
func (b *Bucket) ForEachUint16BytesToUint64Uint64(fn func(key1 uint16, key2 []byte, value1 uint64, value2 uint64) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint16bytes(rawKey)
			value1, value2 := bytesTouint64uint64(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetUint16BytesToUint64Uint64 is a get with key of type pair(uint16,[]byte) and value of type pair(uint64,uint64).
func (b *Bucket) GetUint16BytesToUint64Uint64(key1 uint16, key2 []byte) (uint64, uint64) {
	return bytesTouint64uint64(b.Get(uint16bytesTobytes(key1, key2)))
}

// GetRequiredUint16BytesToUint64Uint64 is a get with key of type pair(uint16,[]byte) and value of type pair(uint64,uint64) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint16BytesToUint64Uint64(key1 uint16, key2 []byte) (uint64, uint64, error) {
	value := b.Get(uint16bytesTobytes(key1, key2))
	if value == nil {
		return 0, 0, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTouint64uint64(value)
	return value1, value2, nil
}

// PutUint16BytesToUint64Uint64 is a put with key of type pair(uint16,[]byte) and value of type pair(uint64,uint64).
func (b *Bucket) PutUint16BytesToUint64Uint64(key1 uint16, key2 []byte, value1 uint64, value2 uint64) error {
	return b.Put(uint16bytesTobytes(key1, key2), uint64uint64Tobytes(value1, value2))
}

// FirstUint16BytesToUint64Uint64 is first with key of type pair(uint16,[]byte) and value of type pair(uint64,uint64).
func (c *Cursor) FirstUint16BytesToUint64Uint64() (uint16, []byte, uint64, uint64) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint16bytes(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key1, key2, value1, value2
}

// LastUint16BytesToUint64Uint64 is first with key of type pair(uint16,[]byte) and value of type pair(uint64,uint64).
func (c *Cursor) LastUint16BytesToUint64Uint64() (uint16, []byte, uint64, uint64) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint16bytes(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key1, key2, value1, value2
}

// NextUint16BytesToUint64Uint64 is first with key of type pair(uint16,[]byte) and value of type pair(uint64,uint64).
func (c *Cursor) NextUint16BytesToUint64Uint64() (uint16, []byte, uint64, uint64) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint16bytes(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key1, key2, value1, value2
}

// PrevUint16BytesToUint64Uint64 is first with key of type pair(uint16,[]byte) and value of type pair(uint64,uint64).
func (c *Cursor) PrevUint16BytesToUint64Uint64() (uint16, []byte, uint64, uint64) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint16bytes(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key1, key2, value1, value2
}

// SeekUint16BytesToUint64Uint64 is first with key of type pair(uint16,[]byte) and value of type pair(uint64,uint64).
func (c *Cursor) SeekUint16BytesToUint64Uint64(key1 uint16, key2 []byte) (uint16, []byte, uint64, uint64) {
	rawKey, rawValue := c.Seek(uint16bytesTobytes(key1, key2))
	key1, key2 = bytesTouint16bytes(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key1, key2, value1, value2
}

// ForEachUint16BytesToUint16Bytes is a for each with key of type pair(uint16,[]byte) and value of type pair(uint16,[]byte).
func (b *Bucket) ForEachUint16BytesToUint16Bytes(fn func(key1 uint16, key2 []byte, value1 uint16, value2 []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint16bytes(rawKey)
			value1, value2 := bytesTouint16bytes(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetUint16BytesToUint16Bytes is a get with key of type pair(uint16,[]byte) and value of type pair(uint16,[]byte).
func (b *Bucket) GetUint16BytesToUint16Bytes(key1 uint16, key2 []byte) (uint16, []byte) {
	return bytesTouint16bytes(b.Get(uint16bytesTobytes(key1, key2)))
}

// GetRequiredUint16BytesToUint16Bytes is a get with key of type pair(uint16,[]byte) and value of type pair(uint16,[]byte) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint16BytesToUint16Bytes(key1 uint16, key2 []byte) (uint16, []byte, error) {
	value := b.Get(uint16bytesTobytes(key1, key2))
	if value == nil {
		return 0, nil, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTouint16bytes(value)
	return value1, value2, nil
}

// PutUint16BytesToUint16Bytes is a put with key of type pair(uint16,[]byte) and value of type pair(uint16,[]byte).
func (b *Bucket) PutUint16BytesToUint16Bytes(key1 uint16, key2 []byte, value1 uint16, value2 []byte) error {
	return b.Put(uint16bytesTobytes(key1, key2), uint16bytesTobytes(value1, value2))
}

// FirstUint16BytesToUint16Bytes is first with key of type pair(uint16,[]byte) and value of type pair(uint16,[]byte).
func (c *Cursor) FirstUint16BytesToUint16Bytes() (uint16, []byte, uint16, []byte) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint16bytes(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key1, key2, value1, value2
}

// LastUint16BytesToUint16Bytes is first with key of type pair(uint16,[]byte) and value of type pair(uint16,[]byte).
func (c *Cursor) LastUint16BytesToUint16Bytes() (uint16, []byte, uint16, []byte) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint16bytes(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key1, key2, value1, value2
}

// NextUint16BytesToUint16Bytes is first with key of type pair(uint16,[]byte) and value of type pair(uint16,[]byte).
func (c *Cursor) NextUint16BytesToUint16Bytes() (uint16, []byte, uint16, []byte) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint16bytes(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key1, key2, value1, value2
}

// PrevUint16BytesToUint16Bytes is first with key of type pair(uint16,[]byte) and value of type pair(uint16,[]byte).
func (c *Cursor) PrevUint16BytesToUint16Bytes() (uint16, []byte, uint16, []byte) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint16bytes(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key1, key2, value1, value2
}

// SeekUint16BytesToUint16Bytes is first with key of type pair(uint16,[]byte) and value of type pair(uint16,[]byte).
func (c *Cursor) SeekUint16BytesToUint16Bytes(key1 uint16, key2 []byte) (uint16, []byte, uint16, []byte) {
	rawKey, rawValue := c.Seek(uint16bytesTobytes(key1, key2))
	key1, key2 = bytesTouint16bytes(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key1, key2, value1, value2
}

// ForEachUint16BytesToUint32Bytes is a for each with key of type pair(uint16,[]byte) and value of type pair(uint32,[]byte).
func (b *Bucket) ForEachUint16BytesToUint32Bytes(fn func(key1 uint16, key2 []byte, value1 uint32, value2 []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint16bytes(rawKey)
			value1, value2 := bytesTouint32bytes(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetUint16BytesToUint32Bytes is a get with key of type pair(uint16,[]byte) and value of type pair(uint32,[]byte).
func (b *Bucket) GetUint16BytesToUint32Bytes(key1 uint16, key2 []byte) (uint32, []byte) {
	return bytesTouint32bytes(b.Get(uint16bytesTobytes(key1, key2)))
}

// GetRequiredUint16BytesToUint32Bytes is a get with key of type pair(uint16,[]byte) and value of type pair(uint32,[]byte) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint16BytesToUint32Bytes(key1 uint16, key2 []byte) (uint32, []byte, error) {
	value := b.Get(uint16bytesTobytes(key1, key2))
	if value == nil {
		return 0, nil, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTouint32bytes(value)
	return value1, value2, nil
}

// PutUint16BytesToUint32Bytes is a put with key of type pair(uint16,[]byte) and value of type pair(uint32,[]byte).
func (b *Bucket) PutUint16BytesToUint32Bytes(key1 uint16, key2 []byte, value1 uint32, value2 []byte) error {
	return b.Put(uint16bytesTobytes(key1, key2), uint32bytesTobytes(value1, value2))
}

// FirstUint16BytesToUint32Bytes is first with key of type pair(uint16,[]byte) and value of type pair(uint32,[]byte).
func (c *Cursor) FirstUint16BytesToUint32Bytes() (uint16, []byte, uint32, []byte) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint16bytes(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key1, key2, value1, value2
}

// LastUint16BytesToUint32Bytes is first with key of type pair(uint16,[]byte) and value of type pair(uint32,[]byte).
func (c *Cursor) LastUint16BytesToUint32Bytes() (uint16, []byte, uint32, []byte) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint16bytes(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key1, key2, value1, value2
}

// NextUint16BytesToUint32Bytes is first with key of type pair(uint16,[]byte) and value of type pair(uint32,[]byte).
func (c *Cursor) NextUint16BytesToUint32Bytes() (uint16, []byte, uint32, []byte) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint16bytes(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key1, key2, value1, value2
}

// PrevUint16BytesToUint32Bytes is first with key of type pair(uint16,[]byte) and value of type pair(uint32,[]byte).
func (c *Cursor) PrevUint16BytesToUint32Bytes() (uint16, []byte, uint32, []byte) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint16bytes(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key1, key2, value1, value2
}

// SeekUint16BytesToUint32Bytes is first with key of type pair(uint16,[]byte) and value of type pair(uint32,[]byte).
func (c *Cursor) SeekUint16BytesToUint32Bytes(key1 uint16, key2 []byte) (uint16, []byte, uint32, []byte) {
	rawKey, rawValue := c.Seek(uint16bytesTobytes(key1, key2))
	key1, key2 = bytesTouint16bytes(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key1, key2, value1, value2
}

// ForEachUint16BytesToUint64Bytes is a for each with key of type pair(uint16,[]byte) and value of type pair(uint64,[]byte).
func (b *Bucket) ForEachUint16BytesToUint64Bytes(fn func(key1 uint16, key2 []byte, value1 uint64, value2 []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint16bytes(rawKey)
			value1, value2 := bytesTouint64bytes(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetUint16BytesToUint64Bytes is a get with key of type pair(uint16,[]byte) and value of type pair(uint64,[]byte).
func (b *Bucket) GetUint16BytesToUint64Bytes(key1 uint16, key2 []byte) (uint64, []byte) {
	return bytesTouint64bytes(b.Get(uint16bytesTobytes(key1, key2)))
}

// GetRequiredUint16BytesToUint64Bytes is a get with key of type pair(uint16,[]byte) and value of type pair(uint64,[]byte) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint16BytesToUint64Bytes(key1 uint16, key2 []byte) (uint64, []byte, error) {
	value := b.Get(uint16bytesTobytes(key1, key2))
	if value == nil {
		return 0, nil, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTouint64bytes(value)
	return value1, value2, nil
}

// PutUint16BytesToUint64Bytes is a put with key of type pair(uint16,[]byte) and value of type pair(uint64,[]byte).
func (b *Bucket) PutUint16BytesToUint64Bytes(key1 uint16, key2 []byte, value1 uint64, value2 []byte) error {
	return b.Put(uint16bytesTobytes(key1, key2), uint64bytesTobytes(value1, value2))
}

// FirstUint16BytesToUint64Bytes is first with key of type pair(uint16,[]byte) and value of type pair(uint64,[]byte).
func (c *Cursor) FirstUint16BytesToUint64Bytes() (uint16, []byte, uint64, []byte) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint16bytes(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key1, key2, value1, value2
}

// LastUint16BytesToUint64Bytes is first with key of type pair(uint16,[]byte) and value of type pair(uint64,[]byte).
func (c *Cursor) LastUint16BytesToUint64Bytes() (uint16, []byte, uint64, []byte) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint16bytes(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key1, key2, value1, value2
}

// NextUint16BytesToUint64Bytes is first with key of type pair(uint16,[]byte) and value of type pair(uint64,[]byte).
func (c *Cursor) NextUint16BytesToUint64Bytes() (uint16, []byte, uint64, []byte) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint16bytes(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key1, key2, value1, value2
}

// PrevUint16BytesToUint64Bytes is first with key of type pair(uint16,[]byte) and value of type pair(uint64,[]byte).
func (c *Cursor) PrevUint16BytesToUint64Bytes() (uint16, []byte, uint64, []byte) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint16bytes(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key1, key2, value1, value2
}

// SeekUint16BytesToUint64Bytes is first with key of type pair(uint16,[]byte) and value of type pair(uint64,[]byte).
func (c *Cursor) SeekUint16BytesToUint64Bytes(key1 uint16, key2 []byte) (uint16, []byte, uint64, []byte) {
	rawKey, rawValue := c.Seek(uint16bytesTobytes(key1, key2))
	key1, key2 = bytesTouint16bytes(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key1, key2, value1, value2
}

// ForEachUint16BytesToBytesUint16 is a for each with key of type pair(uint16,[]byte) and value of type pair([]byte,uint16).
func (b *Bucket) ForEachUint16BytesToBytesUint16(fn func(key1 uint16, key2 []byte, value1 []byte, value2 uint16) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint16bytes(rawKey)
			value1, value2 := bytesTobytesuint16(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetUint16BytesToBytesUint16 is a get with key of type pair(uint16,[]byte) and value of type pair([]byte,uint16).
func (b *Bucket) GetUint16BytesToBytesUint16(key1 uint16, key2 []byte) ([]byte, uint16) {
	return bytesTobytesuint16(b.Get(uint16bytesTobytes(key1, key2)))
}

// GetRequiredUint16BytesToBytesUint16 is a get with key of type pair(uint16,[]byte) and value of type pair([]byte,uint16) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint16BytesToBytesUint16(key1 uint16, key2 []byte) ([]byte, uint16, error) {
	value := b.Get(uint16bytesTobytes(key1, key2))
	if value == nil {
		return nil, 0, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTobytesuint16(value)
	return value1, value2, nil
}

// PutUint16BytesToBytesUint16 is a put with key of type pair(uint16,[]byte) and value of type pair([]byte,uint16).
func (b *Bucket) PutUint16BytesToBytesUint16(key1 uint16, key2 []byte, value1 []byte, value2 uint16) error {
	return b.Put(uint16bytesTobytes(key1, key2), bytesuint16Tobytes(value1, value2))
}

// FirstUint16BytesToBytesUint16 is first with key of type pair(uint16,[]byte) and value of type pair([]byte,uint16).
func (c *Cursor) FirstUint16BytesToBytesUint16() (uint16, []byte, []byte, uint16) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint16bytes(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key1, key2, value1, value2
}

// LastUint16BytesToBytesUint16 is first with key of type pair(uint16,[]byte) and value of type pair([]byte,uint16).
func (c *Cursor) LastUint16BytesToBytesUint16() (uint16, []byte, []byte, uint16) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint16bytes(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key1, key2, value1, value2
}

// NextUint16BytesToBytesUint16 is first with key of type pair(uint16,[]byte) and value of type pair([]byte,uint16).
func (c *Cursor) NextUint16BytesToBytesUint16() (uint16, []byte, []byte, uint16) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint16bytes(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key1, key2, value1, value2
}

// PrevUint16BytesToBytesUint16 is first with key of type pair(uint16,[]byte) and value of type pair([]byte,uint16).
func (c *Cursor) PrevUint16BytesToBytesUint16() (uint16, []byte, []byte, uint16) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint16bytes(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key1, key2, value1, value2
}

// SeekUint16BytesToBytesUint16 is first with key of type pair(uint16,[]byte) and value of type pair([]byte,uint16).
func (c *Cursor) SeekUint16BytesToBytesUint16(key1 uint16, key2 []byte) (uint16, []byte, []byte, uint16) {
	rawKey, rawValue := c.Seek(uint16bytesTobytes(key1, key2))
	key1, key2 = bytesTouint16bytes(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key1, key2, value1, value2
}

// ForEachUint16BytesToBytesUint32 is a for each with key of type pair(uint16,[]byte) and value of type pair([]byte,uint32).
func (b *Bucket) ForEachUint16BytesToBytesUint32(fn func(key1 uint16, key2 []byte, value1 []byte, value2 uint32) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint16bytes(rawKey)
			value1, value2 := bytesTobytesuint32(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetUint16BytesToBytesUint32 is a get with key of type pair(uint16,[]byte) and value of type pair([]byte,uint32).
func (b *Bucket) GetUint16BytesToBytesUint32(key1 uint16, key2 []byte) ([]byte, uint32) {
	return bytesTobytesuint32(b.Get(uint16bytesTobytes(key1, key2)))
}

// GetRequiredUint16BytesToBytesUint32 is a get with key of type pair(uint16,[]byte) and value of type pair([]byte,uint32) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint16BytesToBytesUint32(key1 uint16, key2 []byte) ([]byte, uint32, error) {
	value := b.Get(uint16bytesTobytes(key1, key2))
	if value == nil {
		return nil, 0, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTobytesuint32(value)
	return value1, value2, nil
}

// PutUint16BytesToBytesUint32 is a put with key of type pair(uint16,[]byte) and value of type pair([]byte,uint32).
func (b *Bucket) PutUint16BytesToBytesUint32(key1 uint16, key2 []byte, value1 []byte, value2 uint32) error {
	return b.Put(uint16bytesTobytes(key1, key2), bytesuint32Tobytes(value1, value2))
}

// FirstUint16BytesToBytesUint32 is first with key of type pair(uint16,[]byte) and value of type pair([]byte,uint32).
func (c *Cursor) FirstUint16BytesToBytesUint32() (uint16, []byte, []byte, uint32) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint16bytes(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key1, key2, value1, value2
}

// LastUint16BytesToBytesUint32 is first with key of type pair(uint16,[]byte) and value of type pair([]byte,uint32).
func (c *Cursor) LastUint16BytesToBytesUint32() (uint16, []byte, []byte, uint32) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint16bytes(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key1, key2, value1, value2
}

// NextUint16BytesToBytesUint32 is first with key of type pair(uint16,[]byte) and value of type pair([]byte,uint32).
func (c *Cursor) NextUint16BytesToBytesUint32() (uint16, []byte, []byte, uint32) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint16bytes(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key1, key2, value1, value2
}

// PrevUint16BytesToBytesUint32 is first with key of type pair(uint16,[]byte) and value of type pair([]byte,uint32).
func (c *Cursor) PrevUint16BytesToBytesUint32() (uint16, []byte, []byte, uint32) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint16bytes(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key1, key2, value1, value2
}

// SeekUint16BytesToBytesUint32 is first with key of type pair(uint16,[]byte) and value of type pair([]byte,uint32).
func (c *Cursor) SeekUint16BytesToBytesUint32(key1 uint16, key2 []byte) (uint16, []byte, []byte, uint32) {
	rawKey, rawValue := c.Seek(uint16bytesTobytes(key1, key2))
	key1, key2 = bytesTouint16bytes(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key1, key2, value1, value2
}

// ForEachUint16BytesToBytesUint64 is a for each with key of type pair(uint16,[]byte) and value of type pair([]byte,uint64).
func (b *Bucket) ForEachUint16BytesToBytesUint64(fn func(key1 uint16, key2 []byte, value1 []byte, value2 uint64) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint16bytes(rawKey)
			value1, value2 := bytesTobytesuint64(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetUint16BytesToBytesUint64 is a get with key of type pair(uint16,[]byte) and value of type pair([]byte,uint64).
func (b *Bucket) GetUint16BytesToBytesUint64(key1 uint16, key2 []byte) ([]byte, uint64) {
	return bytesTobytesuint64(b.Get(uint16bytesTobytes(key1, key2)))
}

// GetRequiredUint16BytesToBytesUint64 is a get with key of type pair(uint16,[]byte) and value of type pair([]byte,uint64) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint16BytesToBytesUint64(key1 uint16, key2 []byte) ([]byte, uint64, error) {
	value := b.Get(uint16bytesTobytes(key1, key2))
	if value == nil {
		return nil, 0, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTobytesuint64(value)
	return value1, value2, nil
}

// PutUint16BytesToBytesUint64 is a put with key of type pair(uint16,[]byte) and value of type pair([]byte,uint64).
func (b *Bucket) PutUint16BytesToBytesUint64(key1 uint16, key2 []byte, value1 []byte, value2 uint64) error {
	return b.Put(uint16bytesTobytes(key1, key2), bytesuint64Tobytes(value1, value2))
}

// FirstUint16BytesToBytesUint64 is first with key of type pair(uint16,[]byte) and value of type pair([]byte,uint64).
func (c *Cursor) FirstUint16BytesToBytesUint64() (uint16, []byte, []byte, uint64) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint16bytes(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key1, key2, value1, value2
}

// LastUint16BytesToBytesUint64 is first with key of type pair(uint16,[]byte) and value of type pair([]byte,uint64).
func (c *Cursor) LastUint16BytesToBytesUint64() (uint16, []byte, []byte, uint64) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint16bytes(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key1, key2, value1, value2
}

// NextUint16BytesToBytesUint64 is first with key of type pair(uint16,[]byte) and value of type pair([]byte,uint64).
func (c *Cursor) NextUint16BytesToBytesUint64() (uint16, []byte, []byte, uint64) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint16bytes(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key1, key2, value1, value2
}

// PrevUint16BytesToBytesUint64 is first with key of type pair(uint16,[]byte) and value of type pair([]byte,uint64).
func (c *Cursor) PrevUint16BytesToBytesUint64() (uint16, []byte, []byte, uint64) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint16bytes(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key1, key2, value1, value2
}

// SeekUint16BytesToBytesUint64 is first with key of type pair(uint16,[]byte) and value of type pair([]byte,uint64).
func (c *Cursor) SeekUint16BytesToBytesUint64(key1 uint16, key2 []byte) (uint16, []byte, []byte, uint64) {
	rawKey, rawValue := c.Seek(uint16bytesTobytes(key1, key2))
	key1, key2 = bytesTouint16bytes(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key1, key2, value1, value2
}

// DeleteUint16Bytes is a delete with key of type pair(uint16,[]byte).
func (b *Bucket) DeleteUint16Bytes(key1 uint16, key2 []byte) error {
	return b.Delete(uint16bytesTobytes(key1, key2))
}

// ForEachUint32BytesToBytes is a for each with key of type pair(uint32,[]byte) and value of type []byte.
func (b *Bucket) ForEachUint32BytesToBytes(fn func(key1 uint32, key2 []byte, value []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint32bytes(rawKey)
			value := (rawValue)
			return fn(key1, key2, value)
		},
	)
}

// GetUint32BytesToBytes is a get with key of type pair(uint32,[]byte) and value of type []byte.
func (b *Bucket) GetUint32BytesToBytes(key1 uint32, key2 []byte) []byte {
	return (b.Get(uint32bytesTobytes(key1, key2)))
}

// GetRequiredUint32BytesToBytes is a get with key of type pair(uint32,[]byte) and value of type []byte that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint32BytesToBytes(key1 uint32, key2 []byte) ([]byte, error) {
	value := b.Get(uint32bytesTobytes(key1, key2))
	if value == nil {
		return nil, NewKeyNotFoundError(key1, key2)
	}
	return (value), nil
}

// PutUint32BytesToBytes is a put with key of type pair(uint32,[]byte) and value of type []byte.
func (b *Bucket) PutUint32BytesToBytes(key1 uint32, key2 []byte, value []byte) error {
	return b.Put(uint32bytesTobytes(key1, key2), (value))
}

// FirstUint32BytesToBytes is first with key of type pair(uint32,[]byte) and value of type []byte.
func (c *Cursor) FirstUint32BytesToBytes() (uint32, []byte, []byte) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint32bytes(rawKey)
	value := (rawValue)
	return key1, key2, value
}

// LastUint32BytesToBytes is first with key of type pair(uint32,[]byte) and value of type []byte.
func (c *Cursor) LastUint32BytesToBytes() (uint32, []byte, []byte) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint32bytes(rawKey)
	value := (rawValue)
	return key1, key2, value
}

// NextUint32BytesToBytes is first with key of type pair(uint32,[]byte) and value of type []byte.
func (c *Cursor) NextUint32BytesToBytes() (uint32, []byte, []byte) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint32bytes(rawKey)
	value := (rawValue)
	return key1, key2, value
}

// PrevUint32BytesToBytes is first with key of type pair(uint32,[]byte) and value of type []byte.
func (c *Cursor) PrevUint32BytesToBytes() (uint32, []byte, []byte) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint32bytes(rawKey)
	value := (rawValue)
	return key1, key2, value
}

// SeekUint32BytesToBytes is first with key of type pair(uint32,[]byte) and value of type []byte.
func (c *Cursor) SeekUint32BytesToBytes(key1 uint32, key2 []byte) (uint32, []byte, []byte) {
	rawKey, rawValue := c.Seek(uint32bytesTobytes(key1, key2))
	key1, key2 = bytesTouint32bytes(rawKey)
	value := (rawValue)
	return key1, key2, value
}

// ForEachUint32BytesToString is a for each with key of type pair(uint32,[]byte) and value of type string.
func (b *Bucket) ForEachUint32BytesToString(fn func(key1 uint32, key2 []byte, value string) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint32bytes(rawKey)
			value := string(rawValue)
			return fn(key1, key2, value)
		},
	)
}

// GetUint32BytesToString is a get with key of type pair(uint32,[]byte) and value of type string.
func (b *Bucket) GetUint32BytesToString(key1 uint32, key2 []byte) string {
	return string(b.Get(uint32bytesTobytes(key1, key2)))
}

// GetRequiredUint32BytesToString is a get with key of type pair(uint32,[]byte) and value of type string that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint32BytesToString(key1 uint32, key2 []byte) (string, error) {
	value := b.Get(uint32bytesTobytes(key1, key2))
	if value == nil {
		return "", NewKeyNotFoundError(key1, key2)
	}
	return string(value), nil
}

// PutUint32BytesToString is a put with key of type pair(uint32,[]byte) and value of type string.
func (b *Bucket) PutUint32BytesToString(key1 uint32, key2 []byte, value string) error {
	return b.Put(uint32bytesTobytes(key1, key2), []byte(value))
}

// FirstUint32BytesToString is first with key of type pair(uint32,[]byte) and value of type string.
func (c *Cursor) FirstUint32BytesToString() (uint32, []byte, string) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint32bytes(rawKey)
	value := string(rawValue)
	return key1, key2, value
}

// LastUint32BytesToString is first with key of type pair(uint32,[]byte) and value of type string.
func (c *Cursor) LastUint32BytesToString() (uint32, []byte, string) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint32bytes(rawKey)
	value := string(rawValue)
	return key1, key2, value
}

// NextUint32BytesToString is first with key of type pair(uint32,[]byte) and value of type string.
func (c *Cursor) NextUint32BytesToString() (uint32, []byte, string) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint32bytes(rawKey)
	value := string(rawValue)
	return key1, key2, value
}

// PrevUint32BytesToString is first with key of type pair(uint32,[]byte) and value of type string.
func (c *Cursor) PrevUint32BytesToString() (uint32, []byte, string) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint32bytes(rawKey)
	value := string(rawValue)
	return key1, key2, value
}

// SeekUint32BytesToString is first with key of type pair(uint32,[]byte) and value of type string.
func (c *Cursor) SeekUint32BytesToString(key1 uint32, key2 []byte) (uint32, []byte, string) {
	rawKey, rawValue := c.Seek(uint32bytesTobytes(key1, key2))
	key1, key2 = bytesTouint32bytes(rawKey)
	value := string(rawValue)
	return key1, key2, value
}

// ForEachUint32BytesToUint16 is a for each with key of type pair(uint32,[]byte) and value of type uint16.
func (b *Bucket) ForEachUint32BytesToUint16(fn func(key1 uint32, key2 []byte, value uint16) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint32bytes(rawKey)
			value := bytesTouint16(rawValue)
			return fn(key1, key2, value)
		},
	)
}

// GetUint32BytesToUint16 is a get with key of type pair(uint32,[]byte) and value of type uint16.
func (b *Bucket) GetUint32BytesToUint16(key1 uint32, key2 []byte) uint16 {
	return bytesTouint16(b.Get(uint32bytesTobytes(key1, key2)))
}

// GetRequiredUint32BytesToUint16 is a get with key of type pair(uint32,[]byte) and value of type uint16 that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint32BytesToUint16(key1 uint32, key2 []byte) (uint16, error) {
	value := b.Get(uint32bytesTobytes(key1, key2))
	if value == nil {
		return 0, NewKeyNotFoundError(key1, key2)
	}
	return bytesTouint16(value), nil
}

// PutUint32BytesToUint16 is a put with key of type pair(uint32,[]byte) and value of type uint16.
func (b *Bucket) PutUint32BytesToUint16(key1 uint32, key2 []byte, value uint16) error {
	return b.Put(uint32bytesTobytes(key1, key2), uint16Tobytes(value))
}

// FirstUint32BytesToUint16 is first with key of type pair(uint32,[]byte) and value of type uint16.
func (c *Cursor) FirstUint32BytesToUint16() (uint32, []byte, uint16) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint32bytes(rawKey)
	value := bytesTouint16(rawValue)
	return key1, key2, value
}

// LastUint32BytesToUint16 is first with key of type pair(uint32,[]byte) and value of type uint16.
func (c *Cursor) LastUint32BytesToUint16() (uint32, []byte, uint16) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint32bytes(rawKey)
	value := bytesTouint16(rawValue)
	return key1, key2, value
}

// NextUint32BytesToUint16 is first with key of type pair(uint32,[]byte) and value of type uint16.
func (c *Cursor) NextUint32BytesToUint16() (uint32, []byte, uint16) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint32bytes(rawKey)
	value := bytesTouint16(rawValue)
	return key1, key2, value
}

// PrevUint32BytesToUint16 is first with key of type pair(uint32,[]byte) and value of type uint16.
func (c *Cursor) PrevUint32BytesToUint16() (uint32, []byte, uint16) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint32bytes(rawKey)
	value := bytesTouint16(rawValue)
	return key1, key2, value
}

// SeekUint32BytesToUint16 is first with key of type pair(uint32,[]byte) and value of type uint16.
func (c *Cursor) SeekUint32BytesToUint16(key1 uint32, key2 []byte) (uint32, []byte, uint16) {
	rawKey, rawValue := c.Seek(uint32bytesTobytes(key1, key2))
	key1, key2 = bytesTouint32bytes(rawKey)
	value := bytesTouint16(rawValue)
	return key1, key2, value
}

// ForEachUint32BytesToUint32 is a for each with key of type pair(uint32,[]byte) and value of type uint32.
func (b *Bucket) ForEachUint32BytesToUint32(fn func(key1 uint32, key2 []byte, value uint32) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint32bytes(rawKey)
			value := bytesTouint32(rawValue)
			return fn(key1, key2, value)
		},
	)
}

// GetUint32BytesToUint32 is a get with key of type pair(uint32,[]byte) and value of type uint32.
func (b *Bucket) GetUint32BytesToUint32(key1 uint32, key2 []byte) uint32 {
	return bytesTouint32(b.Get(uint32bytesTobytes(key1, key2)))
}

// GetRequiredUint32BytesToUint32 is a get with key of type pair(uint32,[]byte) and value of type uint32 that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint32BytesToUint32(key1 uint32, key2 []byte) (uint32, error) {
	value := b.Get(uint32bytesTobytes(key1, key2))
	if value == nil {
		return 0, NewKeyNotFoundError(key1, key2)
	}
	return bytesTouint32(value), nil
}

// PutUint32BytesToUint32 is a put with key of type pair(uint32,[]byte) and value of type uint32.
func (b *Bucket) PutUint32BytesToUint32(key1 uint32, key2 []byte, value uint32) error {
	return b.Put(uint32bytesTobytes(key1, key2), uint32Tobytes(value))
}

// FirstUint32BytesToUint32 is first with key of type pair(uint32,[]byte) and value of type uint32.
func (c *Cursor) FirstUint32BytesToUint32() (uint32, []byte, uint32) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint32bytes(rawKey)
	value := bytesTouint32(rawValue)
	return key1, key2, value
}

// LastUint32BytesToUint32 is first with key of type pair(uint32,[]byte) and value of type uint32.
func (c *Cursor) LastUint32BytesToUint32() (uint32, []byte, uint32) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint32bytes(rawKey)
	value := bytesTouint32(rawValue)
	return key1, key2, value
}

// NextUint32BytesToUint32 is first with key of type pair(uint32,[]byte) and value of type uint32.
func (c *Cursor) NextUint32BytesToUint32() (uint32, []byte, uint32) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint32bytes(rawKey)
	value := bytesTouint32(rawValue)
	return key1, key2, value
}

// PrevUint32BytesToUint32 is first with key of type pair(uint32,[]byte) and value of type uint32.
func (c *Cursor) PrevUint32BytesToUint32() (uint32, []byte, uint32) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint32bytes(rawKey)
	value := bytesTouint32(rawValue)
	return key1, key2, value
}

// SeekUint32BytesToUint32 is first with key of type pair(uint32,[]byte) and value of type uint32.
func (c *Cursor) SeekUint32BytesToUint32(key1 uint32, key2 []byte) (uint32, []byte, uint32) {
	rawKey, rawValue := c.Seek(uint32bytesTobytes(key1, key2))
	key1, key2 = bytesTouint32bytes(rawKey)
	value := bytesTouint32(rawValue)
	return key1, key2, value
}

// ForEachUint32BytesToUint64 is a for each with key of type pair(uint32,[]byte) and value of type uint64.
func (b *Bucket) ForEachUint32BytesToUint64(fn func(key1 uint32, key2 []byte, value uint64) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint32bytes(rawKey)
			value := bytesTouint64(rawValue)
			return fn(key1, key2, value)
		},
	)
}

// GetUint32BytesToUint64 is a get with key of type pair(uint32,[]byte) and value of type uint64.
func (b *Bucket) GetUint32BytesToUint64(key1 uint32, key2 []byte) uint64 {
	return bytesTouint64(b.Get(uint32bytesTobytes(key1, key2)))
}

// GetRequiredUint32BytesToUint64 is a get with key of type pair(uint32,[]byte) and value of type uint64 that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint32BytesToUint64(key1 uint32, key2 []byte) (uint64, error) {
	value := b.Get(uint32bytesTobytes(key1, key2))
	if value == nil {
		return 0, NewKeyNotFoundError(key1, key2)
	}
	return bytesTouint64(value), nil
}

// PutUint32BytesToUint64 is a put with key of type pair(uint32,[]byte) and value of type uint64.
func (b *Bucket) PutUint32BytesToUint64(key1 uint32, key2 []byte, value uint64) error {
	return b.Put(uint32bytesTobytes(key1, key2), uint64Tobytes(value))
}

// FirstUint32BytesToUint64 is first with key of type pair(uint32,[]byte) and value of type uint64.
func (c *Cursor) FirstUint32BytesToUint64() (uint32, []byte, uint64) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint32bytes(rawKey)
	value := bytesTouint64(rawValue)
	return key1, key2, value
}

// LastUint32BytesToUint64 is first with key of type pair(uint32,[]byte) and value of type uint64.
func (c *Cursor) LastUint32BytesToUint64() (uint32, []byte, uint64) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint32bytes(rawKey)
	value := bytesTouint64(rawValue)
	return key1, key2, value
}

// NextUint32BytesToUint64 is first with key of type pair(uint32,[]byte) and value of type uint64.
func (c *Cursor) NextUint32BytesToUint64() (uint32, []byte, uint64) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint32bytes(rawKey)
	value := bytesTouint64(rawValue)
	return key1, key2, value
}

// PrevUint32BytesToUint64 is first with key of type pair(uint32,[]byte) and value of type uint64.
func (c *Cursor) PrevUint32BytesToUint64() (uint32, []byte, uint64) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint32bytes(rawKey)
	value := bytesTouint64(rawValue)
	return key1, key2, value
}

// SeekUint32BytesToUint64 is first with key of type pair(uint32,[]byte) and value of type uint64.
func (c *Cursor) SeekUint32BytesToUint64(key1 uint32, key2 []byte) (uint32, []byte, uint64) {
	rawKey, rawValue := c.Seek(uint32bytesTobytes(key1, key2))
	key1, key2 = bytesTouint32bytes(rawKey)
	value := bytesTouint64(rawValue)
	return key1, key2, value
}

// ForEachUint32BytesToUint16Uint16 is a for each with key of type pair(uint32,[]byte) and value of type pair(uint16,uint16).
func (b *Bucket) ForEachUint32BytesToUint16Uint16(fn func(key1 uint32, key2 []byte, value1 uint16, value2 uint16) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint32bytes(rawKey)
			value1, value2 := bytesTouint16uint16(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetUint32BytesToUint16Uint16 is a get with key of type pair(uint32,[]byte) and value of type pair(uint16,uint16).
func (b *Bucket) GetUint32BytesToUint16Uint16(key1 uint32, key2 []byte) (uint16, uint16) {
	return bytesTouint16uint16(b.Get(uint32bytesTobytes(key1, key2)))
}

// GetRequiredUint32BytesToUint16Uint16 is a get with key of type pair(uint32,[]byte) and value of type pair(uint16,uint16) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint32BytesToUint16Uint16(key1 uint32, key2 []byte) (uint16, uint16, error) {
	value := b.Get(uint32bytesTobytes(key1, key2))
	if value == nil {
		return 0, 0, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTouint16uint16(value)
	return value1, value2, nil
}

// PutUint32BytesToUint16Uint16 is a put with key of type pair(uint32,[]byte) and value of type pair(uint16,uint16).
func (b *Bucket) PutUint32BytesToUint16Uint16(key1 uint32, key2 []byte, value1 uint16, value2 uint16) error {
	return b.Put(uint32bytesTobytes(key1, key2), uint16uint16Tobytes(value1, value2))
}

// FirstUint32BytesToUint16Uint16 is first with key of type pair(uint32,[]byte) and value of type pair(uint16,uint16).
func (c *Cursor) FirstUint32BytesToUint16Uint16() (uint32, []byte, uint16, uint16) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint32bytes(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key1, key2, value1, value2
}

// LastUint32BytesToUint16Uint16 is first with key of type pair(uint32,[]byte) and value of type pair(uint16,uint16).
func (c *Cursor) LastUint32BytesToUint16Uint16() (uint32, []byte, uint16, uint16) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint32bytes(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key1, key2, value1, value2
}

// NextUint32BytesToUint16Uint16 is first with key of type pair(uint32,[]byte) and value of type pair(uint16,uint16).
func (c *Cursor) NextUint32BytesToUint16Uint16() (uint32, []byte, uint16, uint16) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint32bytes(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key1, key2, value1, value2
}

// PrevUint32BytesToUint16Uint16 is first with key of type pair(uint32,[]byte) and value of type pair(uint16,uint16).
func (c *Cursor) PrevUint32BytesToUint16Uint16() (uint32, []byte, uint16, uint16) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint32bytes(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key1, key2, value1, value2
}

// SeekUint32BytesToUint16Uint16 is first with key of type pair(uint32,[]byte) and value of type pair(uint16,uint16).
func (c *Cursor) SeekUint32BytesToUint16Uint16(key1 uint32, key2 []byte) (uint32, []byte, uint16, uint16) {
	rawKey, rawValue := c.Seek(uint32bytesTobytes(key1, key2))
	key1, key2 = bytesTouint32bytes(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key1, key2, value1, value2
}

// ForEachUint32BytesToUint32Uint32 is a for each with key of type pair(uint32,[]byte) and value of type pair(uint32,uint32).
func (b *Bucket) ForEachUint32BytesToUint32Uint32(fn func(key1 uint32, key2 []byte, value1 uint32, value2 uint32) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint32bytes(rawKey)
			value1, value2 := bytesTouint32uint32(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetUint32BytesToUint32Uint32 is a get with key of type pair(uint32,[]byte) and value of type pair(uint32,uint32).
func (b *Bucket) GetUint32BytesToUint32Uint32(key1 uint32, key2 []byte) (uint32, uint32) {
	return bytesTouint32uint32(b.Get(uint32bytesTobytes(key1, key2)))
}

// GetRequiredUint32BytesToUint32Uint32 is a get with key of type pair(uint32,[]byte) and value of type pair(uint32,uint32) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint32BytesToUint32Uint32(key1 uint32, key2 []byte) (uint32, uint32, error) {
	value := b.Get(uint32bytesTobytes(key1, key2))
	if value == nil {
		return 0, 0, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTouint32uint32(value)
	return value1, value2, nil
}

// PutUint32BytesToUint32Uint32 is a put with key of type pair(uint32,[]byte) and value of type pair(uint32,uint32).
func (b *Bucket) PutUint32BytesToUint32Uint32(key1 uint32, key2 []byte, value1 uint32, value2 uint32) error {
	return b.Put(uint32bytesTobytes(key1, key2), uint32uint32Tobytes(value1, value2))
}

// FirstUint32BytesToUint32Uint32 is first with key of type pair(uint32,[]byte) and value of type pair(uint32,uint32).
func (c *Cursor) FirstUint32BytesToUint32Uint32() (uint32, []byte, uint32, uint32) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint32bytes(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key1, key2, value1, value2
}

// LastUint32BytesToUint32Uint32 is first with key of type pair(uint32,[]byte) and value of type pair(uint32,uint32).
func (c *Cursor) LastUint32BytesToUint32Uint32() (uint32, []byte, uint32, uint32) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint32bytes(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key1, key2, value1, value2
}

// NextUint32BytesToUint32Uint32 is first with key of type pair(uint32,[]byte) and value of type pair(uint32,uint32).
func (c *Cursor) NextUint32BytesToUint32Uint32() (uint32, []byte, uint32, uint32) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint32bytes(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key1, key2, value1, value2
}

// PrevUint32BytesToUint32Uint32 is first with key of type pair(uint32,[]byte) and value of type pair(uint32,uint32).
func (c *Cursor) PrevUint32BytesToUint32Uint32() (uint32, []byte, uint32, uint32) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint32bytes(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key1, key2, value1, value2
}

// SeekUint32BytesToUint32Uint32 is first with key of type pair(uint32,[]byte) and value of type pair(uint32,uint32).
func (c *Cursor) SeekUint32BytesToUint32Uint32(key1 uint32, key2 []byte) (uint32, []byte, uint32, uint32) {
	rawKey, rawValue := c.Seek(uint32bytesTobytes(key1, key2))
	key1, key2 = bytesTouint32bytes(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key1, key2, value1, value2
}

// ForEachUint32BytesToUint64Uint64 is a for each with key of type pair(uint32,[]byte) and value of type pair(uint64,uint64).
func (b *Bucket) ForEachUint32BytesToUint64Uint64(fn func(key1 uint32, key2 []byte, value1 uint64, value2 uint64) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint32bytes(rawKey)
			value1, value2 := bytesTouint64uint64(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetUint32BytesToUint64Uint64 is a get with key of type pair(uint32,[]byte) and value of type pair(uint64,uint64).
func (b *Bucket) GetUint32BytesToUint64Uint64(key1 uint32, key2 []byte) (uint64, uint64) {
	return bytesTouint64uint64(b.Get(uint32bytesTobytes(key1, key2)))
}

// GetRequiredUint32BytesToUint64Uint64 is a get with key of type pair(uint32,[]byte) and value of type pair(uint64,uint64) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint32BytesToUint64Uint64(key1 uint32, key2 []byte) (uint64, uint64, error) {
	value := b.Get(uint32bytesTobytes(key1, key2))
	if value == nil {
		return 0, 0, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTouint64uint64(value)
	return value1, value2, nil
}

// PutUint32BytesToUint64Uint64 is a put with key of type pair(uint32,[]byte) and value of type pair(uint64,uint64).
func (b *Bucket) PutUint32BytesToUint64Uint64(key1 uint32, key2 []byte, value1 uint64, value2 uint64) error {
	return b.Put(uint32bytesTobytes(key1, key2), uint64uint64Tobytes(value1, value2))
}

// FirstUint32BytesToUint64Uint64 is first with key of type pair(uint32,[]byte) and value of type pair(uint64,uint64).
func (c *Cursor) FirstUint32BytesToUint64Uint64() (uint32, []byte, uint64, uint64) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint32bytes(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key1, key2, value1, value2
}

// LastUint32BytesToUint64Uint64 is first with key of type pair(uint32,[]byte) and value of type pair(uint64,uint64).
func (c *Cursor) LastUint32BytesToUint64Uint64() (uint32, []byte, uint64, uint64) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint32bytes(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key1, key2, value1, value2
}

// NextUint32BytesToUint64Uint64 is first with key of type pair(uint32,[]byte) and value of type pair(uint64,uint64).
func (c *Cursor) NextUint32BytesToUint64Uint64() (uint32, []byte, uint64, uint64) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint32bytes(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key1, key2, value1, value2
}

// PrevUint32BytesToUint64Uint64 is first with key of type pair(uint32,[]byte) and value of type pair(uint64,uint64).
func (c *Cursor) PrevUint32BytesToUint64Uint64() (uint32, []byte, uint64, uint64) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint32bytes(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key1, key2, value1, value2
}

// SeekUint32BytesToUint64Uint64 is first with key of type pair(uint32,[]byte) and value of type pair(uint64,uint64).
func (c *Cursor) SeekUint32BytesToUint64Uint64(key1 uint32, key2 []byte) (uint32, []byte, uint64, uint64) {
	rawKey, rawValue := c.Seek(uint32bytesTobytes(key1, key2))
	key1, key2 = bytesTouint32bytes(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key1, key2, value1, value2
}

// ForEachUint32BytesToUint16Bytes is a for each with key of type pair(uint32,[]byte) and value of type pair(uint16,[]byte).
func (b *Bucket) ForEachUint32BytesToUint16Bytes(fn func(key1 uint32, key2 []byte, value1 uint16, value2 []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint32bytes(rawKey)
			value1, value2 := bytesTouint16bytes(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetUint32BytesToUint16Bytes is a get with key of type pair(uint32,[]byte) and value of type pair(uint16,[]byte).
func (b *Bucket) GetUint32BytesToUint16Bytes(key1 uint32, key2 []byte) (uint16, []byte) {
	return bytesTouint16bytes(b.Get(uint32bytesTobytes(key1, key2)))
}

// GetRequiredUint32BytesToUint16Bytes is a get with key of type pair(uint32,[]byte) and value of type pair(uint16,[]byte) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint32BytesToUint16Bytes(key1 uint32, key2 []byte) (uint16, []byte, error) {
	value := b.Get(uint32bytesTobytes(key1, key2))
	if value == nil {
		return 0, nil, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTouint16bytes(value)
	return value1, value2, nil
}

// PutUint32BytesToUint16Bytes is a put with key of type pair(uint32,[]byte) and value of type pair(uint16,[]byte).
func (b *Bucket) PutUint32BytesToUint16Bytes(key1 uint32, key2 []byte, value1 uint16, value2 []byte) error {
	return b.Put(uint32bytesTobytes(key1, key2), uint16bytesTobytes(value1, value2))
}

// FirstUint32BytesToUint16Bytes is first with key of type pair(uint32,[]byte) and value of type pair(uint16,[]byte).
func (c *Cursor) FirstUint32BytesToUint16Bytes() (uint32, []byte, uint16, []byte) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint32bytes(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key1, key2, value1, value2
}

// LastUint32BytesToUint16Bytes is first with key of type pair(uint32,[]byte) and value of type pair(uint16,[]byte).
func (c *Cursor) LastUint32BytesToUint16Bytes() (uint32, []byte, uint16, []byte) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint32bytes(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key1, key2, value1, value2
}

// NextUint32BytesToUint16Bytes is first with key of type pair(uint32,[]byte) and value of type pair(uint16,[]byte).
func (c *Cursor) NextUint32BytesToUint16Bytes() (uint32, []byte, uint16, []byte) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint32bytes(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key1, key2, value1, value2
}

// PrevUint32BytesToUint16Bytes is first with key of type pair(uint32,[]byte) and value of type pair(uint16,[]byte).
func (c *Cursor) PrevUint32BytesToUint16Bytes() (uint32, []byte, uint16, []byte) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint32bytes(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key1, key2, value1, value2
}

// SeekUint32BytesToUint16Bytes is first with key of type pair(uint32,[]byte) and value of type pair(uint16,[]byte).
func (c *Cursor) SeekUint32BytesToUint16Bytes(key1 uint32, key2 []byte) (uint32, []byte, uint16, []byte) {
	rawKey, rawValue := c.Seek(uint32bytesTobytes(key1, key2))
	key1, key2 = bytesTouint32bytes(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key1, key2, value1, value2
}

// ForEachUint32BytesToUint32Bytes is a for each with key of type pair(uint32,[]byte) and value of type pair(uint32,[]byte).
func (b *Bucket) ForEachUint32BytesToUint32Bytes(fn func(key1 uint32, key2 []byte, value1 uint32, value2 []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint32bytes(rawKey)
			value1, value2 := bytesTouint32bytes(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetUint32BytesToUint32Bytes is a get with key of type pair(uint32,[]byte) and value of type pair(uint32,[]byte).
func (b *Bucket) GetUint32BytesToUint32Bytes(key1 uint32, key2 []byte) (uint32, []byte) {
	return bytesTouint32bytes(b.Get(uint32bytesTobytes(key1, key2)))
}

// GetRequiredUint32BytesToUint32Bytes is a get with key of type pair(uint32,[]byte) and value of type pair(uint32,[]byte) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint32BytesToUint32Bytes(key1 uint32, key2 []byte) (uint32, []byte, error) {
	value := b.Get(uint32bytesTobytes(key1, key2))
	if value == nil {
		return 0, nil, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTouint32bytes(value)
	return value1, value2, nil
}

// PutUint32BytesToUint32Bytes is a put with key of type pair(uint32,[]byte) and value of type pair(uint32,[]byte).
func (b *Bucket) PutUint32BytesToUint32Bytes(key1 uint32, key2 []byte, value1 uint32, value2 []byte) error {
	return b.Put(uint32bytesTobytes(key1, key2), uint32bytesTobytes(value1, value2))
}

// FirstUint32BytesToUint32Bytes is first with key of type pair(uint32,[]byte) and value of type pair(uint32,[]byte).
func (c *Cursor) FirstUint32BytesToUint32Bytes() (uint32, []byte, uint32, []byte) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint32bytes(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key1, key2, value1, value2
}

// LastUint32BytesToUint32Bytes is first with key of type pair(uint32,[]byte) and value of type pair(uint32,[]byte).
func (c *Cursor) LastUint32BytesToUint32Bytes() (uint32, []byte, uint32, []byte) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint32bytes(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key1, key2, value1, value2
}

// NextUint32BytesToUint32Bytes is first with key of type pair(uint32,[]byte) and value of type pair(uint32,[]byte).
func (c *Cursor) NextUint32BytesToUint32Bytes() (uint32, []byte, uint32, []byte) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint32bytes(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key1, key2, value1, value2
}

// PrevUint32BytesToUint32Bytes is first with key of type pair(uint32,[]byte) and value of type pair(uint32,[]byte).
func (c *Cursor) PrevUint32BytesToUint32Bytes() (uint32, []byte, uint32, []byte) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint32bytes(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key1, key2, value1, value2
}

// SeekUint32BytesToUint32Bytes is first with key of type pair(uint32,[]byte) and value of type pair(uint32,[]byte).
func (c *Cursor) SeekUint32BytesToUint32Bytes(key1 uint32, key2 []byte) (uint32, []byte, uint32, []byte) {
	rawKey, rawValue := c.Seek(uint32bytesTobytes(key1, key2))
	key1, key2 = bytesTouint32bytes(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key1, key2, value1, value2
}

// ForEachUint32BytesToUint64Bytes is a for each with key of type pair(uint32,[]byte) and value of type pair(uint64,[]byte).
func (b *Bucket) ForEachUint32BytesToUint64Bytes(fn func(key1 uint32, key2 []byte, value1 uint64, value2 []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint32bytes(rawKey)
			value1, value2 := bytesTouint64bytes(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetUint32BytesToUint64Bytes is a get with key of type pair(uint32,[]byte) and value of type pair(uint64,[]byte).
func (b *Bucket) GetUint32BytesToUint64Bytes(key1 uint32, key2 []byte) (uint64, []byte) {
	return bytesTouint64bytes(b.Get(uint32bytesTobytes(key1, key2)))
}

// GetRequiredUint32BytesToUint64Bytes is a get with key of type pair(uint32,[]byte) and value of type pair(uint64,[]byte) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint32BytesToUint64Bytes(key1 uint32, key2 []byte) (uint64, []byte, error) {
	value := b.Get(uint32bytesTobytes(key1, key2))
	if value == nil {
		return 0, nil, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTouint64bytes(value)
	return value1, value2, nil
}

// PutUint32BytesToUint64Bytes is a put with key of type pair(uint32,[]byte) and value of type pair(uint64,[]byte).
func (b *Bucket) PutUint32BytesToUint64Bytes(key1 uint32, key2 []byte, value1 uint64, value2 []byte) error {
	return b.Put(uint32bytesTobytes(key1, key2), uint64bytesTobytes(value1, value2))
}

// FirstUint32BytesToUint64Bytes is first with key of type pair(uint32,[]byte) and value of type pair(uint64,[]byte).
func (c *Cursor) FirstUint32BytesToUint64Bytes() (uint32, []byte, uint64, []byte) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint32bytes(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key1, key2, value1, value2
}

// LastUint32BytesToUint64Bytes is first with key of type pair(uint32,[]byte) and value of type pair(uint64,[]byte).
func (c *Cursor) LastUint32BytesToUint64Bytes() (uint32, []byte, uint64, []byte) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint32bytes(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key1, key2, value1, value2
}

// NextUint32BytesToUint64Bytes is first with key of type pair(uint32,[]byte) and value of type pair(uint64,[]byte).
func (c *Cursor) NextUint32BytesToUint64Bytes() (uint32, []byte, uint64, []byte) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint32bytes(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key1, key2, value1, value2
}

// PrevUint32BytesToUint64Bytes is first with key of type pair(uint32,[]byte) and value of type pair(uint64,[]byte).
func (c *Cursor) PrevUint32BytesToUint64Bytes() (uint32, []byte, uint64, []byte) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint32bytes(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key1, key2, value1, value2
}

// SeekUint32BytesToUint64Bytes is first with key of type pair(uint32,[]byte) and value of type pair(uint64,[]byte).
func (c *Cursor) SeekUint32BytesToUint64Bytes(key1 uint32, key2 []byte) (uint32, []byte, uint64, []byte) {
	rawKey, rawValue := c.Seek(uint32bytesTobytes(key1, key2))
	key1, key2 = bytesTouint32bytes(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key1, key2, value1, value2
}

// ForEachUint32BytesToBytesUint16 is a for each with key of type pair(uint32,[]byte) and value of type pair([]byte,uint16).
func (b *Bucket) ForEachUint32BytesToBytesUint16(fn func(key1 uint32, key2 []byte, value1 []byte, value2 uint16) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint32bytes(rawKey)
			value1, value2 := bytesTobytesuint16(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetUint32BytesToBytesUint16 is a get with key of type pair(uint32,[]byte) and value of type pair([]byte,uint16).
func (b *Bucket) GetUint32BytesToBytesUint16(key1 uint32, key2 []byte) ([]byte, uint16) {
	return bytesTobytesuint16(b.Get(uint32bytesTobytes(key1, key2)))
}

// GetRequiredUint32BytesToBytesUint16 is a get with key of type pair(uint32,[]byte) and value of type pair([]byte,uint16) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint32BytesToBytesUint16(key1 uint32, key2 []byte) ([]byte, uint16, error) {
	value := b.Get(uint32bytesTobytes(key1, key2))
	if value == nil {
		return nil, 0, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTobytesuint16(value)
	return value1, value2, nil
}

// PutUint32BytesToBytesUint16 is a put with key of type pair(uint32,[]byte) and value of type pair([]byte,uint16).
func (b *Bucket) PutUint32BytesToBytesUint16(key1 uint32, key2 []byte, value1 []byte, value2 uint16) error {
	return b.Put(uint32bytesTobytes(key1, key2), bytesuint16Tobytes(value1, value2))
}

// FirstUint32BytesToBytesUint16 is first with key of type pair(uint32,[]byte) and value of type pair([]byte,uint16).
func (c *Cursor) FirstUint32BytesToBytesUint16() (uint32, []byte, []byte, uint16) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint32bytes(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key1, key2, value1, value2
}

// LastUint32BytesToBytesUint16 is first with key of type pair(uint32,[]byte) and value of type pair([]byte,uint16).
func (c *Cursor) LastUint32BytesToBytesUint16() (uint32, []byte, []byte, uint16) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint32bytes(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key1, key2, value1, value2
}

// NextUint32BytesToBytesUint16 is first with key of type pair(uint32,[]byte) and value of type pair([]byte,uint16).
func (c *Cursor) NextUint32BytesToBytesUint16() (uint32, []byte, []byte, uint16) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint32bytes(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key1, key2, value1, value2
}

// PrevUint32BytesToBytesUint16 is first with key of type pair(uint32,[]byte) and value of type pair([]byte,uint16).
func (c *Cursor) PrevUint32BytesToBytesUint16() (uint32, []byte, []byte, uint16) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint32bytes(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key1, key2, value1, value2
}

// SeekUint32BytesToBytesUint16 is first with key of type pair(uint32,[]byte) and value of type pair([]byte,uint16).
func (c *Cursor) SeekUint32BytesToBytesUint16(key1 uint32, key2 []byte) (uint32, []byte, []byte, uint16) {
	rawKey, rawValue := c.Seek(uint32bytesTobytes(key1, key2))
	key1, key2 = bytesTouint32bytes(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key1, key2, value1, value2
}

// ForEachUint32BytesToBytesUint32 is a for each with key of type pair(uint32,[]byte) and value of type pair([]byte,uint32).
func (b *Bucket) ForEachUint32BytesToBytesUint32(fn func(key1 uint32, key2 []byte, value1 []byte, value2 uint32) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint32bytes(rawKey)
			value1, value2 := bytesTobytesuint32(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetUint32BytesToBytesUint32 is a get with key of type pair(uint32,[]byte) and value of type pair([]byte,uint32).
func (b *Bucket) GetUint32BytesToBytesUint32(key1 uint32, key2 []byte) ([]byte, uint32) {
	return bytesTobytesuint32(b.Get(uint32bytesTobytes(key1, key2)))
}

// GetRequiredUint32BytesToBytesUint32 is a get with key of type pair(uint32,[]byte) and value of type pair([]byte,uint32) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint32BytesToBytesUint32(key1 uint32, key2 []byte) ([]byte, uint32, error) {
	value := b.Get(uint32bytesTobytes(key1, key2))
	if value == nil {
		return nil, 0, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTobytesuint32(value)
	return value1, value2, nil
}

// PutUint32BytesToBytesUint32 is a put with key of type pair(uint32,[]byte) and value of type pair([]byte,uint32).
func (b *Bucket) PutUint32BytesToBytesUint32(key1 uint32, key2 []byte, value1 []byte, value2 uint32) error {
	return b.Put(uint32bytesTobytes(key1, key2), bytesuint32Tobytes(value1, value2))
}

// FirstUint32BytesToBytesUint32 is first with key of type pair(uint32,[]byte) and value of type pair([]byte,uint32).
func (c *Cursor) FirstUint32BytesToBytesUint32() (uint32, []byte, []byte, uint32) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint32bytes(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key1, key2, value1, value2
}

// LastUint32BytesToBytesUint32 is first with key of type pair(uint32,[]byte) and value of type pair([]byte,uint32).
func (c *Cursor) LastUint32BytesToBytesUint32() (uint32, []byte, []byte, uint32) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint32bytes(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key1, key2, value1, value2
}

// NextUint32BytesToBytesUint32 is first with key of type pair(uint32,[]byte) and value of type pair([]byte,uint32).
func (c *Cursor) NextUint32BytesToBytesUint32() (uint32, []byte, []byte, uint32) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint32bytes(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key1, key2, value1, value2
}

// PrevUint32BytesToBytesUint32 is first with key of type pair(uint32,[]byte) and value of type pair([]byte,uint32).
func (c *Cursor) PrevUint32BytesToBytesUint32() (uint32, []byte, []byte, uint32) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint32bytes(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key1, key2, value1, value2
}

// SeekUint32BytesToBytesUint32 is first with key of type pair(uint32,[]byte) and value of type pair([]byte,uint32).
func (c *Cursor) SeekUint32BytesToBytesUint32(key1 uint32, key2 []byte) (uint32, []byte, []byte, uint32) {
	rawKey, rawValue := c.Seek(uint32bytesTobytes(key1, key2))
	key1, key2 = bytesTouint32bytes(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key1, key2, value1, value2
}

// ForEachUint32BytesToBytesUint64 is a for each with key of type pair(uint32,[]byte) and value of type pair([]byte,uint64).
func (b *Bucket) ForEachUint32BytesToBytesUint64(fn func(key1 uint32, key2 []byte, value1 []byte, value2 uint64) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint32bytes(rawKey)
			value1, value2 := bytesTobytesuint64(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetUint32BytesToBytesUint64 is a get with key of type pair(uint32,[]byte) and value of type pair([]byte,uint64).
func (b *Bucket) GetUint32BytesToBytesUint64(key1 uint32, key2 []byte) ([]byte, uint64) {
	return bytesTobytesuint64(b.Get(uint32bytesTobytes(key1, key2)))
}

// GetRequiredUint32BytesToBytesUint64 is a get with key of type pair(uint32,[]byte) and value of type pair([]byte,uint64) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint32BytesToBytesUint64(key1 uint32, key2 []byte) ([]byte, uint64, error) {
	value := b.Get(uint32bytesTobytes(key1, key2))
	if value == nil {
		return nil, 0, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTobytesuint64(value)
	return value1, value2, nil
}

// PutUint32BytesToBytesUint64 is a put with key of type pair(uint32,[]byte) and value of type pair([]byte,uint64).
func (b *Bucket) PutUint32BytesToBytesUint64(key1 uint32, key2 []byte, value1 []byte, value2 uint64) error {
	return b.Put(uint32bytesTobytes(key1, key2), bytesuint64Tobytes(value1, value2))
}

// FirstUint32BytesToBytesUint64 is first with key of type pair(uint32,[]byte) and value of type pair([]byte,uint64).
func (c *Cursor) FirstUint32BytesToBytesUint64() (uint32, []byte, []byte, uint64) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint32bytes(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key1, key2, value1, value2
}

// LastUint32BytesToBytesUint64 is first with key of type pair(uint32,[]byte) and value of type pair([]byte,uint64).
func (c *Cursor) LastUint32BytesToBytesUint64() (uint32, []byte, []byte, uint64) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint32bytes(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key1, key2, value1, value2
}

// NextUint32BytesToBytesUint64 is first with key of type pair(uint32,[]byte) and value of type pair([]byte,uint64).
func (c *Cursor) NextUint32BytesToBytesUint64() (uint32, []byte, []byte, uint64) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint32bytes(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key1, key2, value1, value2
}

// PrevUint32BytesToBytesUint64 is first with key of type pair(uint32,[]byte) and value of type pair([]byte,uint64).
func (c *Cursor) PrevUint32BytesToBytesUint64() (uint32, []byte, []byte, uint64) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint32bytes(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key1, key2, value1, value2
}

// SeekUint32BytesToBytesUint64 is first with key of type pair(uint32,[]byte) and value of type pair([]byte,uint64).
func (c *Cursor) SeekUint32BytesToBytesUint64(key1 uint32, key2 []byte) (uint32, []byte, []byte, uint64) {
	rawKey, rawValue := c.Seek(uint32bytesTobytes(key1, key2))
	key1, key2 = bytesTouint32bytes(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key1, key2, value1, value2
}

// DeleteUint32Bytes is a delete with key of type pair(uint32,[]byte).
func (b *Bucket) DeleteUint32Bytes(key1 uint32, key2 []byte) error {
	return b.Delete(uint32bytesTobytes(key1, key2))
}

// ForEachUint64BytesToBytes is a for each with key of type pair(uint64,[]byte) and value of type []byte.
func (b *Bucket) ForEachUint64BytesToBytes(fn func(key1 uint64, key2 []byte, value []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint64bytes(rawKey)
			value := (rawValue)
			return fn(key1, key2, value)
		},
	)
}

// GetUint64BytesToBytes is a get with key of type pair(uint64,[]byte) and value of type []byte.
func (b *Bucket) GetUint64BytesToBytes(key1 uint64, key2 []byte) []byte {
	return (b.Get(uint64bytesTobytes(key1, key2)))
}

// GetRequiredUint64BytesToBytes is a get with key of type pair(uint64,[]byte) and value of type []byte that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint64BytesToBytes(key1 uint64, key2 []byte) ([]byte, error) {
	value := b.Get(uint64bytesTobytes(key1, key2))
	if value == nil {
		return nil, NewKeyNotFoundError(key1, key2)
	}
	return (value), nil
}

// PutUint64BytesToBytes is a put with key of type pair(uint64,[]byte) and value of type []byte.
func (b *Bucket) PutUint64BytesToBytes(key1 uint64, key2 []byte, value []byte) error {
	return b.Put(uint64bytesTobytes(key1, key2), (value))
}

// FirstUint64BytesToBytes is first with key of type pair(uint64,[]byte) and value of type []byte.
func (c *Cursor) FirstUint64BytesToBytes() (uint64, []byte, []byte) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint64bytes(rawKey)
	value := (rawValue)
	return key1, key2, value
}

// LastUint64BytesToBytes is first with key of type pair(uint64,[]byte) and value of type []byte.
func (c *Cursor) LastUint64BytesToBytes() (uint64, []byte, []byte) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint64bytes(rawKey)
	value := (rawValue)
	return key1, key2, value
}

// NextUint64BytesToBytes is first with key of type pair(uint64,[]byte) and value of type []byte.
func (c *Cursor) NextUint64BytesToBytes() (uint64, []byte, []byte) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint64bytes(rawKey)
	value := (rawValue)
	return key1, key2, value
}

// PrevUint64BytesToBytes is first with key of type pair(uint64,[]byte) and value of type []byte.
func (c *Cursor) PrevUint64BytesToBytes() (uint64, []byte, []byte) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint64bytes(rawKey)
	value := (rawValue)
	return key1, key2, value
}

// SeekUint64BytesToBytes is first with key of type pair(uint64,[]byte) and value of type []byte.
func (c *Cursor) SeekUint64BytesToBytes(key1 uint64, key2 []byte) (uint64, []byte, []byte) {
	rawKey, rawValue := c.Seek(uint64bytesTobytes(key1, key2))
	key1, key2 = bytesTouint64bytes(rawKey)
	value := (rawValue)
	return key1, key2, value
}

// ForEachUint64BytesToString is a for each with key of type pair(uint64,[]byte) and value of type string.
func (b *Bucket) ForEachUint64BytesToString(fn func(key1 uint64, key2 []byte, value string) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint64bytes(rawKey)
			value := string(rawValue)
			return fn(key1, key2, value)
		},
	)
}

// GetUint64BytesToString is a get with key of type pair(uint64,[]byte) and value of type string.
func (b *Bucket) GetUint64BytesToString(key1 uint64, key2 []byte) string {
	return string(b.Get(uint64bytesTobytes(key1, key2)))
}

// GetRequiredUint64BytesToString is a get with key of type pair(uint64,[]byte) and value of type string that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint64BytesToString(key1 uint64, key2 []byte) (string, error) {
	value := b.Get(uint64bytesTobytes(key1, key2))
	if value == nil {
		return "", NewKeyNotFoundError(key1, key2)
	}
	return string(value), nil
}

// PutUint64BytesToString is a put with key of type pair(uint64,[]byte) and value of type string.
func (b *Bucket) PutUint64BytesToString(key1 uint64, key2 []byte, value string) error {
	return b.Put(uint64bytesTobytes(key1, key2), []byte(value))
}

// FirstUint64BytesToString is first with key of type pair(uint64,[]byte) and value of type string.
func (c *Cursor) FirstUint64BytesToString() (uint64, []byte, string) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint64bytes(rawKey)
	value := string(rawValue)
	return key1, key2, value
}

// LastUint64BytesToString is first with key of type pair(uint64,[]byte) and value of type string.
func (c *Cursor) LastUint64BytesToString() (uint64, []byte, string) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint64bytes(rawKey)
	value := string(rawValue)
	return key1, key2, value
}

// NextUint64BytesToString is first with key of type pair(uint64,[]byte) and value of type string.
func (c *Cursor) NextUint64BytesToString() (uint64, []byte, string) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint64bytes(rawKey)
	value := string(rawValue)
	return key1, key2, value
}

// PrevUint64BytesToString is first with key of type pair(uint64,[]byte) and value of type string.
func (c *Cursor) PrevUint64BytesToString() (uint64, []byte, string) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint64bytes(rawKey)
	value := string(rawValue)
	return key1, key2, value
}

// SeekUint64BytesToString is first with key of type pair(uint64,[]byte) and value of type string.
func (c *Cursor) SeekUint64BytesToString(key1 uint64, key2 []byte) (uint64, []byte, string) {
	rawKey, rawValue := c.Seek(uint64bytesTobytes(key1, key2))
	key1, key2 = bytesTouint64bytes(rawKey)
	value := string(rawValue)
	return key1, key2, value
}

// ForEachUint64BytesToUint16 is a for each with key of type pair(uint64,[]byte) and value of type uint16.
func (b *Bucket) ForEachUint64BytesToUint16(fn func(key1 uint64, key2 []byte, value uint16) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint64bytes(rawKey)
			value := bytesTouint16(rawValue)
			return fn(key1, key2, value)
		},
	)
}

// GetUint64BytesToUint16 is a get with key of type pair(uint64,[]byte) and value of type uint16.
func (b *Bucket) GetUint64BytesToUint16(key1 uint64, key2 []byte) uint16 {
	return bytesTouint16(b.Get(uint64bytesTobytes(key1, key2)))
}

// GetRequiredUint64BytesToUint16 is a get with key of type pair(uint64,[]byte) and value of type uint16 that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint64BytesToUint16(key1 uint64, key2 []byte) (uint16, error) {
	value := b.Get(uint64bytesTobytes(key1, key2))
	if value == nil {
		return 0, NewKeyNotFoundError(key1, key2)
	}
	return bytesTouint16(value), nil
}

// PutUint64BytesToUint16 is a put with key of type pair(uint64,[]byte) and value of type uint16.
func (b *Bucket) PutUint64BytesToUint16(key1 uint64, key2 []byte, value uint16) error {
	return b.Put(uint64bytesTobytes(key1, key2), uint16Tobytes(value))
}

// FirstUint64BytesToUint16 is first with key of type pair(uint64,[]byte) and value of type uint16.
func (c *Cursor) FirstUint64BytesToUint16() (uint64, []byte, uint16) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint64bytes(rawKey)
	value := bytesTouint16(rawValue)
	return key1, key2, value
}

// LastUint64BytesToUint16 is first with key of type pair(uint64,[]byte) and value of type uint16.
func (c *Cursor) LastUint64BytesToUint16() (uint64, []byte, uint16) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint64bytes(rawKey)
	value := bytesTouint16(rawValue)
	return key1, key2, value
}

// NextUint64BytesToUint16 is first with key of type pair(uint64,[]byte) and value of type uint16.
func (c *Cursor) NextUint64BytesToUint16() (uint64, []byte, uint16) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint64bytes(rawKey)
	value := bytesTouint16(rawValue)
	return key1, key2, value
}

// PrevUint64BytesToUint16 is first with key of type pair(uint64,[]byte) and value of type uint16.
func (c *Cursor) PrevUint64BytesToUint16() (uint64, []byte, uint16) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint64bytes(rawKey)
	value := bytesTouint16(rawValue)
	return key1, key2, value
}

// SeekUint64BytesToUint16 is first with key of type pair(uint64,[]byte) and value of type uint16.
func (c *Cursor) SeekUint64BytesToUint16(key1 uint64, key2 []byte) (uint64, []byte, uint16) {
	rawKey, rawValue := c.Seek(uint64bytesTobytes(key1, key2))
	key1, key2 = bytesTouint64bytes(rawKey)
	value := bytesTouint16(rawValue)
	return key1, key2, value
}

// ForEachUint64BytesToUint32 is a for each with key of type pair(uint64,[]byte) and value of type uint32.
func (b *Bucket) ForEachUint64BytesToUint32(fn func(key1 uint64, key2 []byte, value uint32) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint64bytes(rawKey)
			value := bytesTouint32(rawValue)
			return fn(key1, key2, value)
		},
	)
}

// GetUint64BytesToUint32 is a get with key of type pair(uint64,[]byte) and value of type uint32.
func (b *Bucket) GetUint64BytesToUint32(key1 uint64, key2 []byte) uint32 {
	return bytesTouint32(b.Get(uint64bytesTobytes(key1, key2)))
}

// GetRequiredUint64BytesToUint32 is a get with key of type pair(uint64,[]byte) and value of type uint32 that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint64BytesToUint32(key1 uint64, key2 []byte) (uint32, error) {
	value := b.Get(uint64bytesTobytes(key1, key2))
	if value == nil {
		return 0, NewKeyNotFoundError(key1, key2)
	}
	return bytesTouint32(value), nil
}

// PutUint64BytesToUint32 is a put with key of type pair(uint64,[]byte) and value of type uint32.
func (b *Bucket) PutUint64BytesToUint32(key1 uint64, key2 []byte, value uint32) error {
	return b.Put(uint64bytesTobytes(key1, key2), uint32Tobytes(value))
}

// FirstUint64BytesToUint32 is first with key of type pair(uint64,[]byte) and value of type uint32.
func (c *Cursor) FirstUint64BytesToUint32() (uint64, []byte, uint32) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint64bytes(rawKey)
	value := bytesTouint32(rawValue)
	return key1, key2, value
}

// LastUint64BytesToUint32 is first with key of type pair(uint64,[]byte) and value of type uint32.
func (c *Cursor) LastUint64BytesToUint32() (uint64, []byte, uint32) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint64bytes(rawKey)
	value := bytesTouint32(rawValue)
	return key1, key2, value
}

// NextUint64BytesToUint32 is first with key of type pair(uint64,[]byte) and value of type uint32.
func (c *Cursor) NextUint64BytesToUint32() (uint64, []byte, uint32) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint64bytes(rawKey)
	value := bytesTouint32(rawValue)
	return key1, key2, value
}

// PrevUint64BytesToUint32 is first with key of type pair(uint64,[]byte) and value of type uint32.
func (c *Cursor) PrevUint64BytesToUint32() (uint64, []byte, uint32) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint64bytes(rawKey)
	value := bytesTouint32(rawValue)
	return key1, key2, value
}

// SeekUint64BytesToUint32 is first with key of type pair(uint64,[]byte) and value of type uint32.
func (c *Cursor) SeekUint64BytesToUint32(key1 uint64, key2 []byte) (uint64, []byte, uint32) {
	rawKey, rawValue := c.Seek(uint64bytesTobytes(key1, key2))
	key1, key2 = bytesTouint64bytes(rawKey)
	value := bytesTouint32(rawValue)
	return key1, key2, value
}

// ForEachUint64BytesToUint64 is a for each with key of type pair(uint64,[]byte) and value of type uint64.
func (b *Bucket) ForEachUint64BytesToUint64(fn func(key1 uint64, key2 []byte, value uint64) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint64bytes(rawKey)
			value := bytesTouint64(rawValue)
			return fn(key1, key2, value)
		},
	)
}

// GetUint64BytesToUint64 is a get with key of type pair(uint64,[]byte) and value of type uint64.
func (b *Bucket) GetUint64BytesToUint64(key1 uint64, key2 []byte) uint64 {
	return bytesTouint64(b.Get(uint64bytesTobytes(key1, key2)))
}

// GetRequiredUint64BytesToUint64 is a get with key of type pair(uint64,[]byte) and value of type uint64 that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint64BytesToUint64(key1 uint64, key2 []byte) (uint64, error) {
	value := b.Get(uint64bytesTobytes(key1, key2))
	if value == nil {
		return 0, NewKeyNotFoundError(key1, key2)
	}
	return bytesTouint64(value), nil
}

// PutUint64BytesToUint64 is a put with key of type pair(uint64,[]byte) and value of type uint64.
func (b *Bucket) PutUint64BytesToUint64(key1 uint64, key2 []byte, value uint64) error {
	return b.Put(uint64bytesTobytes(key1, key2), uint64Tobytes(value))
}

// FirstUint64BytesToUint64 is first with key of type pair(uint64,[]byte) and value of type uint64.
func (c *Cursor) FirstUint64BytesToUint64() (uint64, []byte, uint64) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint64bytes(rawKey)
	value := bytesTouint64(rawValue)
	return key1, key2, value
}

// LastUint64BytesToUint64 is first with key of type pair(uint64,[]byte) and value of type uint64.
func (c *Cursor) LastUint64BytesToUint64() (uint64, []byte, uint64) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint64bytes(rawKey)
	value := bytesTouint64(rawValue)
	return key1, key2, value
}

// NextUint64BytesToUint64 is first with key of type pair(uint64,[]byte) and value of type uint64.
func (c *Cursor) NextUint64BytesToUint64() (uint64, []byte, uint64) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint64bytes(rawKey)
	value := bytesTouint64(rawValue)
	return key1, key2, value
}

// PrevUint64BytesToUint64 is first with key of type pair(uint64,[]byte) and value of type uint64.
func (c *Cursor) PrevUint64BytesToUint64() (uint64, []byte, uint64) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint64bytes(rawKey)
	value := bytesTouint64(rawValue)
	return key1, key2, value
}

// SeekUint64BytesToUint64 is first with key of type pair(uint64,[]byte) and value of type uint64.
func (c *Cursor) SeekUint64BytesToUint64(key1 uint64, key2 []byte) (uint64, []byte, uint64) {
	rawKey, rawValue := c.Seek(uint64bytesTobytes(key1, key2))
	key1, key2 = bytesTouint64bytes(rawKey)
	value := bytesTouint64(rawValue)
	return key1, key2, value
}

// ForEachUint64BytesToUint16Uint16 is a for each with key of type pair(uint64,[]byte) and value of type pair(uint16,uint16).
func (b *Bucket) ForEachUint64BytesToUint16Uint16(fn func(key1 uint64, key2 []byte, value1 uint16, value2 uint16) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint64bytes(rawKey)
			value1, value2 := bytesTouint16uint16(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetUint64BytesToUint16Uint16 is a get with key of type pair(uint64,[]byte) and value of type pair(uint16,uint16).
func (b *Bucket) GetUint64BytesToUint16Uint16(key1 uint64, key2 []byte) (uint16, uint16) {
	return bytesTouint16uint16(b.Get(uint64bytesTobytes(key1, key2)))
}

// GetRequiredUint64BytesToUint16Uint16 is a get with key of type pair(uint64,[]byte) and value of type pair(uint16,uint16) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint64BytesToUint16Uint16(key1 uint64, key2 []byte) (uint16, uint16, error) {
	value := b.Get(uint64bytesTobytes(key1, key2))
	if value == nil {
		return 0, 0, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTouint16uint16(value)
	return value1, value2, nil
}

// PutUint64BytesToUint16Uint16 is a put with key of type pair(uint64,[]byte) and value of type pair(uint16,uint16).
func (b *Bucket) PutUint64BytesToUint16Uint16(key1 uint64, key2 []byte, value1 uint16, value2 uint16) error {
	return b.Put(uint64bytesTobytes(key1, key2), uint16uint16Tobytes(value1, value2))
}

// FirstUint64BytesToUint16Uint16 is first with key of type pair(uint64,[]byte) and value of type pair(uint16,uint16).
func (c *Cursor) FirstUint64BytesToUint16Uint16() (uint64, []byte, uint16, uint16) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint64bytes(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key1, key2, value1, value2
}

// LastUint64BytesToUint16Uint16 is first with key of type pair(uint64,[]byte) and value of type pair(uint16,uint16).
func (c *Cursor) LastUint64BytesToUint16Uint16() (uint64, []byte, uint16, uint16) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint64bytes(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key1, key2, value1, value2
}

// NextUint64BytesToUint16Uint16 is first with key of type pair(uint64,[]byte) and value of type pair(uint16,uint16).
func (c *Cursor) NextUint64BytesToUint16Uint16() (uint64, []byte, uint16, uint16) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint64bytes(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key1, key2, value1, value2
}

// PrevUint64BytesToUint16Uint16 is first with key of type pair(uint64,[]byte) and value of type pair(uint16,uint16).
func (c *Cursor) PrevUint64BytesToUint16Uint16() (uint64, []byte, uint16, uint16) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint64bytes(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key1, key2, value1, value2
}

// SeekUint64BytesToUint16Uint16 is first with key of type pair(uint64,[]byte) and value of type pair(uint16,uint16).
func (c *Cursor) SeekUint64BytesToUint16Uint16(key1 uint64, key2 []byte) (uint64, []byte, uint16, uint16) {
	rawKey, rawValue := c.Seek(uint64bytesTobytes(key1, key2))
	key1, key2 = bytesTouint64bytes(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key1, key2, value1, value2
}

// ForEachUint64BytesToUint32Uint32 is a for each with key of type pair(uint64,[]byte) and value of type pair(uint32,uint32).
func (b *Bucket) ForEachUint64BytesToUint32Uint32(fn func(key1 uint64, key2 []byte, value1 uint32, value2 uint32) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint64bytes(rawKey)
			value1, value2 := bytesTouint32uint32(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetUint64BytesToUint32Uint32 is a get with key of type pair(uint64,[]byte) and value of type pair(uint32,uint32).
func (b *Bucket) GetUint64BytesToUint32Uint32(key1 uint64, key2 []byte) (uint32, uint32) {
	return bytesTouint32uint32(b.Get(uint64bytesTobytes(key1, key2)))
}

// GetRequiredUint64BytesToUint32Uint32 is a get with key of type pair(uint64,[]byte) and value of type pair(uint32,uint32) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint64BytesToUint32Uint32(key1 uint64, key2 []byte) (uint32, uint32, error) {
	value := b.Get(uint64bytesTobytes(key1, key2))
	if value == nil {
		return 0, 0, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTouint32uint32(value)
	return value1, value2, nil
}

// PutUint64BytesToUint32Uint32 is a put with key of type pair(uint64,[]byte) and value of type pair(uint32,uint32).
func (b *Bucket) PutUint64BytesToUint32Uint32(key1 uint64, key2 []byte, value1 uint32, value2 uint32) error {
	return b.Put(uint64bytesTobytes(key1, key2), uint32uint32Tobytes(value1, value2))
}

// FirstUint64BytesToUint32Uint32 is first with key of type pair(uint64,[]byte) and value of type pair(uint32,uint32).
func (c *Cursor) FirstUint64BytesToUint32Uint32() (uint64, []byte, uint32, uint32) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint64bytes(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key1, key2, value1, value2
}

// LastUint64BytesToUint32Uint32 is first with key of type pair(uint64,[]byte) and value of type pair(uint32,uint32).
func (c *Cursor) LastUint64BytesToUint32Uint32() (uint64, []byte, uint32, uint32) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint64bytes(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key1, key2, value1, value2
}

// NextUint64BytesToUint32Uint32 is first with key of type pair(uint64,[]byte) and value of type pair(uint32,uint32).
func (c *Cursor) NextUint64BytesToUint32Uint32() (uint64, []byte, uint32, uint32) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint64bytes(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key1, key2, value1, value2
}

// PrevUint64BytesToUint32Uint32 is first with key of type pair(uint64,[]byte) and value of type pair(uint32,uint32).
func (c *Cursor) PrevUint64BytesToUint32Uint32() (uint64, []byte, uint32, uint32) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint64bytes(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key1, key2, value1, value2
}

// SeekUint64BytesToUint32Uint32 is first with key of type pair(uint64,[]byte) and value of type pair(uint32,uint32).
func (c *Cursor) SeekUint64BytesToUint32Uint32(key1 uint64, key2 []byte) (uint64, []byte, uint32, uint32) {
	rawKey, rawValue := c.Seek(uint64bytesTobytes(key1, key2))
	key1, key2 = bytesTouint64bytes(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key1, key2, value1, value2
}

// ForEachUint64BytesToUint64Uint64 is a for each with key of type pair(uint64,[]byte) and value of type pair(uint64,uint64).
func (b *Bucket) ForEachUint64BytesToUint64Uint64(fn func(key1 uint64, key2 []byte, value1 uint64, value2 uint64) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint64bytes(rawKey)
			value1, value2 := bytesTouint64uint64(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetUint64BytesToUint64Uint64 is a get with key of type pair(uint64,[]byte) and value of type pair(uint64,uint64).
func (b *Bucket) GetUint64BytesToUint64Uint64(key1 uint64, key2 []byte) (uint64, uint64) {
	return bytesTouint64uint64(b.Get(uint64bytesTobytes(key1, key2)))
}

// GetRequiredUint64BytesToUint64Uint64 is a get with key of type pair(uint64,[]byte) and value of type pair(uint64,uint64) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint64BytesToUint64Uint64(key1 uint64, key2 []byte) (uint64, uint64, error) {
	value := b.Get(uint64bytesTobytes(key1, key2))
	if value == nil {
		return 0, 0, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTouint64uint64(value)
	return value1, value2, nil
}

// PutUint64BytesToUint64Uint64 is a put with key of type pair(uint64,[]byte) and value of type pair(uint64,uint64).
func (b *Bucket) PutUint64BytesToUint64Uint64(key1 uint64, key2 []byte, value1 uint64, value2 uint64) error {
	return b.Put(uint64bytesTobytes(key1, key2), uint64uint64Tobytes(value1, value2))
}

// FirstUint64BytesToUint64Uint64 is first with key of type pair(uint64,[]byte) and value of type pair(uint64,uint64).
func (c *Cursor) FirstUint64BytesToUint64Uint64() (uint64, []byte, uint64, uint64) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint64bytes(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key1, key2, value1, value2
}

// LastUint64BytesToUint64Uint64 is first with key of type pair(uint64,[]byte) and value of type pair(uint64,uint64).
func (c *Cursor) LastUint64BytesToUint64Uint64() (uint64, []byte, uint64, uint64) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint64bytes(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key1, key2, value1, value2
}

// NextUint64BytesToUint64Uint64 is first with key of type pair(uint64,[]byte) and value of type pair(uint64,uint64).
func (c *Cursor) NextUint64BytesToUint64Uint64() (uint64, []byte, uint64, uint64) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint64bytes(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key1, key2, value1, value2
}

// PrevUint64BytesToUint64Uint64 is first with key of type pair(uint64,[]byte) and value of type pair(uint64,uint64).
func (c *Cursor) PrevUint64BytesToUint64Uint64() (uint64, []byte, uint64, uint64) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint64bytes(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key1, key2, value1, value2
}

// SeekUint64BytesToUint64Uint64 is first with key of type pair(uint64,[]byte) and value of type pair(uint64,uint64).
func (c *Cursor) SeekUint64BytesToUint64Uint64(key1 uint64, key2 []byte) (uint64, []byte, uint64, uint64) {
	rawKey, rawValue := c.Seek(uint64bytesTobytes(key1, key2))
	key1, key2 = bytesTouint64bytes(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key1, key2, value1, value2
}

// ForEachUint64BytesToUint16Bytes is a for each with key of type pair(uint64,[]byte) and value of type pair(uint16,[]byte).
func (b *Bucket) ForEachUint64BytesToUint16Bytes(fn func(key1 uint64, key2 []byte, value1 uint16, value2 []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint64bytes(rawKey)
			value1, value2 := bytesTouint16bytes(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetUint64BytesToUint16Bytes is a get with key of type pair(uint64,[]byte) and value of type pair(uint16,[]byte).
func (b *Bucket) GetUint64BytesToUint16Bytes(key1 uint64, key2 []byte) (uint16, []byte) {
	return bytesTouint16bytes(b.Get(uint64bytesTobytes(key1, key2)))
}

// GetRequiredUint64BytesToUint16Bytes is a get with key of type pair(uint64,[]byte) and value of type pair(uint16,[]byte) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint64BytesToUint16Bytes(key1 uint64, key2 []byte) (uint16, []byte, error) {
	value := b.Get(uint64bytesTobytes(key1, key2))
	if value == nil {
		return 0, nil, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTouint16bytes(value)
	return value1, value2, nil
}

// PutUint64BytesToUint16Bytes is a put with key of type pair(uint64,[]byte) and value of type pair(uint16,[]byte).
func (b *Bucket) PutUint64BytesToUint16Bytes(key1 uint64, key2 []byte, value1 uint16, value2 []byte) error {
	return b.Put(uint64bytesTobytes(key1, key2), uint16bytesTobytes(value1, value2))
}

// FirstUint64BytesToUint16Bytes is first with key of type pair(uint64,[]byte) and value of type pair(uint16,[]byte).
func (c *Cursor) FirstUint64BytesToUint16Bytes() (uint64, []byte, uint16, []byte) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint64bytes(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key1, key2, value1, value2
}

// LastUint64BytesToUint16Bytes is first with key of type pair(uint64,[]byte) and value of type pair(uint16,[]byte).
func (c *Cursor) LastUint64BytesToUint16Bytes() (uint64, []byte, uint16, []byte) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint64bytes(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key1, key2, value1, value2
}

// NextUint64BytesToUint16Bytes is first with key of type pair(uint64,[]byte) and value of type pair(uint16,[]byte).
func (c *Cursor) NextUint64BytesToUint16Bytes() (uint64, []byte, uint16, []byte) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint64bytes(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key1, key2, value1, value2
}

// PrevUint64BytesToUint16Bytes is first with key of type pair(uint64,[]byte) and value of type pair(uint16,[]byte).
func (c *Cursor) PrevUint64BytesToUint16Bytes() (uint64, []byte, uint16, []byte) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint64bytes(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key1, key2, value1, value2
}

// SeekUint64BytesToUint16Bytes is first with key of type pair(uint64,[]byte) and value of type pair(uint16,[]byte).
func (c *Cursor) SeekUint64BytesToUint16Bytes(key1 uint64, key2 []byte) (uint64, []byte, uint16, []byte) {
	rawKey, rawValue := c.Seek(uint64bytesTobytes(key1, key2))
	key1, key2 = bytesTouint64bytes(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key1, key2, value1, value2
}

// ForEachUint64BytesToUint32Bytes is a for each with key of type pair(uint64,[]byte) and value of type pair(uint32,[]byte).
func (b *Bucket) ForEachUint64BytesToUint32Bytes(fn func(key1 uint64, key2 []byte, value1 uint32, value2 []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint64bytes(rawKey)
			value1, value2 := bytesTouint32bytes(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetUint64BytesToUint32Bytes is a get with key of type pair(uint64,[]byte) and value of type pair(uint32,[]byte).
func (b *Bucket) GetUint64BytesToUint32Bytes(key1 uint64, key2 []byte) (uint32, []byte) {
	return bytesTouint32bytes(b.Get(uint64bytesTobytes(key1, key2)))
}

// GetRequiredUint64BytesToUint32Bytes is a get with key of type pair(uint64,[]byte) and value of type pair(uint32,[]byte) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint64BytesToUint32Bytes(key1 uint64, key2 []byte) (uint32, []byte, error) {
	value := b.Get(uint64bytesTobytes(key1, key2))
	if value == nil {
		return 0, nil, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTouint32bytes(value)
	return value1, value2, nil
}

// PutUint64BytesToUint32Bytes is a put with key of type pair(uint64,[]byte) and value of type pair(uint32,[]byte).
func (b *Bucket) PutUint64BytesToUint32Bytes(key1 uint64, key2 []byte, value1 uint32, value2 []byte) error {
	return b.Put(uint64bytesTobytes(key1, key2), uint32bytesTobytes(value1, value2))
}

// FirstUint64BytesToUint32Bytes is first with key of type pair(uint64,[]byte) and value of type pair(uint32,[]byte).
func (c *Cursor) FirstUint64BytesToUint32Bytes() (uint64, []byte, uint32, []byte) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint64bytes(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key1, key2, value1, value2
}

// LastUint64BytesToUint32Bytes is first with key of type pair(uint64,[]byte) and value of type pair(uint32,[]byte).
func (c *Cursor) LastUint64BytesToUint32Bytes() (uint64, []byte, uint32, []byte) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint64bytes(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key1, key2, value1, value2
}

// NextUint64BytesToUint32Bytes is first with key of type pair(uint64,[]byte) and value of type pair(uint32,[]byte).
func (c *Cursor) NextUint64BytesToUint32Bytes() (uint64, []byte, uint32, []byte) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint64bytes(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key1, key2, value1, value2
}

// PrevUint64BytesToUint32Bytes is first with key of type pair(uint64,[]byte) and value of type pair(uint32,[]byte).
func (c *Cursor) PrevUint64BytesToUint32Bytes() (uint64, []byte, uint32, []byte) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint64bytes(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key1, key2, value1, value2
}

// SeekUint64BytesToUint32Bytes is first with key of type pair(uint64,[]byte) and value of type pair(uint32,[]byte).
func (c *Cursor) SeekUint64BytesToUint32Bytes(key1 uint64, key2 []byte) (uint64, []byte, uint32, []byte) {
	rawKey, rawValue := c.Seek(uint64bytesTobytes(key1, key2))
	key1, key2 = bytesTouint64bytes(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key1, key2, value1, value2
}

// ForEachUint64BytesToUint64Bytes is a for each with key of type pair(uint64,[]byte) and value of type pair(uint64,[]byte).
func (b *Bucket) ForEachUint64BytesToUint64Bytes(fn func(key1 uint64, key2 []byte, value1 uint64, value2 []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint64bytes(rawKey)
			value1, value2 := bytesTouint64bytes(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetUint64BytesToUint64Bytes is a get with key of type pair(uint64,[]byte) and value of type pair(uint64,[]byte).
func (b *Bucket) GetUint64BytesToUint64Bytes(key1 uint64, key2 []byte) (uint64, []byte) {
	return bytesTouint64bytes(b.Get(uint64bytesTobytes(key1, key2)))
}

// GetRequiredUint64BytesToUint64Bytes is a get with key of type pair(uint64,[]byte) and value of type pair(uint64,[]byte) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint64BytesToUint64Bytes(key1 uint64, key2 []byte) (uint64, []byte, error) {
	value := b.Get(uint64bytesTobytes(key1, key2))
	if value == nil {
		return 0, nil, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTouint64bytes(value)
	return value1, value2, nil
}

// PutUint64BytesToUint64Bytes is a put with key of type pair(uint64,[]byte) and value of type pair(uint64,[]byte).
func (b *Bucket) PutUint64BytesToUint64Bytes(key1 uint64, key2 []byte, value1 uint64, value2 []byte) error {
	return b.Put(uint64bytesTobytes(key1, key2), uint64bytesTobytes(value1, value2))
}

// FirstUint64BytesToUint64Bytes is first with key of type pair(uint64,[]byte) and value of type pair(uint64,[]byte).
func (c *Cursor) FirstUint64BytesToUint64Bytes() (uint64, []byte, uint64, []byte) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint64bytes(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key1, key2, value1, value2
}

// LastUint64BytesToUint64Bytes is first with key of type pair(uint64,[]byte) and value of type pair(uint64,[]byte).
func (c *Cursor) LastUint64BytesToUint64Bytes() (uint64, []byte, uint64, []byte) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint64bytes(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key1, key2, value1, value2
}

// NextUint64BytesToUint64Bytes is first with key of type pair(uint64,[]byte) and value of type pair(uint64,[]byte).
func (c *Cursor) NextUint64BytesToUint64Bytes() (uint64, []byte, uint64, []byte) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint64bytes(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key1, key2, value1, value2
}

// PrevUint64BytesToUint64Bytes is first with key of type pair(uint64,[]byte) and value of type pair(uint64,[]byte).
func (c *Cursor) PrevUint64BytesToUint64Bytes() (uint64, []byte, uint64, []byte) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint64bytes(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key1, key2, value1, value2
}

// SeekUint64BytesToUint64Bytes is first with key of type pair(uint64,[]byte) and value of type pair(uint64,[]byte).
func (c *Cursor) SeekUint64BytesToUint64Bytes(key1 uint64, key2 []byte) (uint64, []byte, uint64, []byte) {
	rawKey, rawValue := c.Seek(uint64bytesTobytes(key1, key2))
	key1, key2 = bytesTouint64bytes(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key1, key2, value1, value2
}

// ForEachUint64BytesToBytesUint16 is a for each with key of type pair(uint64,[]byte) and value of type pair([]byte,uint16).
func (b *Bucket) ForEachUint64BytesToBytesUint16(fn func(key1 uint64, key2 []byte, value1 []byte, value2 uint16) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint64bytes(rawKey)
			value1, value2 := bytesTobytesuint16(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetUint64BytesToBytesUint16 is a get with key of type pair(uint64,[]byte) and value of type pair([]byte,uint16).
func (b *Bucket) GetUint64BytesToBytesUint16(key1 uint64, key2 []byte) ([]byte, uint16) {
	return bytesTobytesuint16(b.Get(uint64bytesTobytes(key1, key2)))
}

// GetRequiredUint64BytesToBytesUint16 is a get with key of type pair(uint64,[]byte) and value of type pair([]byte,uint16) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint64BytesToBytesUint16(key1 uint64, key2 []byte) ([]byte, uint16, error) {
	value := b.Get(uint64bytesTobytes(key1, key2))
	if value == nil {
		return nil, 0, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTobytesuint16(value)
	return value1, value2, nil
}

// PutUint64BytesToBytesUint16 is a put with key of type pair(uint64,[]byte) and value of type pair([]byte,uint16).
func (b *Bucket) PutUint64BytesToBytesUint16(key1 uint64, key2 []byte, value1 []byte, value2 uint16) error {
	return b.Put(uint64bytesTobytes(key1, key2), bytesuint16Tobytes(value1, value2))
}

// FirstUint64BytesToBytesUint16 is first with key of type pair(uint64,[]byte) and value of type pair([]byte,uint16).
func (c *Cursor) FirstUint64BytesToBytesUint16() (uint64, []byte, []byte, uint16) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint64bytes(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key1, key2, value1, value2
}

// LastUint64BytesToBytesUint16 is first with key of type pair(uint64,[]byte) and value of type pair([]byte,uint16).
func (c *Cursor) LastUint64BytesToBytesUint16() (uint64, []byte, []byte, uint16) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint64bytes(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key1, key2, value1, value2
}

// NextUint64BytesToBytesUint16 is first with key of type pair(uint64,[]byte) and value of type pair([]byte,uint16).
func (c *Cursor) NextUint64BytesToBytesUint16() (uint64, []byte, []byte, uint16) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint64bytes(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key1, key2, value1, value2
}

// PrevUint64BytesToBytesUint16 is first with key of type pair(uint64,[]byte) and value of type pair([]byte,uint16).
func (c *Cursor) PrevUint64BytesToBytesUint16() (uint64, []byte, []byte, uint16) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint64bytes(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key1, key2, value1, value2
}

// SeekUint64BytesToBytesUint16 is first with key of type pair(uint64,[]byte) and value of type pair([]byte,uint16).
func (c *Cursor) SeekUint64BytesToBytesUint16(key1 uint64, key2 []byte) (uint64, []byte, []byte, uint16) {
	rawKey, rawValue := c.Seek(uint64bytesTobytes(key1, key2))
	key1, key2 = bytesTouint64bytes(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key1, key2, value1, value2
}

// ForEachUint64BytesToBytesUint32 is a for each with key of type pair(uint64,[]byte) and value of type pair([]byte,uint32).
func (b *Bucket) ForEachUint64BytesToBytesUint32(fn func(key1 uint64, key2 []byte, value1 []byte, value2 uint32) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint64bytes(rawKey)
			value1, value2 := bytesTobytesuint32(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetUint64BytesToBytesUint32 is a get with key of type pair(uint64,[]byte) and value of type pair([]byte,uint32).
func (b *Bucket) GetUint64BytesToBytesUint32(key1 uint64, key2 []byte) ([]byte, uint32) {
	return bytesTobytesuint32(b.Get(uint64bytesTobytes(key1, key2)))
}

// GetRequiredUint64BytesToBytesUint32 is a get with key of type pair(uint64,[]byte) and value of type pair([]byte,uint32) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint64BytesToBytesUint32(key1 uint64, key2 []byte) ([]byte, uint32, error) {
	value := b.Get(uint64bytesTobytes(key1, key2))
	if value == nil {
		return nil, 0, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTobytesuint32(value)
	return value1, value2, nil
}

// PutUint64BytesToBytesUint32 is a put with key of type pair(uint64,[]byte) and value of type pair([]byte,uint32).
func (b *Bucket) PutUint64BytesToBytesUint32(key1 uint64, key2 []byte, value1 []byte, value2 uint32) error {
	return b.Put(uint64bytesTobytes(key1, key2), bytesuint32Tobytes(value1, value2))
}

// FirstUint64BytesToBytesUint32 is first with key of type pair(uint64,[]byte) and value of type pair([]byte,uint32).
func (c *Cursor) FirstUint64BytesToBytesUint32() (uint64, []byte, []byte, uint32) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint64bytes(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key1, key2, value1, value2
}

// LastUint64BytesToBytesUint32 is first with key of type pair(uint64,[]byte) and value of type pair([]byte,uint32).
func (c *Cursor) LastUint64BytesToBytesUint32() (uint64, []byte, []byte, uint32) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint64bytes(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key1, key2, value1, value2
}

// NextUint64BytesToBytesUint32 is first with key of type pair(uint64,[]byte) and value of type pair([]byte,uint32).
func (c *Cursor) NextUint64BytesToBytesUint32() (uint64, []byte, []byte, uint32) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint64bytes(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key1, key2, value1, value2
}

// PrevUint64BytesToBytesUint32 is first with key of type pair(uint64,[]byte) and value of type pair([]byte,uint32).
func (c *Cursor) PrevUint64BytesToBytesUint32() (uint64, []byte, []byte, uint32) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint64bytes(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key1, key2, value1, value2
}

// SeekUint64BytesToBytesUint32 is first with key of type pair(uint64,[]byte) and value of type pair([]byte,uint32).
func (c *Cursor) SeekUint64BytesToBytesUint32(key1 uint64, key2 []byte) (uint64, []byte, []byte, uint32) {
	rawKey, rawValue := c.Seek(uint64bytesTobytes(key1, key2))
	key1, key2 = bytesTouint64bytes(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key1, key2, value1, value2
}

// ForEachUint64BytesToBytesUint64 is a for each with key of type pair(uint64,[]byte) and value of type pair([]byte,uint64).
func (b *Bucket) ForEachUint64BytesToBytesUint64(fn func(key1 uint64, key2 []byte, value1 []byte, value2 uint64) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTouint64bytes(rawKey)
			value1, value2 := bytesTobytesuint64(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetUint64BytesToBytesUint64 is a get with key of type pair(uint64,[]byte) and value of type pair([]byte,uint64).
func (b *Bucket) GetUint64BytesToBytesUint64(key1 uint64, key2 []byte) ([]byte, uint64) {
	return bytesTobytesuint64(b.Get(uint64bytesTobytes(key1, key2)))
}

// GetRequiredUint64BytesToBytesUint64 is a get with key of type pair(uint64,[]byte) and value of type pair([]byte,uint64) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredUint64BytesToBytesUint64(key1 uint64, key2 []byte) ([]byte, uint64, error) {
	value := b.Get(uint64bytesTobytes(key1, key2))
	if value == nil {
		return nil, 0, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTobytesuint64(value)
	return value1, value2, nil
}

// PutUint64BytesToBytesUint64 is a put with key of type pair(uint64,[]byte) and value of type pair([]byte,uint64).
func (b *Bucket) PutUint64BytesToBytesUint64(key1 uint64, key2 []byte, value1 []byte, value2 uint64) error {
	return b.Put(uint64bytesTobytes(key1, key2), bytesuint64Tobytes(value1, value2))
}

// FirstUint64BytesToBytesUint64 is first with key of type pair(uint64,[]byte) and value of type pair([]byte,uint64).
func (c *Cursor) FirstUint64BytesToBytesUint64() (uint64, []byte, []byte, uint64) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTouint64bytes(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key1, key2, value1, value2
}

// LastUint64BytesToBytesUint64 is first with key of type pair(uint64,[]byte) and value of type pair([]byte,uint64).
func (c *Cursor) LastUint64BytesToBytesUint64() (uint64, []byte, []byte, uint64) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTouint64bytes(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key1, key2, value1, value2
}

// NextUint64BytesToBytesUint64 is first with key of type pair(uint64,[]byte) and value of type pair([]byte,uint64).
func (c *Cursor) NextUint64BytesToBytesUint64() (uint64, []byte, []byte, uint64) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTouint64bytes(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key1, key2, value1, value2
}

// PrevUint64BytesToBytesUint64 is first with key of type pair(uint64,[]byte) and value of type pair([]byte,uint64).
func (c *Cursor) PrevUint64BytesToBytesUint64() (uint64, []byte, []byte, uint64) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTouint64bytes(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key1, key2, value1, value2
}

// SeekUint64BytesToBytesUint64 is first with key of type pair(uint64,[]byte) and value of type pair([]byte,uint64).
func (c *Cursor) SeekUint64BytesToBytesUint64(key1 uint64, key2 []byte) (uint64, []byte, []byte, uint64) {
	rawKey, rawValue := c.Seek(uint64bytesTobytes(key1, key2))
	key1, key2 = bytesTouint64bytes(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key1, key2, value1, value2
}

// DeleteUint64Bytes is a delete with key of type pair(uint64,[]byte).
func (b *Bucket) DeleteUint64Bytes(key1 uint64, key2 []byte) error {
	return b.Delete(uint64bytesTobytes(key1, key2))
}

// ForEachBytesUint16ToBytes is a for each with key of type pair([]byte,uint16) and value of type []byte.
func (b *Bucket) ForEachBytesUint16ToBytes(fn func(key1 []byte, key2 uint16, value []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTobytesuint16(rawKey)
			value := (rawValue)
			return fn(key1, key2, value)
		},
	)
}

// GetBytesUint16ToBytes is a get with key of type pair([]byte,uint16) and value of type []byte.
func (b *Bucket) GetBytesUint16ToBytes(key1 []byte, key2 uint16) []byte {
	return (b.Get(bytesuint16Tobytes(key1, key2)))
}

// GetRequiredBytesUint16ToBytes is a get with key of type pair([]byte,uint16) and value of type []byte that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesUint16ToBytes(key1 []byte, key2 uint16) ([]byte, error) {
	value := b.Get(bytesuint16Tobytes(key1, key2))
	if value == nil {
		return nil, NewKeyNotFoundError(key1, key2)
	}
	return (value), nil
}

// PutBytesUint16ToBytes is a put with key of type pair([]byte,uint16) and value of type []byte.
func (b *Bucket) PutBytesUint16ToBytes(key1 []byte, key2 uint16, value []byte) error {
	return b.Put(bytesuint16Tobytes(key1, key2), (value))
}

// FirstBytesUint16ToBytes is first with key of type pair([]byte,uint16) and value of type []byte.
func (c *Cursor) FirstBytesUint16ToBytes() ([]byte, uint16, []byte) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTobytesuint16(rawKey)
	value := (rawValue)
	return key1, key2, value
}

// LastBytesUint16ToBytes is first with key of type pair([]byte,uint16) and value of type []byte.
func (c *Cursor) LastBytesUint16ToBytes() ([]byte, uint16, []byte) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTobytesuint16(rawKey)
	value := (rawValue)
	return key1, key2, value
}

// NextBytesUint16ToBytes is first with key of type pair([]byte,uint16) and value of type []byte.
func (c *Cursor) NextBytesUint16ToBytes() ([]byte, uint16, []byte) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTobytesuint16(rawKey)
	value := (rawValue)
	return key1, key2, value
}

// PrevBytesUint16ToBytes is first with key of type pair([]byte,uint16) and value of type []byte.
func (c *Cursor) PrevBytesUint16ToBytes() ([]byte, uint16, []byte) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTobytesuint16(rawKey)
	value := (rawValue)
	return key1, key2, value
}

// SeekBytesUint16ToBytes is first with key of type pair([]byte,uint16) and value of type []byte.
func (c *Cursor) SeekBytesUint16ToBytes(key1 []byte, key2 uint16) ([]byte, uint16, []byte) {
	rawKey, rawValue := c.Seek(bytesuint16Tobytes(key1, key2))
	key1, key2 = bytesTobytesuint16(rawKey)
	value := (rawValue)
	return key1, key2, value
}

// ForEachBytesUint16ToString is a for each with key of type pair([]byte,uint16) and value of type string.
func (b *Bucket) ForEachBytesUint16ToString(fn func(key1 []byte, key2 uint16, value string) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTobytesuint16(rawKey)
			value := string(rawValue)
			return fn(key1, key2, value)
		},
	)
}

// GetBytesUint16ToString is a get with key of type pair([]byte,uint16) and value of type string.
func (b *Bucket) GetBytesUint16ToString(key1 []byte, key2 uint16) string {
	return string(b.Get(bytesuint16Tobytes(key1, key2)))
}

// GetRequiredBytesUint16ToString is a get with key of type pair([]byte,uint16) and value of type string that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesUint16ToString(key1 []byte, key2 uint16) (string, error) {
	value := b.Get(bytesuint16Tobytes(key1, key2))
	if value == nil {
		return "", NewKeyNotFoundError(key1, key2)
	}
	return string(value), nil
}

// PutBytesUint16ToString is a put with key of type pair([]byte,uint16) and value of type string.
func (b *Bucket) PutBytesUint16ToString(key1 []byte, key2 uint16, value string) error {
	return b.Put(bytesuint16Tobytes(key1, key2), []byte(value))
}

// FirstBytesUint16ToString is first with key of type pair([]byte,uint16) and value of type string.
func (c *Cursor) FirstBytesUint16ToString() ([]byte, uint16, string) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTobytesuint16(rawKey)
	value := string(rawValue)
	return key1, key2, value
}

// LastBytesUint16ToString is first with key of type pair([]byte,uint16) and value of type string.
func (c *Cursor) LastBytesUint16ToString() ([]byte, uint16, string) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTobytesuint16(rawKey)
	value := string(rawValue)
	return key1, key2, value
}

// NextBytesUint16ToString is first with key of type pair([]byte,uint16) and value of type string.
func (c *Cursor) NextBytesUint16ToString() ([]byte, uint16, string) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTobytesuint16(rawKey)
	value := string(rawValue)
	return key1, key2, value
}

// PrevBytesUint16ToString is first with key of type pair([]byte,uint16) and value of type string.
func (c *Cursor) PrevBytesUint16ToString() ([]byte, uint16, string) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTobytesuint16(rawKey)
	value := string(rawValue)
	return key1, key2, value
}

// SeekBytesUint16ToString is first with key of type pair([]byte,uint16) and value of type string.
func (c *Cursor) SeekBytesUint16ToString(key1 []byte, key2 uint16) ([]byte, uint16, string) {
	rawKey, rawValue := c.Seek(bytesuint16Tobytes(key1, key2))
	key1, key2 = bytesTobytesuint16(rawKey)
	value := string(rawValue)
	return key1, key2, value
}

// ForEachBytesUint16ToUint16 is a for each with key of type pair([]byte,uint16) and value of type uint16.
func (b *Bucket) ForEachBytesUint16ToUint16(fn func(key1 []byte, key2 uint16, value uint16) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTobytesuint16(rawKey)
			value := bytesTouint16(rawValue)
			return fn(key1, key2, value)
		},
	)
}

// GetBytesUint16ToUint16 is a get with key of type pair([]byte,uint16) and value of type uint16.
func (b *Bucket) GetBytesUint16ToUint16(key1 []byte, key2 uint16) uint16 {
	return bytesTouint16(b.Get(bytesuint16Tobytes(key1, key2)))
}

// GetRequiredBytesUint16ToUint16 is a get with key of type pair([]byte,uint16) and value of type uint16 that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesUint16ToUint16(key1 []byte, key2 uint16) (uint16, error) {
	value := b.Get(bytesuint16Tobytes(key1, key2))
	if value == nil {
		return 0, NewKeyNotFoundError(key1, key2)
	}
	return bytesTouint16(value), nil
}

// PutBytesUint16ToUint16 is a put with key of type pair([]byte,uint16) and value of type uint16.
func (b *Bucket) PutBytesUint16ToUint16(key1 []byte, key2 uint16, value uint16) error {
	return b.Put(bytesuint16Tobytes(key1, key2), uint16Tobytes(value))
}

// FirstBytesUint16ToUint16 is first with key of type pair([]byte,uint16) and value of type uint16.
func (c *Cursor) FirstBytesUint16ToUint16() ([]byte, uint16, uint16) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTobytesuint16(rawKey)
	value := bytesTouint16(rawValue)
	return key1, key2, value
}

// LastBytesUint16ToUint16 is first with key of type pair([]byte,uint16) and value of type uint16.
func (c *Cursor) LastBytesUint16ToUint16() ([]byte, uint16, uint16) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTobytesuint16(rawKey)
	value := bytesTouint16(rawValue)
	return key1, key2, value
}

// NextBytesUint16ToUint16 is first with key of type pair([]byte,uint16) and value of type uint16.
func (c *Cursor) NextBytesUint16ToUint16() ([]byte, uint16, uint16) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTobytesuint16(rawKey)
	value := bytesTouint16(rawValue)
	return key1, key2, value
}

// PrevBytesUint16ToUint16 is first with key of type pair([]byte,uint16) and value of type uint16.
func (c *Cursor) PrevBytesUint16ToUint16() ([]byte, uint16, uint16) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTobytesuint16(rawKey)
	value := bytesTouint16(rawValue)
	return key1, key2, value
}

// SeekBytesUint16ToUint16 is first with key of type pair([]byte,uint16) and value of type uint16.
func (c *Cursor) SeekBytesUint16ToUint16(key1 []byte, key2 uint16) ([]byte, uint16, uint16) {
	rawKey, rawValue := c.Seek(bytesuint16Tobytes(key1, key2))
	key1, key2 = bytesTobytesuint16(rawKey)
	value := bytesTouint16(rawValue)
	return key1, key2, value
}

// ForEachBytesUint16ToUint32 is a for each with key of type pair([]byte,uint16) and value of type uint32.
func (b *Bucket) ForEachBytesUint16ToUint32(fn func(key1 []byte, key2 uint16, value uint32) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTobytesuint16(rawKey)
			value := bytesTouint32(rawValue)
			return fn(key1, key2, value)
		},
	)
}

// GetBytesUint16ToUint32 is a get with key of type pair([]byte,uint16) and value of type uint32.
func (b *Bucket) GetBytesUint16ToUint32(key1 []byte, key2 uint16) uint32 {
	return bytesTouint32(b.Get(bytesuint16Tobytes(key1, key2)))
}

// GetRequiredBytesUint16ToUint32 is a get with key of type pair([]byte,uint16) and value of type uint32 that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesUint16ToUint32(key1 []byte, key2 uint16) (uint32, error) {
	value := b.Get(bytesuint16Tobytes(key1, key2))
	if value == nil {
		return 0, NewKeyNotFoundError(key1, key2)
	}
	return bytesTouint32(value), nil
}

// PutBytesUint16ToUint32 is a put with key of type pair([]byte,uint16) and value of type uint32.
func (b *Bucket) PutBytesUint16ToUint32(key1 []byte, key2 uint16, value uint32) error {
	return b.Put(bytesuint16Tobytes(key1, key2), uint32Tobytes(value))
}

// FirstBytesUint16ToUint32 is first with key of type pair([]byte,uint16) and value of type uint32.
func (c *Cursor) FirstBytesUint16ToUint32() ([]byte, uint16, uint32) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTobytesuint16(rawKey)
	value := bytesTouint32(rawValue)
	return key1, key2, value
}

// LastBytesUint16ToUint32 is first with key of type pair([]byte,uint16) and value of type uint32.
func (c *Cursor) LastBytesUint16ToUint32() ([]byte, uint16, uint32) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTobytesuint16(rawKey)
	value := bytesTouint32(rawValue)
	return key1, key2, value
}

// NextBytesUint16ToUint32 is first with key of type pair([]byte,uint16) and value of type uint32.
func (c *Cursor) NextBytesUint16ToUint32() ([]byte, uint16, uint32) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTobytesuint16(rawKey)
	value := bytesTouint32(rawValue)
	return key1, key2, value
}

// PrevBytesUint16ToUint32 is first with key of type pair([]byte,uint16) and value of type uint32.
func (c *Cursor) PrevBytesUint16ToUint32() ([]byte, uint16, uint32) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTobytesuint16(rawKey)
	value := bytesTouint32(rawValue)
	return key1, key2, value
}

// SeekBytesUint16ToUint32 is first with key of type pair([]byte,uint16) and value of type uint32.
func (c *Cursor) SeekBytesUint16ToUint32(key1 []byte, key2 uint16) ([]byte, uint16, uint32) {
	rawKey, rawValue := c.Seek(bytesuint16Tobytes(key1, key2))
	key1, key2 = bytesTobytesuint16(rawKey)
	value := bytesTouint32(rawValue)
	return key1, key2, value
}

// ForEachBytesUint16ToUint64 is a for each with key of type pair([]byte,uint16) and value of type uint64.
func (b *Bucket) ForEachBytesUint16ToUint64(fn func(key1 []byte, key2 uint16, value uint64) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTobytesuint16(rawKey)
			value := bytesTouint64(rawValue)
			return fn(key1, key2, value)
		},
	)
}

// GetBytesUint16ToUint64 is a get with key of type pair([]byte,uint16) and value of type uint64.
func (b *Bucket) GetBytesUint16ToUint64(key1 []byte, key2 uint16) uint64 {
	return bytesTouint64(b.Get(bytesuint16Tobytes(key1, key2)))
}

// GetRequiredBytesUint16ToUint64 is a get with key of type pair([]byte,uint16) and value of type uint64 that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesUint16ToUint64(key1 []byte, key2 uint16) (uint64, error) {
	value := b.Get(bytesuint16Tobytes(key1, key2))
	if value == nil {
		return 0, NewKeyNotFoundError(key1, key2)
	}
	return bytesTouint64(value), nil
}

// PutBytesUint16ToUint64 is a put with key of type pair([]byte,uint16) and value of type uint64.
func (b *Bucket) PutBytesUint16ToUint64(key1 []byte, key2 uint16, value uint64) error {
	return b.Put(bytesuint16Tobytes(key1, key2), uint64Tobytes(value))
}

// FirstBytesUint16ToUint64 is first with key of type pair([]byte,uint16) and value of type uint64.
func (c *Cursor) FirstBytesUint16ToUint64() ([]byte, uint16, uint64) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTobytesuint16(rawKey)
	value := bytesTouint64(rawValue)
	return key1, key2, value
}

// LastBytesUint16ToUint64 is first with key of type pair([]byte,uint16) and value of type uint64.
func (c *Cursor) LastBytesUint16ToUint64() ([]byte, uint16, uint64) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTobytesuint16(rawKey)
	value := bytesTouint64(rawValue)
	return key1, key2, value
}

// NextBytesUint16ToUint64 is first with key of type pair([]byte,uint16) and value of type uint64.
func (c *Cursor) NextBytesUint16ToUint64() ([]byte, uint16, uint64) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTobytesuint16(rawKey)
	value := bytesTouint64(rawValue)
	return key1, key2, value
}

// PrevBytesUint16ToUint64 is first with key of type pair([]byte,uint16) and value of type uint64.
func (c *Cursor) PrevBytesUint16ToUint64() ([]byte, uint16, uint64) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTobytesuint16(rawKey)
	value := bytesTouint64(rawValue)
	return key1, key2, value
}

// SeekBytesUint16ToUint64 is first with key of type pair([]byte,uint16) and value of type uint64.
func (c *Cursor) SeekBytesUint16ToUint64(key1 []byte, key2 uint16) ([]byte, uint16, uint64) {
	rawKey, rawValue := c.Seek(bytesuint16Tobytes(key1, key2))
	key1, key2 = bytesTobytesuint16(rawKey)
	value := bytesTouint64(rawValue)
	return key1, key2, value
}

// ForEachBytesUint16ToUint16Uint16 is a for each with key of type pair([]byte,uint16) and value of type pair(uint16,uint16).
func (b *Bucket) ForEachBytesUint16ToUint16Uint16(fn func(key1 []byte, key2 uint16, value1 uint16, value2 uint16) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTobytesuint16(rawKey)
			value1, value2 := bytesTouint16uint16(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetBytesUint16ToUint16Uint16 is a get with key of type pair([]byte,uint16) and value of type pair(uint16,uint16).
func (b *Bucket) GetBytesUint16ToUint16Uint16(key1 []byte, key2 uint16) (uint16, uint16) {
	return bytesTouint16uint16(b.Get(bytesuint16Tobytes(key1, key2)))
}

// GetRequiredBytesUint16ToUint16Uint16 is a get with key of type pair([]byte,uint16) and value of type pair(uint16,uint16) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesUint16ToUint16Uint16(key1 []byte, key2 uint16) (uint16, uint16, error) {
	value := b.Get(bytesuint16Tobytes(key1, key2))
	if value == nil {
		return 0, 0, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTouint16uint16(value)
	return value1, value2, nil
}

// PutBytesUint16ToUint16Uint16 is a put with key of type pair([]byte,uint16) and value of type pair(uint16,uint16).
func (b *Bucket) PutBytesUint16ToUint16Uint16(key1 []byte, key2 uint16, value1 uint16, value2 uint16) error {
	return b.Put(bytesuint16Tobytes(key1, key2), uint16uint16Tobytes(value1, value2))
}

// FirstBytesUint16ToUint16Uint16 is first with key of type pair([]byte,uint16) and value of type pair(uint16,uint16).
func (c *Cursor) FirstBytesUint16ToUint16Uint16() ([]byte, uint16, uint16, uint16) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTobytesuint16(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key1, key2, value1, value2
}

// LastBytesUint16ToUint16Uint16 is first with key of type pair([]byte,uint16) and value of type pair(uint16,uint16).
func (c *Cursor) LastBytesUint16ToUint16Uint16() ([]byte, uint16, uint16, uint16) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTobytesuint16(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key1, key2, value1, value2
}

// NextBytesUint16ToUint16Uint16 is first with key of type pair([]byte,uint16) and value of type pair(uint16,uint16).
func (c *Cursor) NextBytesUint16ToUint16Uint16() ([]byte, uint16, uint16, uint16) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTobytesuint16(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key1, key2, value1, value2
}

// PrevBytesUint16ToUint16Uint16 is first with key of type pair([]byte,uint16) and value of type pair(uint16,uint16).
func (c *Cursor) PrevBytesUint16ToUint16Uint16() ([]byte, uint16, uint16, uint16) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTobytesuint16(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key1, key2, value1, value2
}

// SeekBytesUint16ToUint16Uint16 is first with key of type pair([]byte,uint16) and value of type pair(uint16,uint16).
func (c *Cursor) SeekBytesUint16ToUint16Uint16(key1 []byte, key2 uint16) ([]byte, uint16, uint16, uint16) {
	rawKey, rawValue := c.Seek(bytesuint16Tobytes(key1, key2))
	key1, key2 = bytesTobytesuint16(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key1, key2, value1, value2
}

// ForEachBytesUint16ToUint32Uint32 is a for each with key of type pair([]byte,uint16) and value of type pair(uint32,uint32).
func (b *Bucket) ForEachBytesUint16ToUint32Uint32(fn func(key1 []byte, key2 uint16, value1 uint32, value2 uint32) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTobytesuint16(rawKey)
			value1, value2 := bytesTouint32uint32(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetBytesUint16ToUint32Uint32 is a get with key of type pair([]byte,uint16) and value of type pair(uint32,uint32).
func (b *Bucket) GetBytesUint16ToUint32Uint32(key1 []byte, key2 uint16) (uint32, uint32) {
	return bytesTouint32uint32(b.Get(bytesuint16Tobytes(key1, key2)))
}

// GetRequiredBytesUint16ToUint32Uint32 is a get with key of type pair([]byte,uint16) and value of type pair(uint32,uint32) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesUint16ToUint32Uint32(key1 []byte, key2 uint16) (uint32, uint32, error) {
	value := b.Get(bytesuint16Tobytes(key1, key2))
	if value == nil {
		return 0, 0, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTouint32uint32(value)
	return value1, value2, nil
}

// PutBytesUint16ToUint32Uint32 is a put with key of type pair([]byte,uint16) and value of type pair(uint32,uint32).
func (b *Bucket) PutBytesUint16ToUint32Uint32(key1 []byte, key2 uint16, value1 uint32, value2 uint32) error {
	return b.Put(bytesuint16Tobytes(key1, key2), uint32uint32Tobytes(value1, value2))
}

// FirstBytesUint16ToUint32Uint32 is first with key of type pair([]byte,uint16) and value of type pair(uint32,uint32).
func (c *Cursor) FirstBytesUint16ToUint32Uint32() ([]byte, uint16, uint32, uint32) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTobytesuint16(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key1, key2, value1, value2
}

// LastBytesUint16ToUint32Uint32 is first with key of type pair([]byte,uint16) and value of type pair(uint32,uint32).
func (c *Cursor) LastBytesUint16ToUint32Uint32() ([]byte, uint16, uint32, uint32) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTobytesuint16(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key1, key2, value1, value2
}

// NextBytesUint16ToUint32Uint32 is first with key of type pair([]byte,uint16) and value of type pair(uint32,uint32).
func (c *Cursor) NextBytesUint16ToUint32Uint32() ([]byte, uint16, uint32, uint32) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTobytesuint16(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key1, key2, value1, value2
}

// PrevBytesUint16ToUint32Uint32 is first with key of type pair([]byte,uint16) and value of type pair(uint32,uint32).
func (c *Cursor) PrevBytesUint16ToUint32Uint32() ([]byte, uint16, uint32, uint32) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTobytesuint16(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key1, key2, value1, value2
}

// SeekBytesUint16ToUint32Uint32 is first with key of type pair([]byte,uint16) and value of type pair(uint32,uint32).
func (c *Cursor) SeekBytesUint16ToUint32Uint32(key1 []byte, key2 uint16) ([]byte, uint16, uint32, uint32) {
	rawKey, rawValue := c.Seek(bytesuint16Tobytes(key1, key2))
	key1, key2 = bytesTobytesuint16(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key1, key2, value1, value2
}

// ForEachBytesUint16ToUint64Uint64 is a for each with key of type pair([]byte,uint16) and value of type pair(uint64,uint64).
func (b *Bucket) ForEachBytesUint16ToUint64Uint64(fn func(key1 []byte, key2 uint16, value1 uint64, value2 uint64) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTobytesuint16(rawKey)
			value1, value2 := bytesTouint64uint64(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetBytesUint16ToUint64Uint64 is a get with key of type pair([]byte,uint16) and value of type pair(uint64,uint64).
func (b *Bucket) GetBytesUint16ToUint64Uint64(key1 []byte, key2 uint16) (uint64, uint64) {
	return bytesTouint64uint64(b.Get(bytesuint16Tobytes(key1, key2)))
}

// GetRequiredBytesUint16ToUint64Uint64 is a get with key of type pair([]byte,uint16) and value of type pair(uint64,uint64) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesUint16ToUint64Uint64(key1 []byte, key2 uint16) (uint64, uint64, error) {
	value := b.Get(bytesuint16Tobytes(key1, key2))
	if value == nil {
		return 0, 0, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTouint64uint64(value)
	return value1, value2, nil
}

// PutBytesUint16ToUint64Uint64 is a put with key of type pair([]byte,uint16) and value of type pair(uint64,uint64).
func (b *Bucket) PutBytesUint16ToUint64Uint64(key1 []byte, key2 uint16, value1 uint64, value2 uint64) error {
	return b.Put(bytesuint16Tobytes(key1, key2), uint64uint64Tobytes(value1, value2))
}

// FirstBytesUint16ToUint64Uint64 is first with key of type pair([]byte,uint16) and value of type pair(uint64,uint64).
func (c *Cursor) FirstBytesUint16ToUint64Uint64() ([]byte, uint16, uint64, uint64) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTobytesuint16(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key1, key2, value1, value2
}

// LastBytesUint16ToUint64Uint64 is first with key of type pair([]byte,uint16) and value of type pair(uint64,uint64).
func (c *Cursor) LastBytesUint16ToUint64Uint64() ([]byte, uint16, uint64, uint64) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTobytesuint16(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key1, key2, value1, value2
}

// NextBytesUint16ToUint64Uint64 is first with key of type pair([]byte,uint16) and value of type pair(uint64,uint64).
func (c *Cursor) NextBytesUint16ToUint64Uint64() ([]byte, uint16, uint64, uint64) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTobytesuint16(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key1, key2, value1, value2
}

// PrevBytesUint16ToUint64Uint64 is first with key of type pair([]byte,uint16) and value of type pair(uint64,uint64).
func (c *Cursor) PrevBytesUint16ToUint64Uint64() ([]byte, uint16, uint64, uint64) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTobytesuint16(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key1, key2, value1, value2
}

// SeekBytesUint16ToUint64Uint64 is first with key of type pair([]byte,uint16) and value of type pair(uint64,uint64).
func (c *Cursor) SeekBytesUint16ToUint64Uint64(key1 []byte, key2 uint16) ([]byte, uint16, uint64, uint64) {
	rawKey, rawValue := c.Seek(bytesuint16Tobytes(key1, key2))
	key1, key2 = bytesTobytesuint16(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key1, key2, value1, value2
}

// ForEachBytesUint16ToUint16Bytes is a for each with key of type pair([]byte,uint16) and value of type pair(uint16,[]byte).
func (b *Bucket) ForEachBytesUint16ToUint16Bytes(fn func(key1 []byte, key2 uint16, value1 uint16, value2 []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTobytesuint16(rawKey)
			value1, value2 := bytesTouint16bytes(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetBytesUint16ToUint16Bytes is a get with key of type pair([]byte,uint16) and value of type pair(uint16,[]byte).
func (b *Bucket) GetBytesUint16ToUint16Bytes(key1 []byte, key2 uint16) (uint16, []byte) {
	return bytesTouint16bytes(b.Get(bytesuint16Tobytes(key1, key2)))
}

// GetRequiredBytesUint16ToUint16Bytes is a get with key of type pair([]byte,uint16) and value of type pair(uint16,[]byte) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesUint16ToUint16Bytes(key1 []byte, key2 uint16) (uint16, []byte, error) {
	value := b.Get(bytesuint16Tobytes(key1, key2))
	if value == nil {
		return 0, nil, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTouint16bytes(value)
	return value1, value2, nil
}

// PutBytesUint16ToUint16Bytes is a put with key of type pair([]byte,uint16) and value of type pair(uint16,[]byte).
func (b *Bucket) PutBytesUint16ToUint16Bytes(key1 []byte, key2 uint16, value1 uint16, value2 []byte) error {
	return b.Put(bytesuint16Tobytes(key1, key2), uint16bytesTobytes(value1, value2))
}

// FirstBytesUint16ToUint16Bytes is first with key of type pair([]byte,uint16) and value of type pair(uint16,[]byte).
func (c *Cursor) FirstBytesUint16ToUint16Bytes() ([]byte, uint16, uint16, []byte) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTobytesuint16(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key1, key2, value1, value2
}

// LastBytesUint16ToUint16Bytes is first with key of type pair([]byte,uint16) and value of type pair(uint16,[]byte).
func (c *Cursor) LastBytesUint16ToUint16Bytes() ([]byte, uint16, uint16, []byte) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTobytesuint16(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key1, key2, value1, value2
}

// NextBytesUint16ToUint16Bytes is first with key of type pair([]byte,uint16) and value of type pair(uint16,[]byte).
func (c *Cursor) NextBytesUint16ToUint16Bytes() ([]byte, uint16, uint16, []byte) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTobytesuint16(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key1, key2, value1, value2
}

// PrevBytesUint16ToUint16Bytes is first with key of type pair([]byte,uint16) and value of type pair(uint16,[]byte).
func (c *Cursor) PrevBytesUint16ToUint16Bytes() ([]byte, uint16, uint16, []byte) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTobytesuint16(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key1, key2, value1, value2
}

// SeekBytesUint16ToUint16Bytes is first with key of type pair([]byte,uint16) and value of type pair(uint16,[]byte).
func (c *Cursor) SeekBytesUint16ToUint16Bytes(key1 []byte, key2 uint16) ([]byte, uint16, uint16, []byte) {
	rawKey, rawValue := c.Seek(bytesuint16Tobytes(key1, key2))
	key1, key2 = bytesTobytesuint16(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key1, key2, value1, value2
}

// ForEachBytesUint16ToUint32Bytes is a for each with key of type pair([]byte,uint16) and value of type pair(uint32,[]byte).
func (b *Bucket) ForEachBytesUint16ToUint32Bytes(fn func(key1 []byte, key2 uint16, value1 uint32, value2 []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTobytesuint16(rawKey)
			value1, value2 := bytesTouint32bytes(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetBytesUint16ToUint32Bytes is a get with key of type pair([]byte,uint16) and value of type pair(uint32,[]byte).
func (b *Bucket) GetBytesUint16ToUint32Bytes(key1 []byte, key2 uint16) (uint32, []byte) {
	return bytesTouint32bytes(b.Get(bytesuint16Tobytes(key1, key2)))
}

// GetRequiredBytesUint16ToUint32Bytes is a get with key of type pair([]byte,uint16) and value of type pair(uint32,[]byte) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesUint16ToUint32Bytes(key1 []byte, key2 uint16) (uint32, []byte, error) {
	value := b.Get(bytesuint16Tobytes(key1, key2))
	if value == nil {
		return 0, nil, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTouint32bytes(value)
	return value1, value2, nil
}

// PutBytesUint16ToUint32Bytes is a put with key of type pair([]byte,uint16) and value of type pair(uint32,[]byte).
func (b *Bucket) PutBytesUint16ToUint32Bytes(key1 []byte, key2 uint16, value1 uint32, value2 []byte) error {
	return b.Put(bytesuint16Tobytes(key1, key2), uint32bytesTobytes(value1, value2))
}

// FirstBytesUint16ToUint32Bytes is first with key of type pair([]byte,uint16) and value of type pair(uint32,[]byte).
func (c *Cursor) FirstBytesUint16ToUint32Bytes() ([]byte, uint16, uint32, []byte) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTobytesuint16(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key1, key2, value1, value2
}

// LastBytesUint16ToUint32Bytes is first with key of type pair([]byte,uint16) and value of type pair(uint32,[]byte).
func (c *Cursor) LastBytesUint16ToUint32Bytes() ([]byte, uint16, uint32, []byte) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTobytesuint16(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key1, key2, value1, value2
}

// NextBytesUint16ToUint32Bytes is first with key of type pair([]byte,uint16) and value of type pair(uint32,[]byte).
func (c *Cursor) NextBytesUint16ToUint32Bytes() ([]byte, uint16, uint32, []byte) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTobytesuint16(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key1, key2, value1, value2
}

// PrevBytesUint16ToUint32Bytes is first with key of type pair([]byte,uint16) and value of type pair(uint32,[]byte).
func (c *Cursor) PrevBytesUint16ToUint32Bytes() ([]byte, uint16, uint32, []byte) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTobytesuint16(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key1, key2, value1, value2
}

// SeekBytesUint16ToUint32Bytes is first with key of type pair([]byte,uint16) and value of type pair(uint32,[]byte).
func (c *Cursor) SeekBytesUint16ToUint32Bytes(key1 []byte, key2 uint16) ([]byte, uint16, uint32, []byte) {
	rawKey, rawValue := c.Seek(bytesuint16Tobytes(key1, key2))
	key1, key2 = bytesTobytesuint16(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key1, key2, value1, value2
}

// ForEachBytesUint16ToUint64Bytes is a for each with key of type pair([]byte,uint16) and value of type pair(uint64,[]byte).
func (b *Bucket) ForEachBytesUint16ToUint64Bytes(fn func(key1 []byte, key2 uint16, value1 uint64, value2 []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTobytesuint16(rawKey)
			value1, value2 := bytesTouint64bytes(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetBytesUint16ToUint64Bytes is a get with key of type pair([]byte,uint16) and value of type pair(uint64,[]byte).
func (b *Bucket) GetBytesUint16ToUint64Bytes(key1 []byte, key2 uint16) (uint64, []byte) {
	return bytesTouint64bytes(b.Get(bytesuint16Tobytes(key1, key2)))
}

// GetRequiredBytesUint16ToUint64Bytes is a get with key of type pair([]byte,uint16) and value of type pair(uint64,[]byte) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesUint16ToUint64Bytes(key1 []byte, key2 uint16) (uint64, []byte, error) {
	value := b.Get(bytesuint16Tobytes(key1, key2))
	if value == nil {
		return 0, nil, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTouint64bytes(value)
	return value1, value2, nil
}

// PutBytesUint16ToUint64Bytes is a put with key of type pair([]byte,uint16) and value of type pair(uint64,[]byte).
func (b *Bucket) PutBytesUint16ToUint64Bytes(key1 []byte, key2 uint16, value1 uint64, value2 []byte) error {
	return b.Put(bytesuint16Tobytes(key1, key2), uint64bytesTobytes(value1, value2))
}

// FirstBytesUint16ToUint64Bytes is first with key of type pair([]byte,uint16) and value of type pair(uint64,[]byte).
func (c *Cursor) FirstBytesUint16ToUint64Bytes() ([]byte, uint16, uint64, []byte) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTobytesuint16(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key1, key2, value1, value2
}

// LastBytesUint16ToUint64Bytes is first with key of type pair([]byte,uint16) and value of type pair(uint64,[]byte).
func (c *Cursor) LastBytesUint16ToUint64Bytes() ([]byte, uint16, uint64, []byte) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTobytesuint16(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key1, key2, value1, value2
}

// NextBytesUint16ToUint64Bytes is first with key of type pair([]byte,uint16) and value of type pair(uint64,[]byte).
func (c *Cursor) NextBytesUint16ToUint64Bytes() ([]byte, uint16, uint64, []byte) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTobytesuint16(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key1, key2, value1, value2
}

// PrevBytesUint16ToUint64Bytes is first with key of type pair([]byte,uint16) and value of type pair(uint64,[]byte).
func (c *Cursor) PrevBytesUint16ToUint64Bytes() ([]byte, uint16, uint64, []byte) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTobytesuint16(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key1, key2, value1, value2
}

// SeekBytesUint16ToUint64Bytes is first with key of type pair([]byte,uint16) and value of type pair(uint64,[]byte).
func (c *Cursor) SeekBytesUint16ToUint64Bytes(key1 []byte, key2 uint16) ([]byte, uint16, uint64, []byte) {
	rawKey, rawValue := c.Seek(bytesuint16Tobytes(key1, key2))
	key1, key2 = bytesTobytesuint16(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key1, key2, value1, value2
}

// ForEachBytesUint16ToBytesUint16 is a for each with key of type pair([]byte,uint16) and value of type pair([]byte,uint16).
func (b *Bucket) ForEachBytesUint16ToBytesUint16(fn func(key1 []byte, key2 uint16, value1 []byte, value2 uint16) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTobytesuint16(rawKey)
			value1, value2 := bytesTobytesuint16(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetBytesUint16ToBytesUint16 is a get with key of type pair([]byte,uint16) and value of type pair([]byte,uint16).
func (b *Bucket) GetBytesUint16ToBytesUint16(key1 []byte, key2 uint16) ([]byte, uint16) {
	return bytesTobytesuint16(b.Get(bytesuint16Tobytes(key1, key2)))
}

// GetRequiredBytesUint16ToBytesUint16 is a get with key of type pair([]byte,uint16) and value of type pair([]byte,uint16) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesUint16ToBytesUint16(key1 []byte, key2 uint16) ([]byte, uint16, error) {
	value := b.Get(bytesuint16Tobytes(key1, key2))
	if value == nil {
		return nil, 0, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTobytesuint16(value)
	return value1, value2, nil
}

// PutBytesUint16ToBytesUint16 is a put with key of type pair([]byte,uint16) and value of type pair([]byte,uint16).
func (b *Bucket) PutBytesUint16ToBytesUint16(key1 []byte, key2 uint16, value1 []byte, value2 uint16) error {
	return b.Put(bytesuint16Tobytes(key1, key2), bytesuint16Tobytes(value1, value2))
}

// FirstBytesUint16ToBytesUint16 is first with key of type pair([]byte,uint16) and value of type pair([]byte,uint16).
func (c *Cursor) FirstBytesUint16ToBytesUint16() ([]byte, uint16, []byte, uint16) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTobytesuint16(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key1, key2, value1, value2
}

// LastBytesUint16ToBytesUint16 is first with key of type pair([]byte,uint16) and value of type pair([]byte,uint16).
func (c *Cursor) LastBytesUint16ToBytesUint16() ([]byte, uint16, []byte, uint16) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTobytesuint16(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key1, key2, value1, value2
}

// NextBytesUint16ToBytesUint16 is first with key of type pair([]byte,uint16) and value of type pair([]byte,uint16).
func (c *Cursor) NextBytesUint16ToBytesUint16() ([]byte, uint16, []byte, uint16) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTobytesuint16(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key1, key2, value1, value2
}

// PrevBytesUint16ToBytesUint16 is first with key of type pair([]byte,uint16) and value of type pair([]byte,uint16).
func (c *Cursor) PrevBytesUint16ToBytesUint16() ([]byte, uint16, []byte, uint16) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTobytesuint16(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key1, key2, value1, value2
}

// SeekBytesUint16ToBytesUint16 is first with key of type pair([]byte,uint16) and value of type pair([]byte,uint16).
func (c *Cursor) SeekBytesUint16ToBytesUint16(key1 []byte, key2 uint16) ([]byte, uint16, []byte, uint16) {
	rawKey, rawValue := c.Seek(bytesuint16Tobytes(key1, key2))
	key1, key2 = bytesTobytesuint16(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key1, key2, value1, value2
}

// ForEachBytesUint16ToBytesUint32 is a for each with key of type pair([]byte,uint16) and value of type pair([]byte,uint32).
func (b *Bucket) ForEachBytesUint16ToBytesUint32(fn func(key1 []byte, key2 uint16, value1 []byte, value2 uint32) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTobytesuint16(rawKey)
			value1, value2 := bytesTobytesuint32(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetBytesUint16ToBytesUint32 is a get with key of type pair([]byte,uint16) and value of type pair([]byte,uint32).
func (b *Bucket) GetBytesUint16ToBytesUint32(key1 []byte, key2 uint16) ([]byte, uint32) {
	return bytesTobytesuint32(b.Get(bytesuint16Tobytes(key1, key2)))
}

// GetRequiredBytesUint16ToBytesUint32 is a get with key of type pair([]byte,uint16) and value of type pair([]byte,uint32) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesUint16ToBytesUint32(key1 []byte, key2 uint16) ([]byte, uint32, error) {
	value := b.Get(bytesuint16Tobytes(key1, key2))
	if value == nil {
		return nil, 0, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTobytesuint32(value)
	return value1, value2, nil
}

// PutBytesUint16ToBytesUint32 is a put with key of type pair([]byte,uint16) and value of type pair([]byte,uint32).
func (b *Bucket) PutBytesUint16ToBytesUint32(key1 []byte, key2 uint16, value1 []byte, value2 uint32) error {
	return b.Put(bytesuint16Tobytes(key1, key2), bytesuint32Tobytes(value1, value2))
}

// FirstBytesUint16ToBytesUint32 is first with key of type pair([]byte,uint16) and value of type pair([]byte,uint32).
func (c *Cursor) FirstBytesUint16ToBytesUint32() ([]byte, uint16, []byte, uint32) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTobytesuint16(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key1, key2, value1, value2
}

// LastBytesUint16ToBytesUint32 is first with key of type pair([]byte,uint16) and value of type pair([]byte,uint32).
func (c *Cursor) LastBytesUint16ToBytesUint32() ([]byte, uint16, []byte, uint32) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTobytesuint16(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key1, key2, value1, value2
}

// NextBytesUint16ToBytesUint32 is first with key of type pair([]byte,uint16) and value of type pair([]byte,uint32).
func (c *Cursor) NextBytesUint16ToBytesUint32() ([]byte, uint16, []byte, uint32) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTobytesuint16(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key1, key2, value1, value2
}

// PrevBytesUint16ToBytesUint32 is first with key of type pair([]byte,uint16) and value of type pair([]byte,uint32).
func (c *Cursor) PrevBytesUint16ToBytesUint32() ([]byte, uint16, []byte, uint32) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTobytesuint16(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key1, key2, value1, value2
}

// SeekBytesUint16ToBytesUint32 is first with key of type pair([]byte,uint16) and value of type pair([]byte,uint32).
func (c *Cursor) SeekBytesUint16ToBytesUint32(key1 []byte, key2 uint16) ([]byte, uint16, []byte, uint32) {
	rawKey, rawValue := c.Seek(bytesuint16Tobytes(key1, key2))
	key1, key2 = bytesTobytesuint16(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key1, key2, value1, value2
}

// ForEachBytesUint16ToBytesUint64 is a for each with key of type pair([]byte,uint16) and value of type pair([]byte,uint64).
func (b *Bucket) ForEachBytesUint16ToBytesUint64(fn func(key1 []byte, key2 uint16, value1 []byte, value2 uint64) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTobytesuint16(rawKey)
			value1, value2 := bytesTobytesuint64(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetBytesUint16ToBytesUint64 is a get with key of type pair([]byte,uint16) and value of type pair([]byte,uint64).
func (b *Bucket) GetBytesUint16ToBytesUint64(key1 []byte, key2 uint16) ([]byte, uint64) {
	return bytesTobytesuint64(b.Get(bytesuint16Tobytes(key1, key2)))
}

// GetRequiredBytesUint16ToBytesUint64 is a get with key of type pair([]byte,uint16) and value of type pair([]byte,uint64) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesUint16ToBytesUint64(key1 []byte, key2 uint16) ([]byte, uint64, error) {
	value := b.Get(bytesuint16Tobytes(key1, key2))
	if value == nil {
		return nil, 0, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTobytesuint64(value)
	return value1, value2, nil
}

// PutBytesUint16ToBytesUint64 is a put with key of type pair([]byte,uint16) and value of type pair([]byte,uint64).
func (b *Bucket) PutBytesUint16ToBytesUint64(key1 []byte, key2 uint16, value1 []byte, value2 uint64) error {
	return b.Put(bytesuint16Tobytes(key1, key2), bytesuint64Tobytes(value1, value2))
}

// FirstBytesUint16ToBytesUint64 is first with key of type pair([]byte,uint16) and value of type pair([]byte,uint64).
func (c *Cursor) FirstBytesUint16ToBytesUint64() ([]byte, uint16, []byte, uint64) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTobytesuint16(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key1, key2, value1, value2
}

// LastBytesUint16ToBytesUint64 is first with key of type pair([]byte,uint16) and value of type pair([]byte,uint64).
func (c *Cursor) LastBytesUint16ToBytesUint64() ([]byte, uint16, []byte, uint64) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTobytesuint16(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key1, key2, value1, value2
}

// NextBytesUint16ToBytesUint64 is first with key of type pair([]byte,uint16) and value of type pair([]byte,uint64).
func (c *Cursor) NextBytesUint16ToBytesUint64() ([]byte, uint16, []byte, uint64) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTobytesuint16(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key1, key2, value1, value2
}

// PrevBytesUint16ToBytesUint64 is first with key of type pair([]byte,uint16) and value of type pair([]byte,uint64).
func (c *Cursor) PrevBytesUint16ToBytesUint64() ([]byte, uint16, []byte, uint64) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTobytesuint16(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key1, key2, value1, value2
}

// SeekBytesUint16ToBytesUint64 is first with key of type pair([]byte,uint16) and value of type pair([]byte,uint64).
func (c *Cursor) SeekBytesUint16ToBytesUint64(key1 []byte, key2 uint16) ([]byte, uint16, []byte, uint64) {
	rawKey, rawValue := c.Seek(bytesuint16Tobytes(key1, key2))
	key1, key2 = bytesTobytesuint16(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key1, key2, value1, value2
}

// DeleteBytesUint16 is a delete with key of type pair([]byte,uint16).
func (b *Bucket) DeleteBytesUint16(key1 []byte, key2 uint16) error {
	return b.Delete(bytesuint16Tobytes(key1, key2))
}

// ForEachBytesUint32ToBytes is a for each with key of type pair([]byte,uint32) and value of type []byte.
func (b *Bucket) ForEachBytesUint32ToBytes(fn func(key1 []byte, key2 uint32, value []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTobytesuint32(rawKey)
			value := (rawValue)
			return fn(key1, key2, value)
		},
	)
}

// GetBytesUint32ToBytes is a get with key of type pair([]byte,uint32) and value of type []byte.
func (b *Bucket) GetBytesUint32ToBytes(key1 []byte, key2 uint32) []byte {
	return (b.Get(bytesuint32Tobytes(key1, key2)))
}

// GetRequiredBytesUint32ToBytes is a get with key of type pair([]byte,uint32) and value of type []byte that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesUint32ToBytes(key1 []byte, key2 uint32) ([]byte, error) {
	value := b.Get(bytesuint32Tobytes(key1, key2))
	if value == nil {
		return nil, NewKeyNotFoundError(key1, key2)
	}
	return (value), nil
}

// PutBytesUint32ToBytes is a put with key of type pair([]byte,uint32) and value of type []byte.
func (b *Bucket) PutBytesUint32ToBytes(key1 []byte, key2 uint32, value []byte) error {
	return b.Put(bytesuint32Tobytes(key1, key2), (value))
}

// FirstBytesUint32ToBytes is first with key of type pair([]byte,uint32) and value of type []byte.
func (c *Cursor) FirstBytesUint32ToBytes() ([]byte, uint32, []byte) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTobytesuint32(rawKey)
	value := (rawValue)
	return key1, key2, value
}

// LastBytesUint32ToBytes is first with key of type pair([]byte,uint32) and value of type []byte.
func (c *Cursor) LastBytesUint32ToBytes() ([]byte, uint32, []byte) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTobytesuint32(rawKey)
	value := (rawValue)
	return key1, key2, value
}

// NextBytesUint32ToBytes is first with key of type pair([]byte,uint32) and value of type []byte.
func (c *Cursor) NextBytesUint32ToBytes() ([]byte, uint32, []byte) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTobytesuint32(rawKey)
	value := (rawValue)
	return key1, key2, value
}

// PrevBytesUint32ToBytes is first with key of type pair([]byte,uint32) and value of type []byte.
func (c *Cursor) PrevBytesUint32ToBytes() ([]byte, uint32, []byte) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTobytesuint32(rawKey)
	value := (rawValue)
	return key1, key2, value
}

// SeekBytesUint32ToBytes is first with key of type pair([]byte,uint32) and value of type []byte.
func (c *Cursor) SeekBytesUint32ToBytes(key1 []byte, key2 uint32) ([]byte, uint32, []byte) {
	rawKey, rawValue := c.Seek(bytesuint32Tobytes(key1, key2))
	key1, key2 = bytesTobytesuint32(rawKey)
	value := (rawValue)
	return key1, key2, value
}

// ForEachBytesUint32ToString is a for each with key of type pair([]byte,uint32) and value of type string.
func (b *Bucket) ForEachBytesUint32ToString(fn func(key1 []byte, key2 uint32, value string) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTobytesuint32(rawKey)
			value := string(rawValue)
			return fn(key1, key2, value)
		},
	)
}

// GetBytesUint32ToString is a get with key of type pair([]byte,uint32) and value of type string.
func (b *Bucket) GetBytesUint32ToString(key1 []byte, key2 uint32) string {
	return string(b.Get(bytesuint32Tobytes(key1, key2)))
}

// GetRequiredBytesUint32ToString is a get with key of type pair([]byte,uint32) and value of type string that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesUint32ToString(key1 []byte, key2 uint32) (string, error) {
	value := b.Get(bytesuint32Tobytes(key1, key2))
	if value == nil {
		return "", NewKeyNotFoundError(key1, key2)
	}
	return string(value), nil
}

// PutBytesUint32ToString is a put with key of type pair([]byte,uint32) and value of type string.
func (b *Bucket) PutBytesUint32ToString(key1 []byte, key2 uint32, value string) error {
	return b.Put(bytesuint32Tobytes(key1, key2), []byte(value))
}

// FirstBytesUint32ToString is first with key of type pair([]byte,uint32) and value of type string.
func (c *Cursor) FirstBytesUint32ToString() ([]byte, uint32, string) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTobytesuint32(rawKey)
	value := string(rawValue)
	return key1, key2, value
}

// LastBytesUint32ToString is first with key of type pair([]byte,uint32) and value of type string.
func (c *Cursor) LastBytesUint32ToString() ([]byte, uint32, string) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTobytesuint32(rawKey)
	value := string(rawValue)
	return key1, key2, value
}

// NextBytesUint32ToString is first with key of type pair([]byte,uint32) and value of type string.
func (c *Cursor) NextBytesUint32ToString() ([]byte, uint32, string) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTobytesuint32(rawKey)
	value := string(rawValue)
	return key1, key2, value
}

// PrevBytesUint32ToString is first with key of type pair([]byte,uint32) and value of type string.
func (c *Cursor) PrevBytesUint32ToString() ([]byte, uint32, string) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTobytesuint32(rawKey)
	value := string(rawValue)
	return key1, key2, value
}

// SeekBytesUint32ToString is first with key of type pair([]byte,uint32) and value of type string.
func (c *Cursor) SeekBytesUint32ToString(key1 []byte, key2 uint32) ([]byte, uint32, string) {
	rawKey, rawValue := c.Seek(bytesuint32Tobytes(key1, key2))
	key1, key2 = bytesTobytesuint32(rawKey)
	value := string(rawValue)
	return key1, key2, value
}

// ForEachBytesUint32ToUint16 is a for each with key of type pair([]byte,uint32) and value of type uint16.
func (b *Bucket) ForEachBytesUint32ToUint16(fn func(key1 []byte, key2 uint32, value uint16) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTobytesuint32(rawKey)
			value := bytesTouint16(rawValue)
			return fn(key1, key2, value)
		},
	)
}

// GetBytesUint32ToUint16 is a get with key of type pair([]byte,uint32) and value of type uint16.
func (b *Bucket) GetBytesUint32ToUint16(key1 []byte, key2 uint32) uint16 {
	return bytesTouint16(b.Get(bytesuint32Tobytes(key1, key2)))
}

// GetRequiredBytesUint32ToUint16 is a get with key of type pair([]byte,uint32) and value of type uint16 that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesUint32ToUint16(key1 []byte, key2 uint32) (uint16, error) {
	value := b.Get(bytesuint32Tobytes(key1, key2))
	if value == nil {
		return 0, NewKeyNotFoundError(key1, key2)
	}
	return bytesTouint16(value), nil
}

// PutBytesUint32ToUint16 is a put with key of type pair([]byte,uint32) and value of type uint16.
func (b *Bucket) PutBytesUint32ToUint16(key1 []byte, key2 uint32, value uint16) error {
	return b.Put(bytesuint32Tobytes(key1, key2), uint16Tobytes(value))
}

// FirstBytesUint32ToUint16 is first with key of type pair([]byte,uint32) and value of type uint16.
func (c *Cursor) FirstBytesUint32ToUint16() ([]byte, uint32, uint16) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTobytesuint32(rawKey)
	value := bytesTouint16(rawValue)
	return key1, key2, value
}

// LastBytesUint32ToUint16 is first with key of type pair([]byte,uint32) and value of type uint16.
func (c *Cursor) LastBytesUint32ToUint16() ([]byte, uint32, uint16) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTobytesuint32(rawKey)
	value := bytesTouint16(rawValue)
	return key1, key2, value
}

// NextBytesUint32ToUint16 is first with key of type pair([]byte,uint32) and value of type uint16.
func (c *Cursor) NextBytesUint32ToUint16() ([]byte, uint32, uint16) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTobytesuint32(rawKey)
	value := bytesTouint16(rawValue)
	return key1, key2, value
}

// PrevBytesUint32ToUint16 is first with key of type pair([]byte,uint32) and value of type uint16.
func (c *Cursor) PrevBytesUint32ToUint16() ([]byte, uint32, uint16) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTobytesuint32(rawKey)
	value := bytesTouint16(rawValue)
	return key1, key2, value
}

// SeekBytesUint32ToUint16 is first with key of type pair([]byte,uint32) and value of type uint16.
func (c *Cursor) SeekBytesUint32ToUint16(key1 []byte, key2 uint32) ([]byte, uint32, uint16) {
	rawKey, rawValue := c.Seek(bytesuint32Tobytes(key1, key2))
	key1, key2 = bytesTobytesuint32(rawKey)
	value := bytesTouint16(rawValue)
	return key1, key2, value
}

// ForEachBytesUint32ToUint32 is a for each with key of type pair([]byte,uint32) and value of type uint32.
func (b *Bucket) ForEachBytesUint32ToUint32(fn func(key1 []byte, key2 uint32, value uint32) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTobytesuint32(rawKey)
			value := bytesTouint32(rawValue)
			return fn(key1, key2, value)
		},
	)
}

// GetBytesUint32ToUint32 is a get with key of type pair([]byte,uint32) and value of type uint32.
func (b *Bucket) GetBytesUint32ToUint32(key1 []byte, key2 uint32) uint32 {
	return bytesTouint32(b.Get(bytesuint32Tobytes(key1, key2)))
}

// GetRequiredBytesUint32ToUint32 is a get with key of type pair([]byte,uint32) and value of type uint32 that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesUint32ToUint32(key1 []byte, key2 uint32) (uint32, error) {
	value := b.Get(bytesuint32Tobytes(key1, key2))
	if value == nil {
		return 0, NewKeyNotFoundError(key1, key2)
	}
	return bytesTouint32(value), nil
}

// PutBytesUint32ToUint32 is a put with key of type pair([]byte,uint32) and value of type uint32.
func (b *Bucket) PutBytesUint32ToUint32(key1 []byte, key2 uint32, value uint32) error {
	return b.Put(bytesuint32Tobytes(key1, key2), uint32Tobytes(value))
}

// FirstBytesUint32ToUint32 is first with key of type pair([]byte,uint32) and value of type uint32.
func (c *Cursor) FirstBytesUint32ToUint32() ([]byte, uint32, uint32) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTobytesuint32(rawKey)
	value := bytesTouint32(rawValue)
	return key1, key2, value
}

// LastBytesUint32ToUint32 is first with key of type pair([]byte,uint32) and value of type uint32.
func (c *Cursor) LastBytesUint32ToUint32() ([]byte, uint32, uint32) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTobytesuint32(rawKey)
	value := bytesTouint32(rawValue)
	return key1, key2, value
}

// NextBytesUint32ToUint32 is first with key of type pair([]byte,uint32) and value of type uint32.
func (c *Cursor) NextBytesUint32ToUint32() ([]byte, uint32, uint32) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTobytesuint32(rawKey)
	value := bytesTouint32(rawValue)
	return key1, key2, value
}

// PrevBytesUint32ToUint32 is first with key of type pair([]byte,uint32) and value of type uint32.
func (c *Cursor) PrevBytesUint32ToUint32() ([]byte, uint32, uint32) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTobytesuint32(rawKey)
	value := bytesTouint32(rawValue)
	return key1, key2, value
}

// SeekBytesUint32ToUint32 is first with key of type pair([]byte,uint32) and value of type uint32.
func (c *Cursor) SeekBytesUint32ToUint32(key1 []byte, key2 uint32) ([]byte, uint32, uint32) {
	rawKey, rawValue := c.Seek(bytesuint32Tobytes(key1, key2))
	key1, key2 = bytesTobytesuint32(rawKey)
	value := bytesTouint32(rawValue)
	return key1, key2, value
}

// ForEachBytesUint32ToUint64 is a for each with key of type pair([]byte,uint32) and value of type uint64.
func (b *Bucket) ForEachBytesUint32ToUint64(fn func(key1 []byte, key2 uint32, value uint64) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTobytesuint32(rawKey)
			value := bytesTouint64(rawValue)
			return fn(key1, key2, value)
		},
	)
}

// GetBytesUint32ToUint64 is a get with key of type pair([]byte,uint32) and value of type uint64.
func (b *Bucket) GetBytesUint32ToUint64(key1 []byte, key2 uint32) uint64 {
	return bytesTouint64(b.Get(bytesuint32Tobytes(key1, key2)))
}

// GetRequiredBytesUint32ToUint64 is a get with key of type pair([]byte,uint32) and value of type uint64 that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesUint32ToUint64(key1 []byte, key2 uint32) (uint64, error) {
	value := b.Get(bytesuint32Tobytes(key1, key2))
	if value == nil {
		return 0, NewKeyNotFoundError(key1, key2)
	}
	return bytesTouint64(value), nil
}

// PutBytesUint32ToUint64 is a put with key of type pair([]byte,uint32) and value of type uint64.
func (b *Bucket) PutBytesUint32ToUint64(key1 []byte, key2 uint32, value uint64) error {
	return b.Put(bytesuint32Tobytes(key1, key2), uint64Tobytes(value))
}

// FirstBytesUint32ToUint64 is first with key of type pair([]byte,uint32) and value of type uint64.
func (c *Cursor) FirstBytesUint32ToUint64() ([]byte, uint32, uint64) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTobytesuint32(rawKey)
	value := bytesTouint64(rawValue)
	return key1, key2, value
}

// LastBytesUint32ToUint64 is first with key of type pair([]byte,uint32) and value of type uint64.
func (c *Cursor) LastBytesUint32ToUint64() ([]byte, uint32, uint64) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTobytesuint32(rawKey)
	value := bytesTouint64(rawValue)
	return key1, key2, value
}

// NextBytesUint32ToUint64 is first with key of type pair([]byte,uint32) and value of type uint64.
func (c *Cursor) NextBytesUint32ToUint64() ([]byte, uint32, uint64) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTobytesuint32(rawKey)
	value := bytesTouint64(rawValue)
	return key1, key2, value
}

// PrevBytesUint32ToUint64 is first with key of type pair([]byte,uint32) and value of type uint64.
func (c *Cursor) PrevBytesUint32ToUint64() ([]byte, uint32, uint64) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTobytesuint32(rawKey)
	value := bytesTouint64(rawValue)
	return key1, key2, value
}

// SeekBytesUint32ToUint64 is first with key of type pair([]byte,uint32) and value of type uint64.
func (c *Cursor) SeekBytesUint32ToUint64(key1 []byte, key2 uint32) ([]byte, uint32, uint64) {
	rawKey, rawValue := c.Seek(bytesuint32Tobytes(key1, key2))
	key1, key2 = bytesTobytesuint32(rawKey)
	value := bytesTouint64(rawValue)
	return key1, key2, value
}

// ForEachBytesUint32ToUint16Uint16 is a for each with key of type pair([]byte,uint32) and value of type pair(uint16,uint16).
func (b *Bucket) ForEachBytesUint32ToUint16Uint16(fn func(key1 []byte, key2 uint32, value1 uint16, value2 uint16) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTobytesuint32(rawKey)
			value1, value2 := bytesTouint16uint16(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetBytesUint32ToUint16Uint16 is a get with key of type pair([]byte,uint32) and value of type pair(uint16,uint16).
func (b *Bucket) GetBytesUint32ToUint16Uint16(key1 []byte, key2 uint32) (uint16, uint16) {
	return bytesTouint16uint16(b.Get(bytesuint32Tobytes(key1, key2)))
}

// GetRequiredBytesUint32ToUint16Uint16 is a get with key of type pair([]byte,uint32) and value of type pair(uint16,uint16) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesUint32ToUint16Uint16(key1 []byte, key2 uint32) (uint16, uint16, error) {
	value := b.Get(bytesuint32Tobytes(key1, key2))
	if value == nil {
		return 0, 0, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTouint16uint16(value)
	return value1, value2, nil
}

// PutBytesUint32ToUint16Uint16 is a put with key of type pair([]byte,uint32) and value of type pair(uint16,uint16).
func (b *Bucket) PutBytesUint32ToUint16Uint16(key1 []byte, key2 uint32, value1 uint16, value2 uint16) error {
	return b.Put(bytesuint32Tobytes(key1, key2), uint16uint16Tobytes(value1, value2))
}

// FirstBytesUint32ToUint16Uint16 is first with key of type pair([]byte,uint32) and value of type pair(uint16,uint16).
func (c *Cursor) FirstBytesUint32ToUint16Uint16() ([]byte, uint32, uint16, uint16) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTobytesuint32(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key1, key2, value1, value2
}

// LastBytesUint32ToUint16Uint16 is first with key of type pair([]byte,uint32) and value of type pair(uint16,uint16).
func (c *Cursor) LastBytesUint32ToUint16Uint16() ([]byte, uint32, uint16, uint16) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTobytesuint32(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key1, key2, value1, value2
}

// NextBytesUint32ToUint16Uint16 is first with key of type pair([]byte,uint32) and value of type pair(uint16,uint16).
func (c *Cursor) NextBytesUint32ToUint16Uint16() ([]byte, uint32, uint16, uint16) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTobytesuint32(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key1, key2, value1, value2
}

// PrevBytesUint32ToUint16Uint16 is first with key of type pair([]byte,uint32) and value of type pair(uint16,uint16).
func (c *Cursor) PrevBytesUint32ToUint16Uint16() ([]byte, uint32, uint16, uint16) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTobytesuint32(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key1, key2, value1, value2
}

// SeekBytesUint32ToUint16Uint16 is first with key of type pair([]byte,uint32) and value of type pair(uint16,uint16).
func (c *Cursor) SeekBytesUint32ToUint16Uint16(key1 []byte, key2 uint32) ([]byte, uint32, uint16, uint16) {
	rawKey, rawValue := c.Seek(bytesuint32Tobytes(key1, key2))
	key1, key2 = bytesTobytesuint32(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key1, key2, value1, value2
}

// ForEachBytesUint32ToUint32Uint32 is a for each with key of type pair([]byte,uint32) and value of type pair(uint32,uint32).
func (b *Bucket) ForEachBytesUint32ToUint32Uint32(fn func(key1 []byte, key2 uint32, value1 uint32, value2 uint32) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTobytesuint32(rawKey)
			value1, value2 := bytesTouint32uint32(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetBytesUint32ToUint32Uint32 is a get with key of type pair([]byte,uint32) and value of type pair(uint32,uint32).
func (b *Bucket) GetBytesUint32ToUint32Uint32(key1 []byte, key2 uint32) (uint32, uint32) {
	return bytesTouint32uint32(b.Get(bytesuint32Tobytes(key1, key2)))
}

// GetRequiredBytesUint32ToUint32Uint32 is a get with key of type pair([]byte,uint32) and value of type pair(uint32,uint32) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesUint32ToUint32Uint32(key1 []byte, key2 uint32) (uint32, uint32, error) {
	value := b.Get(bytesuint32Tobytes(key1, key2))
	if value == nil {
		return 0, 0, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTouint32uint32(value)
	return value1, value2, nil
}

// PutBytesUint32ToUint32Uint32 is a put with key of type pair([]byte,uint32) and value of type pair(uint32,uint32).
func (b *Bucket) PutBytesUint32ToUint32Uint32(key1 []byte, key2 uint32, value1 uint32, value2 uint32) error {
	return b.Put(bytesuint32Tobytes(key1, key2), uint32uint32Tobytes(value1, value2))
}

// FirstBytesUint32ToUint32Uint32 is first with key of type pair([]byte,uint32) and value of type pair(uint32,uint32).
func (c *Cursor) FirstBytesUint32ToUint32Uint32() ([]byte, uint32, uint32, uint32) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTobytesuint32(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key1, key2, value1, value2
}

// LastBytesUint32ToUint32Uint32 is first with key of type pair([]byte,uint32) and value of type pair(uint32,uint32).
func (c *Cursor) LastBytesUint32ToUint32Uint32() ([]byte, uint32, uint32, uint32) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTobytesuint32(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key1, key2, value1, value2
}

// NextBytesUint32ToUint32Uint32 is first with key of type pair([]byte,uint32) and value of type pair(uint32,uint32).
func (c *Cursor) NextBytesUint32ToUint32Uint32() ([]byte, uint32, uint32, uint32) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTobytesuint32(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key1, key2, value1, value2
}

// PrevBytesUint32ToUint32Uint32 is first with key of type pair([]byte,uint32) and value of type pair(uint32,uint32).
func (c *Cursor) PrevBytesUint32ToUint32Uint32() ([]byte, uint32, uint32, uint32) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTobytesuint32(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key1, key2, value1, value2
}

// SeekBytesUint32ToUint32Uint32 is first with key of type pair([]byte,uint32) and value of type pair(uint32,uint32).
func (c *Cursor) SeekBytesUint32ToUint32Uint32(key1 []byte, key2 uint32) ([]byte, uint32, uint32, uint32) {
	rawKey, rawValue := c.Seek(bytesuint32Tobytes(key1, key2))
	key1, key2 = bytesTobytesuint32(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key1, key2, value1, value2
}

// ForEachBytesUint32ToUint64Uint64 is a for each with key of type pair([]byte,uint32) and value of type pair(uint64,uint64).
func (b *Bucket) ForEachBytesUint32ToUint64Uint64(fn func(key1 []byte, key2 uint32, value1 uint64, value2 uint64) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTobytesuint32(rawKey)
			value1, value2 := bytesTouint64uint64(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetBytesUint32ToUint64Uint64 is a get with key of type pair([]byte,uint32) and value of type pair(uint64,uint64).
func (b *Bucket) GetBytesUint32ToUint64Uint64(key1 []byte, key2 uint32) (uint64, uint64) {
	return bytesTouint64uint64(b.Get(bytesuint32Tobytes(key1, key2)))
}

// GetRequiredBytesUint32ToUint64Uint64 is a get with key of type pair([]byte,uint32) and value of type pair(uint64,uint64) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesUint32ToUint64Uint64(key1 []byte, key2 uint32) (uint64, uint64, error) {
	value := b.Get(bytesuint32Tobytes(key1, key2))
	if value == nil {
		return 0, 0, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTouint64uint64(value)
	return value1, value2, nil
}

// PutBytesUint32ToUint64Uint64 is a put with key of type pair([]byte,uint32) and value of type pair(uint64,uint64).
func (b *Bucket) PutBytesUint32ToUint64Uint64(key1 []byte, key2 uint32, value1 uint64, value2 uint64) error {
	return b.Put(bytesuint32Tobytes(key1, key2), uint64uint64Tobytes(value1, value2))
}

// FirstBytesUint32ToUint64Uint64 is first with key of type pair([]byte,uint32) and value of type pair(uint64,uint64).
func (c *Cursor) FirstBytesUint32ToUint64Uint64() ([]byte, uint32, uint64, uint64) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTobytesuint32(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key1, key2, value1, value2
}

// LastBytesUint32ToUint64Uint64 is first with key of type pair([]byte,uint32) and value of type pair(uint64,uint64).
func (c *Cursor) LastBytesUint32ToUint64Uint64() ([]byte, uint32, uint64, uint64) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTobytesuint32(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key1, key2, value1, value2
}

// NextBytesUint32ToUint64Uint64 is first with key of type pair([]byte,uint32) and value of type pair(uint64,uint64).
func (c *Cursor) NextBytesUint32ToUint64Uint64() ([]byte, uint32, uint64, uint64) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTobytesuint32(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key1, key2, value1, value2
}

// PrevBytesUint32ToUint64Uint64 is first with key of type pair([]byte,uint32) and value of type pair(uint64,uint64).
func (c *Cursor) PrevBytesUint32ToUint64Uint64() ([]byte, uint32, uint64, uint64) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTobytesuint32(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key1, key2, value1, value2
}

// SeekBytesUint32ToUint64Uint64 is first with key of type pair([]byte,uint32) and value of type pair(uint64,uint64).
func (c *Cursor) SeekBytesUint32ToUint64Uint64(key1 []byte, key2 uint32) ([]byte, uint32, uint64, uint64) {
	rawKey, rawValue := c.Seek(bytesuint32Tobytes(key1, key2))
	key1, key2 = bytesTobytesuint32(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key1, key2, value1, value2
}

// ForEachBytesUint32ToUint16Bytes is a for each with key of type pair([]byte,uint32) and value of type pair(uint16,[]byte).
func (b *Bucket) ForEachBytesUint32ToUint16Bytes(fn func(key1 []byte, key2 uint32, value1 uint16, value2 []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTobytesuint32(rawKey)
			value1, value2 := bytesTouint16bytes(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetBytesUint32ToUint16Bytes is a get with key of type pair([]byte,uint32) and value of type pair(uint16,[]byte).
func (b *Bucket) GetBytesUint32ToUint16Bytes(key1 []byte, key2 uint32) (uint16, []byte) {
	return bytesTouint16bytes(b.Get(bytesuint32Tobytes(key1, key2)))
}

// GetRequiredBytesUint32ToUint16Bytes is a get with key of type pair([]byte,uint32) and value of type pair(uint16,[]byte) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesUint32ToUint16Bytes(key1 []byte, key2 uint32) (uint16, []byte, error) {
	value := b.Get(bytesuint32Tobytes(key1, key2))
	if value == nil {
		return 0, nil, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTouint16bytes(value)
	return value1, value2, nil
}

// PutBytesUint32ToUint16Bytes is a put with key of type pair([]byte,uint32) and value of type pair(uint16,[]byte).
func (b *Bucket) PutBytesUint32ToUint16Bytes(key1 []byte, key2 uint32, value1 uint16, value2 []byte) error {
	return b.Put(bytesuint32Tobytes(key1, key2), uint16bytesTobytes(value1, value2))
}

// FirstBytesUint32ToUint16Bytes is first with key of type pair([]byte,uint32) and value of type pair(uint16,[]byte).
func (c *Cursor) FirstBytesUint32ToUint16Bytes() ([]byte, uint32, uint16, []byte) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTobytesuint32(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key1, key2, value1, value2
}

// LastBytesUint32ToUint16Bytes is first with key of type pair([]byte,uint32) and value of type pair(uint16,[]byte).
func (c *Cursor) LastBytesUint32ToUint16Bytes() ([]byte, uint32, uint16, []byte) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTobytesuint32(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key1, key2, value1, value2
}

// NextBytesUint32ToUint16Bytes is first with key of type pair([]byte,uint32) and value of type pair(uint16,[]byte).
func (c *Cursor) NextBytesUint32ToUint16Bytes() ([]byte, uint32, uint16, []byte) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTobytesuint32(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key1, key2, value1, value2
}

// PrevBytesUint32ToUint16Bytes is first with key of type pair([]byte,uint32) and value of type pair(uint16,[]byte).
func (c *Cursor) PrevBytesUint32ToUint16Bytes() ([]byte, uint32, uint16, []byte) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTobytesuint32(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key1, key2, value1, value2
}

// SeekBytesUint32ToUint16Bytes is first with key of type pair([]byte,uint32) and value of type pair(uint16,[]byte).
func (c *Cursor) SeekBytesUint32ToUint16Bytes(key1 []byte, key2 uint32) ([]byte, uint32, uint16, []byte) {
	rawKey, rawValue := c.Seek(bytesuint32Tobytes(key1, key2))
	key1, key2 = bytesTobytesuint32(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key1, key2, value1, value2
}

// ForEachBytesUint32ToUint32Bytes is a for each with key of type pair([]byte,uint32) and value of type pair(uint32,[]byte).
func (b *Bucket) ForEachBytesUint32ToUint32Bytes(fn func(key1 []byte, key2 uint32, value1 uint32, value2 []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTobytesuint32(rawKey)
			value1, value2 := bytesTouint32bytes(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetBytesUint32ToUint32Bytes is a get with key of type pair([]byte,uint32) and value of type pair(uint32,[]byte).
func (b *Bucket) GetBytesUint32ToUint32Bytes(key1 []byte, key2 uint32) (uint32, []byte) {
	return bytesTouint32bytes(b.Get(bytesuint32Tobytes(key1, key2)))
}

// GetRequiredBytesUint32ToUint32Bytes is a get with key of type pair([]byte,uint32) and value of type pair(uint32,[]byte) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesUint32ToUint32Bytes(key1 []byte, key2 uint32) (uint32, []byte, error) {
	value := b.Get(bytesuint32Tobytes(key1, key2))
	if value == nil {
		return 0, nil, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTouint32bytes(value)
	return value1, value2, nil
}

// PutBytesUint32ToUint32Bytes is a put with key of type pair([]byte,uint32) and value of type pair(uint32,[]byte).
func (b *Bucket) PutBytesUint32ToUint32Bytes(key1 []byte, key2 uint32, value1 uint32, value2 []byte) error {
	return b.Put(bytesuint32Tobytes(key1, key2), uint32bytesTobytes(value1, value2))
}

// FirstBytesUint32ToUint32Bytes is first with key of type pair([]byte,uint32) and value of type pair(uint32,[]byte).
func (c *Cursor) FirstBytesUint32ToUint32Bytes() ([]byte, uint32, uint32, []byte) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTobytesuint32(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key1, key2, value1, value2
}

// LastBytesUint32ToUint32Bytes is first with key of type pair([]byte,uint32) and value of type pair(uint32,[]byte).
func (c *Cursor) LastBytesUint32ToUint32Bytes() ([]byte, uint32, uint32, []byte) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTobytesuint32(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key1, key2, value1, value2
}

// NextBytesUint32ToUint32Bytes is first with key of type pair([]byte,uint32) and value of type pair(uint32,[]byte).
func (c *Cursor) NextBytesUint32ToUint32Bytes() ([]byte, uint32, uint32, []byte) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTobytesuint32(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key1, key2, value1, value2
}

// PrevBytesUint32ToUint32Bytes is first with key of type pair([]byte,uint32) and value of type pair(uint32,[]byte).
func (c *Cursor) PrevBytesUint32ToUint32Bytes() ([]byte, uint32, uint32, []byte) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTobytesuint32(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key1, key2, value1, value2
}

// SeekBytesUint32ToUint32Bytes is first with key of type pair([]byte,uint32) and value of type pair(uint32,[]byte).
func (c *Cursor) SeekBytesUint32ToUint32Bytes(key1 []byte, key2 uint32) ([]byte, uint32, uint32, []byte) {
	rawKey, rawValue := c.Seek(bytesuint32Tobytes(key1, key2))
	key1, key2 = bytesTobytesuint32(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key1, key2, value1, value2
}

// ForEachBytesUint32ToUint64Bytes is a for each with key of type pair([]byte,uint32) and value of type pair(uint64,[]byte).
func (b *Bucket) ForEachBytesUint32ToUint64Bytes(fn func(key1 []byte, key2 uint32, value1 uint64, value2 []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTobytesuint32(rawKey)
			value1, value2 := bytesTouint64bytes(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetBytesUint32ToUint64Bytes is a get with key of type pair([]byte,uint32) and value of type pair(uint64,[]byte).
func (b *Bucket) GetBytesUint32ToUint64Bytes(key1 []byte, key2 uint32) (uint64, []byte) {
	return bytesTouint64bytes(b.Get(bytesuint32Tobytes(key1, key2)))
}

// GetRequiredBytesUint32ToUint64Bytes is a get with key of type pair([]byte,uint32) and value of type pair(uint64,[]byte) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesUint32ToUint64Bytes(key1 []byte, key2 uint32) (uint64, []byte, error) {
	value := b.Get(bytesuint32Tobytes(key1, key2))
	if value == nil {
		return 0, nil, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTouint64bytes(value)
	return value1, value2, nil
}

// PutBytesUint32ToUint64Bytes is a put with key of type pair([]byte,uint32) and value of type pair(uint64,[]byte).
func (b *Bucket) PutBytesUint32ToUint64Bytes(key1 []byte, key2 uint32, value1 uint64, value2 []byte) error {
	return b.Put(bytesuint32Tobytes(key1, key2), uint64bytesTobytes(value1, value2))
}

// FirstBytesUint32ToUint64Bytes is first with key of type pair([]byte,uint32) and value of type pair(uint64,[]byte).
func (c *Cursor) FirstBytesUint32ToUint64Bytes() ([]byte, uint32, uint64, []byte) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTobytesuint32(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key1, key2, value1, value2
}

// LastBytesUint32ToUint64Bytes is first with key of type pair([]byte,uint32) and value of type pair(uint64,[]byte).
func (c *Cursor) LastBytesUint32ToUint64Bytes() ([]byte, uint32, uint64, []byte) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTobytesuint32(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key1, key2, value1, value2
}

// NextBytesUint32ToUint64Bytes is first with key of type pair([]byte,uint32) and value of type pair(uint64,[]byte).
func (c *Cursor) NextBytesUint32ToUint64Bytes() ([]byte, uint32, uint64, []byte) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTobytesuint32(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key1, key2, value1, value2
}

// PrevBytesUint32ToUint64Bytes is first with key of type pair([]byte,uint32) and value of type pair(uint64,[]byte).
func (c *Cursor) PrevBytesUint32ToUint64Bytes() ([]byte, uint32, uint64, []byte) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTobytesuint32(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key1, key2, value1, value2
}

// SeekBytesUint32ToUint64Bytes is first with key of type pair([]byte,uint32) and value of type pair(uint64,[]byte).
func (c *Cursor) SeekBytesUint32ToUint64Bytes(key1 []byte, key2 uint32) ([]byte, uint32, uint64, []byte) {
	rawKey, rawValue := c.Seek(bytesuint32Tobytes(key1, key2))
	key1, key2 = bytesTobytesuint32(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key1, key2, value1, value2
}

// ForEachBytesUint32ToBytesUint16 is a for each with key of type pair([]byte,uint32) and value of type pair([]byte,uint16).
func (b *Bucket) ForEachBytesUint32ToBytesUint16(fn func(key1 []byte, key2 uint32, value1 []byte, value2 uint16) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTobytesuint32(rawKey)
			value1, value2 := bytesTobytesuint16(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetBytesUint32ToBytesUint16 is a get with key of type pair([]byte,uint32) and value of type pair([]byte,uint16).
func (b *Bucket) GetBytesUint32ToBytesUint16(key1 []byte, key2 uint32) ([]byte, uint16) {
	return bytesTobytesuint16(b.Get(bytesuint32Tobytes(key1, key2)))
}

// GetRequiredBytesUint32ToBytesUint16 is a get with key of type pair([]byte,uint32) and value of type pair([]byte,uint16) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesUint32ToBytesUint16(key1 []byte, key2 uint32) ([]byte, uint16, error) {
	value := b.Get(bytesuint32Tobytes(key1, key2))
	if value == nil {
		return nil, 0, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTobytesuint16(value)
	return value1, value2, nil
}

// PutBytesUint32ToBytesUint16 is a put with key of type pair([]byte,uint32) and value of type pair([]byte,uint16).
func (b *Bucket) PutBytesUint32ToBytesUint16(key1 []byte, key2 uint32, value1 []byte, value2 uint16) error {
	return b.Put(bytesuint32Tobytes(key1, key2), bytesuint16Tobytes(value1, value2))
}

// FirstBytesUint32ToBytesUint16 is first with key of type pair([]byte,uint32) and value of type pair([]byte,uint16).
func (c *Cursor) FirstBytesUint32ToBytesUint16() ([]byte, uint32, []byte, uint16) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTobytesuint32(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key1, key2, value1, value2
}

// LastBytesUint32ToBytesUint16 is first with key of type pair([]byte,uint32) and value of type pair([]byte,uint16).
func (c *Cursor) LastBytesUint32ToBytesUint16() ([]byte, uint32, []byte, uint16) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTobytesuint32(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key1, key2, value1, value2
}

// NextBytesUint32ToBytesUint16 is first with key of type pair([]byte,uint32) and value of type pair([]byte,uint16).
func (c *Cursor) NextBytesUint32ToBytesUint16() ([]byte, uint32, []byte, uint16) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTobytesuint32(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key1, key2, value1, value2
}

// PrevBytesUint32ToBytesUint16 is first with key of type pair([]byte,uint32) and value of type pair([]byte,uint16).
func (c *Cursor) PrevBytesUint32ToBytesUint16() ([]byte, uint32, []byte, uint16) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTobytesuint32(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key1, key2, value1, value2
}

// SeekBytesUint32ToBytesUint16 is first with key of type pair([]byte,uint32) and value of type pair([]byte,uint16).
func (c *Cursor) SeekBytesUint32ToBytesUint16(key1 []byte, key2 uint32) ([]byte, uint32, []byte, uint16) {
	rawKey, rawValue := c.Seek(bytesuint32Tobytes(key1, key2))
	key1, key2 = bytesTobytesuint32(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key1, key2, value1, value2
}

// ForEachBytesUint32ToBytesUint32 is a for each with key of type pair([]byte,uint32) and value of type pair([]byte,uint32).
func (b *Bucket) ForEachBytesUint32ToBytesUint32(fn func(key1 []byte, key2 uint32, value1 []byte, value2 uint32) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTobytesuint32(rawKey)
			value1, value2 := bytesTobytesuint32(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetBytesUint32ToBytesUint32 is a get with key of type pair([]byte,uint32) and value of type pair([]byte,uint32).
func (b *Bucket) GetBytesUint32ToBytesUint32(key1 []byte, key2 uint32) ([]byte, uint32) {
	return bytesTobytesuint32(b.Get(bytesuint32Tobytes(key1, key2)))
}

// GetRequiredBytesUint32ToBytesUint32 is a get with key of type pair([]byte,uint32) and value of type pair([]byte,uint32) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesUint32ToBytesUint32(key1 []byte, key2 uint32) ([]byte, uint32, error) {
	value := b.Get(bytesuint32Tobytes(key1, key2))
	if value == nil {
		return nil, 0, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTobytesuint32(value)
	return value1, value2, nil
}

// PutBytesUint32ToBytesUint32 is a put with key of type pair([]byte,uint32) and value of type pair([]byte,uint32).
func (b *Bucket) PutBytesUint32ToBytesUint32(key1 []byte, key2 uint32, value1 []byte, value2 uint32) error {
	return b.Put(bytesuint32Tobytes(key1, key2), bytesuint32Tobytes(value1, value2))
}

// FirstBytesUint32ToBytesUint32 is first with key of type pair([]byte,uint32) and value of type pair([]byte,uint32).
func (c *Cursor) FirstBytesUint32ToBytesUint32() ([]byte, uint32, []byte, uint32) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTobytesuint32(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key1, key2, value1, value2
}

// LastBytesUint32ToBytesUint32 is first with key of type pair([]byte,uint32) and value of type pair([]byte,uint32).
func (c *Cursor) LastBytesUint32ToBytesUint32() ([]byte, uint32, []byte, uint32) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTobytesuint32(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key1, key2, value1, value2
}

// NextBytesUint32ToBytesUint32 is first with key of type pair([]byte,uint32) and value of type pair([]byte,uint32).
func (c *Cursor) NextBytesUint32ToBytesUint32() ([]byte, uint32, []byte, uint32) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTobytesuint32(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key1, key2, value1, value2
}

// PrevBytesUint32ToBytesUint32 is first with key of type pair([]byte,uint32) and value of type pair([]byte,uint32).
func (c *Cursor) PrevBytesUint32ToBytesUint32() ([]byte, uint32, []byte, uint32) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTobytesuint32(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key1, key2, value1, value2
}

// SeekBytesUint32ToBytesUint32 is first with key of type pair([]byte,uint32) and value of type pair([]byte,uint32).
func (c *Cursor) SeekBytesUint32ToBytesUint32(key1 []byte, key2 uint32) ([]byte, uint32, []byte, uint32) {
	rawKey, rawValue := c.Seek(bytesuint32Tobytes(key1, key2))
	key1, key2 = bytesTobytesuint32(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key1, key2, value1, value2
}

// ForEachBytesUint32ToBytesUint64 is a for each with key of type pair([]byte,uint32) and value of type pair([]byte,uint64).
func (b *Bucket) ForEachBytesUint32ToBytesUint64(fn func(key1 []byte, key2 uint32, value1 []byte, value2 uint64) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTobytesuint32(rawKey)
			value1, value2 := bytesTobytesuint64(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetBytesUint32ToBytesUint64 is a get with key of type pair([]byte,uint32) and value of type pair([]byte,uint64).
func (b *Bucket) GetBytesUint32ToBytesUint64(key1 []byte, key2 uint32) ([]byte, uint64) {
	return bytesTobytesuint64(b.Get(bytesuint32Tobytes(key1, key2)))
}

// GetRequiredBytesUint32ToBytesUint64 is a get with key of type pair([]byte,uint32) and value of type pair([]byte,uint64) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesUint32ToBytesUint64(key1 []byte, key2 uint32) ([]byte, uint64, error) {
	value := b.Get(bytesuint32Tobytes(key1, key2))
	if value == nil {
		return nil, 0, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTobytesuint64(value)
	return value1, value2, nil
}

// PutBytesUint32ToBytesUint64 is a put with key of type pair([]byte,uint32) and value of type pair([]byte,uint64).
func (b *Bucket) PutBytesUint32ToBytesUint64(key1 []byte, key2 uint32, value1 []byte, value2 uint64) error {
	return b.Put(bytesuint32Tobytes(key1, key2), bytesuint64Tobytes(value1, value2))
}

// FirstBytesUint32ToBytesUint64 is first with key of type pair([]byte,uint32) and value of type pair([]byte,uint64).
func (c *Cursor) FirstBytesUint32ToBytesUint64() ([]byte, uint32, []byte, uint64) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTobytesuint32(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key1, key2, value1, value2
}

// LastBytesUint32ToBytesUint64 is first with key of type pair([]byte,uint32) and value of type pair([]byte,uint64).
func (c *Cursor) LastBytesUint32ToBytesUint64() ([]byte, uint32, []byte, uint64) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTobytesuint32(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key1, key2, value1, value2
}

// NextBytesUint32ToBytesUint64 is first with key of type pair([]byte,uint32) and value of type pair([]byte,uint64).
func (c *Cursor) NextBytesUint32ToBytesUint64() ([]byte, uint32, []byte, uint64) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTobytesuint32(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key1, key2, value1, value2
}

// PrevBytesUint32ToBytesUint64 is first with key of type pair([]byte,uint32) and value of type pair([]byte,uint64).
func (c *Cursor) PrevBytesUint32ToBytesUint64() ([]byte, uint32, []byte, uint64) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTobytesuint32(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key1, key2, value1, value2
}

// SeekBytesUint32ToBytesUint64 is first with key of type pair([]byte,uint32) and value of type pair([]byte,uint64).
func (c *Cursor) SeekBytesUint32ToBytesUint64(key1 []byte, key2 uint32) ([]byte, uint32, []byte, uint64) {
	rawKey, rawValue := c.Seek(bytesuint32Tobytes(key1, key2))
	key1, key2 = bytesTobytesuint32(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key1, key2, value1, value2
}

// DeleteBytesUint32 is a delete with key of type pair([]byte,uint32).
func (b *Bucket) DeleteBytesUint32(key1 []byte, key2 uint32) error {
	return b.Delete(bytesuint32Tobytes(key1, key2))
}

// ForEachBytesUint64ToBytes is a for each with key of type pair([]byte,uint64) and value of type []byte.
func (b *Bucket) ForEachBytesUint64ToBytes(fn func(key1 []byte, key2 uint64, value []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTobytesuint64(rawKey)
			value := (rawValue)
			return fn(key1, key2, value)
		},
	)
}

// GetBytesUint64ToBytes is a get with key of type pair([]byte,uint64) and value of type []byte.
func (b *Bucket) GetBytesUint64ToBytes(key1 []byte, key2 uint64) []byte {
	return (b.Get(bytesuint64Tobytes(key1, key2)))
}

// GetRequiredBytesUint64ToBytes is a get with key of type pair([]byte,uint64) and value of type []byte that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesUint64ToBytes(key1 []byte, key2 uint64) ([]byte, error) {
	value := b.Get(bytesuint64Tobytes(key1, key2))
	if value == nil {
		return nil, NewKeyNotFoundError(key1, key2)
	}
	return (value), nil
}

// PutBytesUint64ToBytes is a put with key of type pair([]byte,uint64) and value of type []byte.
func (b *Bucket) PutBytesUint64ToBytes(key1 []byte, key2 uint64, value []byte) error {
	return b.Put(bytesuint64Tobytes(key1, key2), (value))
}

// FirstBytesUint64ToBytes is first with key of type pair([]byte,uint64) and value of type []byte.
func (c *Cursor) FirstBytesUint64ToBytes() ([]byte, uint64, []byte) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTobytesuint64(rawKey)
	value := (rawValue)
	return key1, key2, value
}

// LastBytesUint64ToBytes is first with key of type pair([]byte,uint64) and value of type []byte.
func (c *Cursor) LastBytesUint64ToBytes() ([]byte, uint64, []byte) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTobytesuint64(rawKey)
	value := (rawValue)
	return key1, key2, value
}

// NextBytesUint64ToBytes is first with key of type pair([]byte,uint64) and value of type []byte.
func (c *Cursor) NextBytesUint64ToBytes() ([]byte, uint64, []byte) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTobytesuint64(rawKey)
	value := (rawValue)
	return key1, key2, value
}

// PrevBytesUint64ToBytes is first with key of type pair([]byte,uint64) and value of type []byte.
func (c *Cursor) PrevBytesUint64ToBytes() ([]byte, uint64, []byte) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTobytesuint64(rawKey)
	value := (rawValue)
	return key1, key2, value
}

// SeekBytesUint64ToBytes is first with key of type pair([]byte,uint64) and value of type []byte.
func (c *Cursor) SeekBytesUint64ToBytes(key1 []byte, key2 uint64) ([]byte, uint64, []byte) {
	rawKey, rawValue := c.Seek(bytesuint64Tobytes(key1, key2))
	key1, key2 = bytesTobytesuint64(rawKey)
	value := (rawValue)
	return key1, key2, value
}

// ForEachBytesUint64ToString is a for each with key of type pair([]byte,uint64) and value of type string.
func (b *Bucket) ForEachBytesUint64ToString(fn func(key1 []byte, key2 uint64, value string) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTobytesuint64(rawKey)
			value := string(rawValue)
			return fn(key1, key2, value)
		},
	)
}

// GetBytesUint64ToString is a get with key of type pair([]byte,uint64) and value of type string.
func (b *Bucket) GetBytesUint64ToString(key1 []byte, key2 uint64) string {
	return string(b.Get(bytesuint64Tobytes(key1, key2)))
}

// GetRequiredBytesUint64ToString is a get with key of type pair([]byte,uint64) and value of type string that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesUint64ToString(key1 []byte, key2 uint64) (string, error) {
	value := b.Get(bytesuint64Tobytes(key1, key2))
	if value == nil {
		return "", NewKeyNotFoundError(key1, key2)
	}
	return string(value), nil
}

// PutBytesUint64ToString is a put with key of type pair([]byte,uint64) and value of type string.
func (b *Bucket) PutBytesUint64ToString(key1 []byte, key2 uint64, value string) error {
	return b.Put(bytesuint64Tobytes(key1, key2), []byte(value))
}

// FirstBytesUint64ToString is first with key of type pair([]byte,uint64) and value of type string.
func (c *Cursor) FirstBytesUint64ToString() ([]byte, uint64, string) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTobytesuint64(rawKey)
	value := string(rawValue)
	return key1, key2, value
}

// LastBytesUint64ToString is first with key of type pair([]byte,uint64) and value of type string.
func (c *Cursor) LastBytesUint64ToString() ([]byte, uint64, string) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTobytesuint64(rawKey)
	value := string(rawValue)
	return key1, key2, value
}

// NextBytesUint64ToString is first with key of type pair([]byte,uint64) and value of type string.
func (c *Cursor) NextBytesUint64ToString() ([]byte, uint64, string) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTobytesuint64(rawKey)
	value := string(rawValue)
	return key1, key2, value
}

// PrevBytesUint64ToString is first with key of type pair([]byte,uint64) and value of type string.
func (c *Cursor) PrevBytesUint64ToString() ([]byte, uint64, string) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTobytesuint64(rawKey)
	value := string(rawValue)
	return key1, key2, value
}

// SeekBytesUint64ToString is first with key of type pair([]byte,uint64) and value of type string.
func (c *Cursor) SeekBytesUint64ToString(key1 []byte, key2 uint64) ([]byte, uint64, string) {
	rawKey, rawValue := c.Seek(bytesuint64Tobytes(key1, key2))
	key1, key2 = bytesTobytesuint64(rawKey)
	value := string(rawValue)
	return key1, key2, value
}

// ForEachBytesUint64ToUint16 is a for each with key of type pair([]byte,uint64) and value of type uint16.
func (b *Bucket) ForEachBytesUint64ToUint16(fn func(key1 []byte, key2 uint64, value uint16) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTobytesuint64(rawKey)
			value := bytesTouint16(rawValue)
			return fn(key1, key2, value)
		},
	)
}

// GetBytesUint64ToUint16 is a get with key of type pair([]byte,uint64) and value of type uint16.
func (b *Bucket) GetBytesUint64ToUint16(key1 []byte, key2 uint64) uint16 {
	return bytesTouint16(b.Get(bytesuint64Tobytes(key1, key2)))
}

// GetRequiredBytesUint64ToUint16 is a get with key of type pair([]byte,uint64) and value of type uint16 that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesUint64ToUint16(key1 []byte, key2 uint64) (uint16, error) {
	value := b.Get(bytesuint64Tobytes(key1, key2))
	if value == nil {
		return 0, NewKeyNotFoundError(key1, key2)
	}
	return bytesTouint16(value), nil
}

// PutBytesUint64ToUint16 is a put with key of type pair([]byte,uint64) and value of type uint16.
func (b *Bucket) PutBytesUint64ToUint16(key1 []byte, key2 uint64, value uint16) error {
	return b.Put(bytesuint64Tobytes(key1, key2), uint16Tobytes(value))
}

// FirstBytesUint64ToUint16 is first with key of type pair([]byte,uint64) and value of type uint16.
func (c *Cursor) FirstBytesUint64ToUint16() ([]byte, uint64, uint16) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTobytesuint64(rawKey)
	value := bytesTouint16(rawValue)
	return key1, key2, value
}

// LastBytesUint64ToUint16 is first with key of type pair([]byte,uint64) and value of type uint16.
func (c *Cursor) LastBytesUint64ToUint16() ([]byte, uint64, uint16) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTobytesuint64(rawKey)
	value := bytesTouint16(rawValue)
	return key1, key2, value
}

// NextBytesUint64ToUint16 is first with key of type pair([]byte,uint64) and value of type uint16.
func (c *Cursor) NextBytesUint64ToUint16() ([]byte, uint64, uint16) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTobytesuint64(rawKey)
	value := bytesTouint16(rawValue)
	return key1, key2, value
}

// PrevBytesUint64ToUint16 is first with key of type pair([]byte,uint64) and value of type uint16.
func (c *Cursor) PrevBytesUint64ToUint16() ([]byte, uint64, uint16) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTobytesuint64(rawKey)
	value := bytesTouint16(rawValue)
	return key1, key2, value
}

// SeekBytesUint64ToUint16 is first with key of type pair([]byte,uint64) and value of type uint16.
func (c *Cursor) SeekBytesUint64ToUint16(key1 []byte, key2 uint64) ([]byte, uint64, uint16) {
	rawKey, rawValue := c.Seek(bytesuint64Tobytes(key1, key2))
	key1, key2 = bytesTobytesuint64(rawKey)
	value := bytesTouint16(rawValue)
	return key1, key2, value
}

// ForEachBytesUint64ToUint32 is a for each with key of type pair([]byte,uint64) and value of type uint32.
func (b *Bucket) ForEachBytesUint64ToUint32(fn func(key1 []byte, key2 uint64, value uint32) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTobytesuint64(rawKey)
			value := bytesTouint32(rawValue)
			return fn(key1, key2, value)
		},
	)
}

// GetBytesUint64ToUint32 is a get with key of type pair([]byte,uint64) and value of type uint32.
func (b *Bucket) GetBytesUint64ToUint32(key1 []byte, key2 uint64) uint32 {
	return bytesTouint32(b.Get(bytesuint64Tobytes(key1, key2)))
}

// GetRequiredBytesUint64ToUint32 is a get with key of type pair([]byte,uint64) and value of type uint32 that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesUint64ToUint32(key1 []byte, key2 uint64) (uint32, error) {
	value := b.Get(bytesuint64Tobytes(key1, key2))
	if value == nil {
		return 0, NewKeyNotFoundError(key1, key2)
	}
	return bytesTouint32(value), nil
}

// PutBytesUint64ToUint32 is a put with key of type pair([]byte,uint64) and value of type uint32.
func (b *Bucket) PutBytesUint64ToUint32(key1 []byte, key2 uint64, value uint32) error {
	return b.Put(bytesuint64Tobytes(key1, key2), uint32Tobytes(value))
}

// FirstBytesUint64ToUint32 is first with key of type pair([]byte,uint64) and value of type uint32.
func (c *Cursor) FirstBytesUint64ToUint32() ([]byte, uint64, uint32) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTobytesuint64(rawKey)
	value := bytesTouint32(rawValue)
	return key1, key2, value
}

// LastBytesUint64ToUint32 is first with key of type pair([]byte,uint64) and value of type uint32.
func (c *Cursor) LastBytesUint64ToUint32() ([]byte, uint64, uint32) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTobytesuint64(rawKey)
	value := bytesTouint32(rawValue)
	return key1, key2, value
}

// NextBytesUint64ToUint32 is first with key of type pair([]byte,uint64) and value of type uint32.
func (c *Cursor) NextBytesUint64ToUint32() ([]byte, uint64, uint32) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTobytesuint64(rawKey)
	value := bytesTouint32(rawValue)
	return key1, key2, value
}

// PrevBytesUint64ToUint32 is first with key of type pair([]byte,uint64) and value of type uint32.
func (c *Cursor) PrevBytesUint64ToUint32() ([]byte, uint64, uint32) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTobytesuint64(rawKey)
	value := bytesTouint32(rawValue)
	return key1, key2, value
}

// SeekBytesUint64ToUint32 is first with key of type pair([]byte,uint64) and value of type uint32.
func (c *Cursor) SeekBytesUint64ToUint32(key1 []byte, key2 uint64) ([]byte, uint64, uint32) {
	rawKey, rawValue := c.Seek(bytesuint64Tobytes(key1, key2))
	key1, key2 = bytesTobytesuint64(rawKey)
	value := bytesTouint32(rawValue)
	return key1, key2, value
}

// ForEachBytesUint64ToUint64 is a for each with key of type pair([]byte,uint64) and value of type uint64.
func (b *Bucket) ForEachBytesUint64ToUint64(fn func(key1 []byte, key2 uint64, value uint64) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTobytesuint64(rawKey)
			value := bytesTouint64(rawValue)
			return fn(key1, key2, value)
		},
	)
}

// GetBytesUint64ToUint64 is a get with key of type pair([]byte,uint64) and value of type uint64.
func (b *Bucket) GetBytesUint64ToUint64(key1 []byte, key2 uint64) uint64 {
	return bytesTouint64(b.Get(bytesuint64Tobytes(key1, key2)))
}

// GetRequiredBytesUint64ToUint64 is a get with key of type pair([]byte,uint64) and value of type uint64 that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesUint64ToUint64(key1 []byte, key2 uint64) (uint64, error) {
	value := b.Get(bytesuint64Tobytes(key1, key2))
	if value == nil {
		return 0, NewKeyNotFoundError(key1, key2)
	}
	return bytesTouint64(value), nil
}

// PutBytesUint64ToUint64 is a put with key of type pair([]byte,uint64) and value of type uint64.
func (b *Bucket) PutBytesUint64ToUint64(key1 []byte, key2 uint64, value uint64) error {
	return b.Put(bytesuint64Tobytes(key1, key2), uint64Tobytes(value))
}

// FirstBytesUint64ToUint64 is first with key of type pair([]byte,uint64) and value of type uint64.
func (c *Cursor) FirstBytesUint64ToUint64() ([]byte, uint64, uint64) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTobytesuint64(rawKey)
	value := bytesTouint64(rawValue)
	return key1, key2, value
}

// LastBytesUint64ToUint64 is first with key of type pair([]byte,uint64) and value of type uint64.
func (c *Cursor) LastBytesUint64ToUint64() ([]byte, uint64, uint64) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTobytesuint64(rawKey)
	value := bytesTouint64(rawValue)
	return key1, key2, value
}

// NextBytesUint64ToUint64 is first with key of type pair([]byte,uint64) and value of type uint64.
func (c *Cursor) NextBytesUint64ToUint64() ([]byte, uint64, uint64) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTobytesuint64(rawKey)
	value := bytesTouint64(rawValue)
	return key1, key2, value
}

// PrevBytesUint64ToUint64 is first with key of type pair([]byte,uint64) and value of type uint64.
func (c *Cursor) PrevBytesUint64ToUint64() ([]byte, uint64, uint64) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTobytesuint64(rawKey)
	value := bytesTouint64(rawValue)
	return key1, key2, value
}

// SeekBytesUint64ToUint64 is first with key of type pair([]byte,uint64) and value of type uint64.
func (c *Cursor) SeekBytesUint64ToUint64(key1 []byte, key2 uint64) ([]byte, uint64, uint64) {
	rawKey, rawValue := c.Seek(bytesuint64Tobytes(key1, key2))
	key1, key2 = bytesTobytesuint64(rawKey)
	value := bytesTouint64(rawValue)
	return key1, key2, value
}

// ForEachBytesUint64ToUint16Uint16 is a for each with key of type pair([]byte,uint64) and value of type pair(uint16,uint16).
func (b *Bucket) ForEachBytesUint64ToUint16Uint16(fn func(key1 []byte, key2 uint64, value1 uint16, value2 uint16) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTobytesuint64(rawKey)
			value1, value2 := bytesTouint16uint16(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetBytesUint64ToUint16Uint16 is a get with key of type pair([]byte,uint64) and value of type pair(uint16,uint16).
func (b *Bucket) GetBytesUint64ToUint16Uint16(key1 []byte, key2 uint64) (uint16, uint16) {
	return bytesTouint16uint16(b.Get(bytesuint64Tobytes(key1, key2)))
}

// GetRequiredBytesUint64ToUint16Uint16 is a get with key of type pair([]byte,uint64) and value of type pair(uint16,uint16) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesUint64ToUint16Uint16(key1 []byte, key2 uint64) (uint16, uint16, error) {
	value := b.Get(bytesuint64Tobytes(key1, key2))
	if value == nil {
		return 0, 0, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTouint16uint16(value)
	return value1, value2, nil
}

// PutBytesUint64ToUint16Uint16 is a put with key of type pair([]byte,uint64) and value of type pair(uint16,uint16).
func (b *Bucket) PutBytesUint64ToUint16Uint16(key1 []byte, key2 uint64, value1 uint16, value2 uint16) error {
	return b.Put(bytesuint64Tobytes(key1, key2), uint16uint16Tobytes(value1, value2))
}

// FirstBytesUint64ToUint16Uint16 is first with key of type pair([]byte,uint64) and value of type pair(uint16,uint16).
func (c *Cursor) FirstBytesUint64ToUint16Uint16() ([]byte, uint64, uint16, uint16) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTobytesuint64(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key1, key2, value1, value2
}

// LastBytesUint64ToUint16Uint16 is first with key of type pair([]byte,uint64) and value of type pair(uint16,uint16).
func (c *Cursor) LastBytesUint64ToUint16Uint16() ([]byte, uint64, uint16, uint16) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTobytesuint64(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key1, key2, value1, value2
}

// NextBytesUint64ToUint16Uint16 is first with key of type pair([]byte,uint64) and value of type pair(uint16,uint16).
func (c *Cursor) NextBytesUint64ToUint16Uint16() ([]byte, uint64, uint16, uint16) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTobytesuint64(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key1, key2, value1, value2
}

// PrevBytesUint64ToUint16Uint16 is first with key of type pair([]byte,uint64) and value of type pair(uint16,uint16).
func (c *Cursor) PrevBytesUint64ToUint16Uint16() ([]byte, uint64, uint16, uint16) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTobytesuint64(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key1, key2, value1, value2
}

// SeekBytesUint64ToUint16Uint16 is first with key of type pair([]byte,uint64) and value of type pair(uint16,uint16).
func (c *Cursor) SeekBytesUint64ToUint16Uint16(key1 []byte, key2 uint64) ([]byte, uint64, uint16, uint16) {
	rawKey, rawValue := c.Seek(bytesuint64Tobytes(key1, key2))
	key1, key2 = bytesTobytesuint64(rawKey)
	value1, value2 := bytesTouint16uint16(rawValue)
	return key1, key2, value1, value2
}

// ForEachBytesUint64ToUint32Uint32 is a for each with key of type pair([]byte,uint64) and value of type pair(uint32,uint32).
func (b *Bucket) ForEachBytesUint64ToUint32Uint32(fn func(key1 []byte, key2 uint64, value1 uint32, value2 uint32) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTobytesuint64(rawKey)
			value1, value2 := bytesTouint32uint32(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetBytesUint64ToUint32Uint32 is a get with key of type pair([]byte,uint64) and value of type pair(uint32,uint32).
func (b *Bucket) GetBytesUint64ToUint32Uint32(key1 []byte, key2 uint64) (uint32, uint32) {
	return bytesTouint32uint32(b.Get(bytesuint64Tobytes(key1, key2)))
}

// GetRequiredBytesUint64ToUint32Uint32 is a get with key of type pair([]byte,uint64) and value of type pair(uint32,uint32) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesUint64ToUint32Uint32(key1 []byte, key2 uint64) (uint32, uint32, error) {
	value := b.Get(bytesuint64Tobytes(key1, key2))
	if value == nil {
		return 0, 0, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTouint32uint32(value)
	return value1, value2, nil
}

// PutBytesUint64ToUint32Uint32 is a put with key of type pair([]byte,uint64) and value of type pair(uint32,uint32).
func (b *Bucket) PutBytesUint64ToUint32Uint32(key1 []byte, key2 uint64, value1 uint32, value2 uint32) error {
	return b.Put(bytesuint64Tobytes(key1, key2), uint32uint32Tobytes(value1, value2))
}

// FirstBytesUint64ToUint32Uint32 is first with key of type pair([]byte,uint64) and value of type pair(uint32,uint32).
func (c *Cursor) FirstBytesUint64ToUint32Uint32() ([]byte, uint64, uint32, uint32) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTobytesuint64(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key1, key2, value1, value2
}

// LastBytesUint64ToUint32Uint32 is first with key of type pair([]byte,uint64) and value of type pair(uint32,uint32).
func (c *Cursor) LastBytesUint64ToUint32Uint32() ([]byte, uint64, uint32, uint32) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTobytesuint64(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key1, key2, value1, value2
}

// NextBytesUint64ToUint32Uint32 is first with key of type pair([]byte,uint64) and value of type pair(uint32,uint32).
func (c *Cursor) NextBytesUint64ToUint32Uint32() ([]byte, uint64, uint32, uint32) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTobytesuint64(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key1, key2, value1, value2
}

// PrevBytesUint64ToUint32Uint32 is first with key of type pair([]byte,uint64) and value of type pair(uint32,uint32).
func (c *Cursor) PrevBytesUint64ToUint32Uint32() ([]byte, uint64, uint32, uint32) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTobytesuint64(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key1, key2, value1, value2
}

// SeekBytesUint64ToUint32Uint32 is first with key of type pair([]byte,uint64) and value of type pair(uint32,uint32).
func (c *Cursor) SeekBytesUint64ToUint32Uint32(key1 []byte, key2 uint64) ([]byte, uint64, uint32, uint32) {
	rawKey, rawValue := c.Seek(bytesuint64Tobytes(key1, key2))
	key1, key2 = bytesTobytesuint64(rawKey)
	value1, value2 := bytesTouint32uint32(rawValue)
	return key1, key2, value1, value2
}

// ForEachBytesUint64ToUint64Uint64 is a for each with key of type pair([]byte,uint64) and value of type pair(uint64,uint64).
func (b *Bucket) ForEachBytesUint64ToUint64Uint64(fn func(key1 []byte, key2 uint64, value1 uint64, value2 uint64) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTobytesuint64(rawKey)
			value1, value2 := bytesTouint64uint64(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetBytesUint64ToUint64Uint64 is a get with key of type pair([]byte,uint64) and value of type pair(uint64,uint64).
func (b *Bucket) GetBytesUint64ToUint64Uint64(key1 []byte, key2 uint64) (uint64, uint64) {
	return bytesTouint64uint64(b.Get(bytesuint64Tobytes(key1, key2)))
}

// GetRequiredBytesUint64ToUint64Uint64 is a get with key of type pair([]byte,uint64) and value of type pair(uint64,uint64) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesUint64ToUint64Uint64(key1 []byte, key2 uint64) (uint64, uint64, error) {
	value := b.Get(bytesuint64Tobytes(key1, key2))
	if value == nil {
		return 0, 0, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTouint64uint64(value)
	return value1, value2, nil
}

// PutBytesUint64ToUint64Uint64 is a put with key of type pair([]byte,uint64) and value of type pair(uint64,uint64).
func (b *Bucket) PutBytesUint64ToUint64Uint64(key1 []byte, key2 uint64, value1 uint64, value2 uint64) error {
	return b.Put(bytesuint64Tobytes(key1, key2), uint64uint64Tobytes(value1, value2))
}

// FirstBytesUint64ToUint64Uint64 is first with key of type pair([]byte,uint64) and value of type pair(uint64,uint64).
func (c *Cursor) FirstBytesUint64ToUint64Uint64() ([]byte, uint64, uint64, uint64) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTobytesuint64(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key1, key2, value1, value2
}

// LastBytesUint64ToUint64Uint64 is first with key of type pair([]byte,uint64) and value of type pair(uint64,uint64).
func (c *Cursor) LastBytesUint64ToUint64Uint64() ([]byte, uint64, uint64, uint64) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTobytesuint64(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key1, key2, value1, value2
}

// NextBytesUint64ToUint64Uint64 is first with key of type pair([]byte,uint64) and value of type pair(uint64,uint64).
func (c *Cursor) NextBytesUint64ToUint64Uint64() ([]byte, uint64, uint64, uint64) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTobytesuint64(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key1, key2, value1, value2
}

// PrevBytesUint64ToUint64Uint64 is first with key of type pair([]byte,uint64) and value of type pair(uint64,uint64).
func (c *Cursor) PrevBytesUint64ToUint64Uint64() ([]byte, uint64, uint64, uint64) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTobytesuint64(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key1, key2, value1, value2
}

// SeekBytesUint64ToUint64Uint64 is first with key of type pair([]byte,uint64) and value of type pair(uint64,uint64).
func (c *Cursor) SeekBytesUint64ToUint64Uint64(key1 []byte, key2 uint64) ([]byte, uint64, uint64, uint64) {
	rawKey, rawValue := c.Seek(bytesuint64Tobytes(key1, key2))
	key1, key2 = bytesTobytesuint64(rawKey)
	value1, value2 := bytesTouint64uint64(rawValue)
	return key1, key2, value1, value2
}

// ForEachBytesUint64ToUint16Bytes is a for each with key of type pair([]byte,uint64) and value of type pair(uint16,[]byte).
func (b *Bucket) ForEachBytesUint64ToUint16Bytes(fn func(key1 []byte, key2 uint64, value1 uint16, value2 []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTobytesuint64(rawKey)
			value1, value2 := bytesTouint16bytes(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetBytesUint64ToUint16Bytes is a get with key of type pair([]byte,uint64) and value of type pair(uint16,[]byte).
func (b *Bucket) GetBytesUint64ToUint16Bytes(key1 []byte, key2 uint64) (uint16, []byte) {
	return bytesTouint16bytes(b.Get(bytesuint64Tobytes(key1, key2)))
}

// GetRequiredBytesUint64ToUint16Bytes is a get with key of type pair([]byte,uint64) and value of type pair(uint16,[]byte) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesUint64ToUint16Bytes(key1 []byte, key2 uint64) (uint16, []byte, error) {
	value := b.Get(bytesuint64Tobytes(key1, key2))
	if value == nil {
		return 0, nil, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTouint16bytes(value)
	return value1, value2, nil
}

// PutBytesUint64ToUint16Bytes is a put with key of type pair([]byte,uint64) and value of type pair(uint16,[]byte).
func (b *Bucket) PutBytesUint64ToUint16Bytes(key1 []byte, key2 uint64, value1 uint16, value2 []byte) error {
	return b.Put(bytesuint64Tobytes(key1, key2), uint16bytesTobytes(value1, value2))
}

// FirstBytesUint64ToUint16Bytes is first with key of type pair([]byte,uint64) and value of type pair(uint16,[]byte).
func (c *Cursor) FirstBytesUint64ToUint16Bytes() ([]byte, uint64, uint16, []byte) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTobytesuint64(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key1, key2, value1, value2
}

// LastBytesUint64ToUint16Bytes is first with key of type pair([]byte,uint64) and value of type pair(uint16,[]byte).
func (c *Cursor) LastBytesUint64ToUint16Bytes() ([]byte, uint64, uint16, []byte) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTobytesuint64(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key1, key2, value1, value2
}

// NextBytesUint64ToUint16Bytes is first with key of type pair([]byte,uint64) and value of type pair(uint16,[]byte).
func (c *Cursor) NextBytesUint64ToUint16Bytes() ([]byte, uint64, uint16, []byte) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTobytesuint64(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key1, key2, value1, value2
}

// PrevBytesUint64ToUint16Bytes is first with key of type pair([]byte,uint64) and value of type pair(uint16,[]byte).
func (c *Cursor) PrevBytesUint64ToUint16Bytes() ([]byte, uint64, uint16, []byte) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTobytesuint64(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key1, key2, value1, value2
}

// SeekBytesUint64ToUint16Bytes is first with key of type pair([]byte,uint64) and value of type pair(uint16,[]byte).
func (c *Cursor) SeekBytesUint64ToUint16Bytes(key1 []byte, key2 uint64) ([]byte, uint64, uint16, []byte) {
	rawKey, rawValue := c.Seek(bytesuint64Tobytes(key1, key2))
	key1, key2 = bytesTobytesuint64(rawKey)
	value1, value2 := bytesTouint16bytes(rawValue)
	return key1, key2, value1, value2
}

// ForEachBytesUint64ToUint32Bytes is a for each with key of type pair([]byte,uint64) and value of type pair(uint32,[]byte).
func (b *Bucket) ForEachBytesUint64ToUint32Bytes(fn func(key1 []byte, key2 uint64, value1 uint32, value2 []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTobytesuint64(rawKey)
			value1, value2 := bytesTouint32bytes(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetBytesUint64ToUint32Bytes is a get with key of type pair([]byte,uint64) and value of type pair(uint32,[]byte).
func (b *Bucket) GetBytesUint64ToUint32Bytes(key1 []byte, key2 uint64) (uint32, []byte) {
	return bytesTouint32bytes(b.Get(bytesuint64Tobytes(key1, key2)))
}

// GetRequiredBytesUint64ToUint32Bytes is a get with key of type pair([]byte,uint64) and value of type pair(uint32,[]byte) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesUint64ToUint32Bytes(key1 []byte, key2 uint64) (uint32, []byte, error) {
	value := b.Get(bytesuint64Tobytes(key1, key2))
	if value == nil {
		return 0, nil, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTouint32bytes(value)
	return value1, value2, nil
}

// PutBytesUint64ToUint32Bytes is a put with key of type pair([]byte,uint64) and value of type pair(uint32,[]byte).
func (b *Bucket) PutBytesUint64ToUint32Bytes(key1 []byte, key2 uint64, value1 uint32, value2 []byte) error {
	return b.Put(bytesuint64Tobytes(key1, key2), uint32bytesTobytes(value1, value2))
}

// FirstBytesUint64ToUint32Bytes is first with key of type pair([]byte,uint64) and value of type pair(uint32,[]byte).
func (c *Cursor) FirstBytesUint64ToUint32Bytes() ([]byte, uint64, uint32, []byte) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTobytesuint64(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key1, key2, value1, value2
}

// LastBytesUint64ToUint32Bytes is first with key of type pair([]byte,uint64) and value of type pair(uint32,[]byte).
func (c *Cursor) LastBytesUint64ToUint32Bytes() ([]byte, uint64, uint32, []byte) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTobytesuint64(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key1, key2, value1, value2
}

// NextBytesUint64ToUint32Bytes is first with key of type pair([]byte,uint64) and value of type pair(uint32,[]byte).
func (c *Cursor) NextBytesUint64ToUint32Bytes() ([]byte, uint64, uint32, []byte) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTobytesuint64(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key1, key2, value1, value2
}

// PrevBytesUint64ToUint32Bytes is first with key of type pair([]byte,uint64) and value of type pair(uint32,[]byte).
func (c *Cursor) PrevBytesUint64ToUint32Bytes() ([]byte, uint64, uint32, []byte) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTobytesuint64(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key1, key2, value1, value2
}

// SeekBytesUint64ToUint32Bytes is first with key of type pair([]byte,uint64) and value of type pair(uint32,[]byte).
func (c *Cursor) SeekBytesUint64ToUint32Bytes(key1 []byte, key2 uint64) ([]byte, uint64, uint32, []byte) {
	rawKey, rawValue := c.Seek(bytesuint64Tobytes(key1, key2))
	key1, key2 = bytesTobytesuint64(rawKey)
	value1, value2 := bytesTouint32bytes(rawValue)
	return key1, key2, value1, value2
}

// ForEachBytesUint64ToUint64Bytes is a for each with key of type pair([]byte,uint64) and value of type pair(uint64,[]byte).
func (b *Bucket) ForEachBytesUint64ToUint64Bytes(fn func(key1 []byte, key2 uint64, value1 uint64, value2 []byte) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTobytesuint64(rawKey)
			value1, value2 := bytesTouint64bytes(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetBytesUint64ToUint64Bytes is a get with key of type pair([]byte,uint64) and value of type pair(uint64,[]byte).
func (b *Bucket) GetBytesUint64ToUint64Bytes(key1 []byte, key2 uint64) (uint64, []byte) {
	return bytesTouint64bytes(b.Get(bytesuint64Tobytes(key1, key2)))
}

// GetRequiredBytesUint64ToUint64Bytes is a get with key of type pair([]byte,uint64) and value of type pair(uint64,[]byte) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesUint64ToUint64Bytes(key1 []byte, key2 uint64) (uint64, []byte, error) {
	value := b.Get(bytesuint64Tobytes(key1, key2))
	if value == nil {
		return 0, nil, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTouint64bytes(value)
	return value1, value2, nil
}

// PutBytesUint64ToUint64Bytes is a put with key of type pair([]byte,uint64) and value of type pair(uint64,[]byte).
func (b *Bucket) PutBytesUint64ToUint64Bytes(key1 []byte, key2 uint64, value1 uint64, value2 []byte) error {
	return b.Put(bytesuint64Tobytes(key1, key2), uint64bytesTobytes(value1, value2))
}

// FirstBytesUint64ToUint64Bytes is first with key of type pair([]byte,uint64) and value of type pair(uint64,[]byte).
func (c *Cursor) FirstBytesUint64ToUint64Bytes() ([]byte, uint64, uint64, []byte) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTobytesuint64(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key1, key2, value1, value2
}

// LastBytesUint64ToUint64Bytes is first with key of type pair([]byte,uint64) and value of type pair(uint64,[]byte).
func (c *Cursor) LastBytesUint64ToUint64Bytes() ([]byte, uint64, uint64, []byte) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTobytesuint64(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key1, key2, value1, value2
}

// NextBytesUint64ToUint64Bytes is first with key of type pair([]byte,uint64) and value of type pair(uint64,[]byte).
func (c *Cursor) NextBytesUint64ToUint64Bytes() ([]byte, uint64, uint64, []byte) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTobytesuint64(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key1, key2, value1, value2
}

// PrevBytesUint64ToUint64Bytes is first with key of type pair([]byte,uint64) and value of type pair(uint64,[]byte).
func (c *Cursor) PrevBytesUint64ToUint64Bytes() ([]byte, uint64, uint64, []byte) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTobytesuint64(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key1, key2, value1, value2
}

// SeekBytesUint64ToUint64Bytes is first with key of type pair([]byte,uint64) and value of type pair(uint64,[]byte).
func (c *Cursor) SeekBytesUint64ToUint64Bytes(key1 []byte, key2 uint64) ([]byte, uint64, uint64, []byte) {
	rawKey, rawValue := c.Seek(bytesuint64Tobytes(key1, key2))
	key1, key2 = bytesTobytesuint64(rawKey)
	value1, value2 := bytesTouint64bytes(rawValue)
	return key1, key2, value1, value2
}

// ForEachBytesUint64ToBytesUint16 is a for each with key of type pair([]byte,uint64) and value of type pair([]byte,uint16).
func (b *Bucket) ForEachBytesUint64ToBytesUint16(fn func(key1 []byte, key2 uint64, value1 []byte, value2 uint16) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTobytesuint64(rawKey)
			value1, value2 := bytesTobytesuint16(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetBytesUint64ToBytesUint16 is a get with key of type pair([]byte,uint64) and value of type pair([]byte,uint16).
func (b *Bucket) GetBytesUint64ToBytesUint16(key1 []byte, key2 uint64) ([]byte, uint16) {
	return bytesTobytesuint16(b.Get(bytesuint64Tobytes(key1, key2)))
}

// GetRequiredBytesUint64ToBytesUint16 is a get with key of type pair([]byte,uint64) and value of type pair([]byte,uint16) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesUint64ToBytesUint16(key1 []byte, key2 uint64) ([]byte, uint16, error) {
	value := b.Get(bytesuint64Tobytes(key1, key2))
	if value == nil {
		return nil, 0, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTobytesuint16(value)
	return value1, value2, nil
}

// PutBytesUint64ToBytesUint16 is a put with key of type pair([]byte,uint64) and value of type pair([]byte,uint16).
func (b *Bucket) PutBytesUint64ToBytesUint16(key1 []byte, key2 uint64, value1 []byte, value2 uint16) error {
	return b.Put(bytesuint64Tobytes(key1, key2), bytesuint16Tobytes(value1, value2))
}

// FirstBytesUint64ToBytesUint16 is first with key of type pair([]byte,uint64) and value of type pair([]byte,uint16).
func (c *Cursor) FirstBytesUint64ToBytesUint16() ([]byte, uint64, []byte, uint16) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTobytesuint64(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key1, key2, value1, value2
}

// LastBytesUint64ToBytesUint16 is first with key of type pair([]byte,uint64) and value of type pair([]byte,uint16).
func (c *Cursor) LastBytesUint64ToBytesUint16() ([]byte, uint64, []byte, uint16) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTobytesuint64(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key1, key2, value1, value2
}

// NextBytesUint64ToBytesUint16 is first with key of type pair([]byte,uint64) and value of type pair([]byte,uint16).
func (c *Cursor) NextBytesUint64ToBytesUint16() ([]byte, uint64, []byte, uint16) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTobytesuint64(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key1, key2, value1, value2
}

// PrevBytesUint64ToBytesUint16 is first with key of type pair([]byte,uint64) and value of type pair([]byte,uint16).
func (c *Cursor) PrevBytesUint64ToBytesUint16() ([]byte, uint64, []byte, uint16) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTobytesuint64(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key1, key2, value1, value2
}

// SeekBytesUint64ToBytesUint16 is first with key of type pair([]byte,uint64) and value of type pair([]byte,uint16).
func (c *Cursor) SeekBytesUint64ToBytesUint16(key1 []byte, key2 uint64) ([]byte, uint64, []byte, uint16) {
	rawKey, rawValue := c.Seek(bytesuint64Tobytes(key1, key2))
	key1, key2 = bytesTobytesuint64(rawKey)
	value1, value2 := bytesTobytesuint16(rawValue)
	return key1, key2, value1, value2
}

// ForEachBytesUint64ToBytesUint32 is a for each with key of type pair([]byte,uint64) and value of type pair([]byte,uint32).
func (b *Bucket) ForEachBytesUint64ToBytesUint32(fn func(key1 []byte, key2 uint64, value1 []byte, value2 uint32) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTobytesuint64(rawKey)
			value1, value2 := bytesTobytesuint32(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetBytesUint64ToBytesUint32 is a get with key of type pair([]byte,uint64) and value of type pair([]byte,uint32).
func (b *Bucket) GetBytesUint64ToBytesUint32(key1 []byte, key2 uint64) ([]byte, uint32) {
	return bytesTobytesuint32(b.Get(bytesuint64Tobytes(key1, key2)))
}

// GetRequiredBytesUint64ToBytesUint32 is a get with key of type pair([]byte,uint64) and value of type pair([]byte,uint32) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesUint64ToBytesUint32(key1 []byte, key2 uint64) ([]byte, uint32, error) {
	value := b.Get(bytesuint64Tobytes(key1, key2))
	if value == nil {
		return nil, 0, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTobytesuint32(value)
	return value1, value2, nil
}

// PutBytesUint64ToBytesUint32 is a put with key of type pair([]byte,uint64) and value of type pair([]byte,uint32).
func (b *Bucket) PutBytesUint64ToBytesUint32(key1 []byte, key2 uint64, value1 []byte, value2 uint32) error {
	return b.Put(bytesuint64Tobytes(key1, key2), bytesuint32Tobytes(value1, value2))
}

// FirstBytesUint64ToBytesUint32 is first with key of type pair([]byte,uint64) and value of type pair([]byte,uint32).
func (c *Cursor) FirstBytesUint64ToBytesUint32() ([]byte, uint64, []byte, uint32) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTobytesuint64(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key1, key2, value1, value2
}

// LastBytesUint64ToBytesUint32 is first with key of type pair([]byte,uint64) and value of type pair([]byte,uint32).
func (c *Cursor) LastBytesUint64ToBytesUint32() ([]byte, uint64, []byte, uint32) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTobytesuint64(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key1, key2, value1, value2
}

// NextBytesUint64ToBytesUint32 is first with key of type pair([]byte,uint64) and value of type pair([]byte,uint32).
func (c *Cursor) NextBytesUint64ToBytesUint32() ([]byte, uint64, []byte, uint32) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTobytesuint64(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key1, key2, value1, value2
}

// PrevBytesUint64ToBytesUint32 is first with key of type pair([]byte,uint64) and value of type pair([]byte,uint32).
func (c *Cursor) PrevBytesUint64ToBytesUint32() ([]byte, uint64, []byte, uint32) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTobytesuint64(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key1, key2, value1, value2
}

// SeekBytesUint64ToBytesUint32 is first with key of type pair([]byte,uint64) and value of type pair([]byte,uint32).
func (c *Cursor) SeekBytesUint64ToBytesUint32(key1 []byte, key2 uint64) ([]byte, uint64, []byte, uint32) {
	rawKey, rawValue := c.Seek(bytesuint64Tobytes(key1, key2))
	key1, key2 = bytesTobytesuint64(rawKey)
	value1, value2 := bytesTobytesuint32(rawValue)
	return key1, key2, value1, value2
}

// ForEachBytesUint64ToBytesUint64 is a for each with key of type pair([]byte,uint64) and value of type pair([]byte,uint64).
func (b *Bucket) ForEachBytesUint64ToBytesUint64(fn func(key1 []byte, key2 uint64, value1 []byte, value2 uint64) error) error {
	return b.ForEach(
		func(rawKey []byte, rawValue []byte) error {
			key1, key2 := bytesTobytesuint64(rawKey)
			value1, value2 := bytesTobytesuint64(rawValue)
			return fn(key1, key2, value1, value2)
		},
	)
}

// GetBytesUint64ToBytesUint64 is a get with key of type pair([]byte,uint64) and value of type pair([]byte,uint64).
func (b *Bucket) GetBytesUint64ToBytesUint64(key1 []byte, key2 uint64) ([]byte, uint64) {
	return bytesTobytesuint64(b.Get(bytesuint64Tobytes(key1, key2)))
}

// GetRequiredBytesUint64ToBytesUint64 is a get with key of type pair([]byte,uint64) and value of type pair([]byte,uint64) that returns a KeyNotFoundError if the key is not found.
func (b *Bucket) GetRequiredBytesUint64ToBytesUint64(key1 []byte, key2 uint64) ([]byte, uint64, error) {
	value := b.Get(bytesuint64Tobytes(key1, key2))
	if value == nil {
		return nil, 0, NewKeyNotFoundError(key1, key2)
	}
	value1, value2 := bytesTobytesuint64(value)
	return value1, value2, nil
}

// PutBytesUint64ToBytesUint64 is a put with key of type pair([]byte,uint64) and value of type pair([]byte,uint64).
func (b *Bucket) PutBytesUint64ToBytesUint64(key1 []byte, key2 uint64, value1 []byte, value2 uint64) error {
	return b.Put(bytesuint64Tobytes(key1, key2), bytesuint64Tobytes(value1, value2))
}

// FirstBytesUint64ToBytesUint64 is first with key of type pair([]byte,uint64) and value of type pair([]byte,uint64).
func (c *Cursor) FirstBytesUint64ToBytesUint64() ([]byte, uint64, []byte, uint64) {
	rawKey, rawValue := c.First()
	key1, key2 := bytesTobytesuint64(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key1, key2, value1, value2
}

// LastBytesUint64ToBytesUint64 is first with key of type pair([]byte,uint64) and value of type pair([]byte,uint64).
func (c *Cursor) LastBytesUint64ToBytesUint64() ([]byte, uint64, []byte, uint64) {
	rawKey, rawValue := c.Last()
	key1, key2 := bytesTobytesuint64(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key1, key2, value1, value2
}

// NextBytesUint64ToBytesUint64 is first with key of type pair([]byte,uint64) and value of type pair([]byte,uint64).
func (c *Cursor) NextBytesUint64ToBytesUint64() ([]byte, uint64, []byte, uint64) {
	rawKey, rawValue := c.Next()
	key1, key2 := bytesTobytesuint64(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key1, key2, value1, value2
}

// PrevBytesUint64ToBytesUint64 is first with key of type pair([]byte,uint64) and value of type pair([]byte,uint64).
func (c *Cursor) PrevBytesUint64ToBytesUint64() ([]byte, uint64, []byte, uint64) {
	rawKey, rawValue := c.Prev()
	key1, key2 := bytesTobytesuint64(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key1, key2, value1, value2
}

// SeekBytesUint64ToBytesUint64 is first with key of type pair([]byte,uint64) and value of type pair([]byte,uint64).
func (c *Cursor) SeekBytesUint64ToBytesUint64(key1 []byte, key2 uint64) ([]byte, uint64, []byte, uint64) {
	rawKey, rawValue := c.Seek(bytesuint64Tobytes(key1, key2))
	key1, key2 = bytesTobytesuint64(rawKey)
	value1, value2 := bytesTobytesuint64(rawValue)
	return key1, key2, value1, value2
}

// DeleteBytesUint64 is a delete with key of type pair([]byte,uint64).
func (b *Bucket) DeleteBytesUint64(key1 []byte, key2 uint64) error {
	return b.Delete(bytesuint64Tobytes(key1, key2))
}
