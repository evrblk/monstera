package monstera

import "encoding/binary"

func ConcatBytes(items ...interface{}) []byte {
	total := 0
	for _, item := range items {
		if item == nil {
			panic("nil item")
		}

		switch i := item.(type) {
		case uint64:
			total = total + 8
		case int64:
			total = total + 8
		case uint32:
			total = total + 4
		case int32:
			total = total + 4
		case []byte:
			total = total + len(i)
		case string:
			total = total + len(i)
		default:
			panic("wrong item type")
		}
	}

	key := make([]byte, total)

	c := 0
	for _, item := range items {
		if item == nil {
			panic("nil item")
		}

		switch i := item.(type) {
		case uint64:
			binary.BigEndian.PutUint64(key[c:], i)
			c = c + 8
		case int64:
			binary.BigEndian.PutUint64(key[c:], uint64(i))
			c = c + 8
		case uint32:
			binary.BigEndian.PutUint32(key[c:], i)
			c = c + 4
		case int32:
			binary.BigEndian.PutUint32(key[c:], uint32(i))
			c = c + 4
		case []byte:
			copy(key[c:], i)
			c = c + len(i)
		case string:
			copy(key[c:], i)
			c = c + len(i)
		}
	}

	return key
}
