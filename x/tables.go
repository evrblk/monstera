package monsterax

import (
	"bytes"
	"encoding/binary"

	"github.com/evrblk/monstera"
	"google.golang.org/protobuf/proto"
)

// ptr is a generic constraint for proto messages (pointers)
type ptr[T any] interface {
	*T
	proto.Message
}

type PaginationToken struct {
	Key     []byte
	Reverse bool
}

type ListPaginatedResult[T ptr[U], U any] struct {
	Items                   []T
	NextPaginationToken     *PaginationToken
	PreviousPaginationToken *PaginationToken
}

type table struct {
	keyLowerBound []byte
	keyUpperBound []byte
	tableId       []byte
	tableKeyRange *monstera.KeyRange
}

func newTable(tableId []byte, keyLowerBound []byte, keyUpperBound []byte) table {
	tableKeyRange := &monstera.KeyRange{
		Lower: monstera.ConcatBytes(tableId, keyLowerBound),
		Upper: monstera.ConcatBytes(tableId, keyUpperBound),
	}

	return table{
		keyLowerBound: keyLowerBound,
		keyUpperBound: keyUpperBound,
		tableId:       tableId,
		tableKeyRange: tableKeyRange,
	}
}

func (t *table) get(txn *monstera.Txn, key []byte) ([]byte, error) {
	return txn.Get(t.getFullKey(key))
}

func (t *table) set(txn *monstera.Txn, key []byte, value []byte) error {
	return txn.Set(t.getFullKey(key), value)
}

func (t *table) delete(txn *monstera.Txn, key []byte) error {
	return txn.Delete(t.getFullKey(key))
}

func (t *table) prefixExists(txn *monstera.Txn, prefix []byte) (bool, error) {
	return txn.PrefixExists(t.getFullKey(prefix))
}

func (t *table) eachPrefix(txn *monstera.Txn, prefix []byte, fn func(key []byte, value []byte) (bool, error)) error {
	return txn.EachPrefix(t.getFullKey(prefix), func(key []byte, value []byte) (bool, error) {
		return fn(key[len(t.tableId):], value)
	})
}

func (t *table) eachPrefixKeys(txn *monstera.Txn, prefix []byte, fn func(key []byte) (bool, error)) error {
	return txn.EachPrefixKeys(t.getFullKey(prefix), func(key []byte) (bool, error) {
		return fn(key[len(t.tableId):])
	})
}

func (t *table) listInRange(txn *monstera.Txn, lowerBound []byte, upperBound []byte, reverse bool, fn func(key []byte, value []byte) (bool, error)) error {
	var lower []byte
	if lowerBound != nil {
		lower = t.getFullKey(lowerBound)
	} else {
		lower = t.tableKeyRange.Lower
	}

	var upper []byte
	if upperBound != nil {
		upper = t.getFullKey(upperBound)
	} else {
		upper = t.tableKeyRange.Upper
	}

	return txn.EachRange(lower, upper, reverse, func(key []byte, value []byte) (bool, error) {
		return fn(key[len(t.tableId):], value)
	})
}

type rawListPaginatedResult struct {
	Items                   [][]byte
	NextPaginationToken     *PaginationToken
	PreviousPaginationToken *PaginationToken
}

func (t *table) listPaginated(txn *monstera.Txn, paginationToken *PaginationToken, limit int) (*rawListPaginatedResult, error) {
	result := &rawListPaginatedResult{
		Items: make([][]byte, 0),
	}

	if paginationToken == nil {
		err := t.listInRange(txn, t.keyLowerBound, t.keyUpperBound, false, func(key []byte, value []byte) (bool, error) {
			if len(result.Items) == limit {
				result.NextPaginationToken = &PaginationToken{
					Key:     key,
					Reverse: false,
				}
				return false, nil
			} else {
				result.Items = append(result.Items, value)
				return true, nil
			}
		})
		if err != nil {
			return nil, err
		}
	} else if !paginationToken.Reverse {
		err := t.listInRange(txn, t.keyLowerBound, paginationToken.Key, true, func(key []byte, value []byte) (bool, error) {
			if bytes.Equal(key, paginationToken.Key) {
				return true, nil
			}

			result.PreviousPaginationToken = &PaginationToken{
				Key:     key,
				Reverse: true,
			}
			return false, nil
		})
		if err != nil {
			return nil, err
		}

		err = t.listInRange(txn, paginationToken.Key, t.keyUpperBound, false, func(key []byte, value []byte) (bool, error) {
			if len(result.Items) == limit {
				result.NextPaginationToken = &PaginationToken{
					Key:     key,
					Reverse: false,
				}
				return false, nil
			} else {
				result.Items = append(result.Items, value)
				return true, nil
			}
		})
		if err != nil {
			return nil, err
		}
	} else {
		err := t.listInRange(txn, paginationToken.Key, t.keyUpperBound, false, func(key []byte, value []byte) (bool, error) {
			if bytes.Equal(key, paginationToken.Key) {
				return true, nil
			}

			result.NextPaginationToken = &PaginationToken{
				Key:     key,
				Reverse: false,
			}
			return false, nil
		})
		if err != nil {
			return nil, err
		}

		err = t.listInRange(txn, t.keyLowerBound, paginationToken.Key, true, func(key []byte, value []byte) (bool, error) {
			if len(result.Items) == limit {
				result.PreviousPaginationToken = &PaginationToken{
					Key:     key,
					Reverse: true,
				}
				return false, nil
			} else {
				result.Items = append(result.Items, value)
				return true, nil
			}
		})
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

func (t *table) listPrefixedPaginated(txn *monstera.Txn, prefix []byte, paginationToken *PaginationToken, limit int) (*rawListPaginatedResult, error) {
	result := &rawListPaginatedResult{
		Items: make([][]byte, 0),
	}

	if paginationToken == nil {
		err := t.eachPrefix(txn, prefix, func(key []byte, value []byte) (bool, error) {
			if len(result.Items) == limit {
				result.NextPaginationToken = &PaginationToken{
					Key:     key,
					Reverse: false,
				}
				return false, nil
			} else {
				result.Items = append(result.Items, value)
				return true, nil
			}
		})
		if err != nil {
			return nil, err
		}
	} else if !paginationToken.Reverse {
		err := t.listInRange(txn, t.keyLowerBound, paginationToken.Key, true, func(key []byte, value []byte) (bool, error) {
			if !bytes.HasPrefix(key, prefix) {
				return false, nil
			}

			if bytes.Equal(key, paginationToken.Key) {
				return true, nil
			}

			result.PreviousPaginationToken = &PaginationToken{
				Key:     key,
				Reverse: true,
			}
			return false, nil
		})
		if err != nil {
			return nil, err
		}

		err = t.listInRange(txn, paginationToken.Key, t.keyUpperBound, false, func(key []byte, value []byte) (bool, error) {
			if !bytes.HasPrefix(key, prefix) {
				return false, nil
			}

			if len(result.Items) == limit {
				result.NextPaginationToken = &PaginationToken{
					Key:     key,
					Reverse: false,
				}
				return false, nil
			} else {
				result.Items = append(result.Items, value)
				return true, nil
			}
		})
		if err != nil {
			return nil, err
		}
	} else {
		err := t.listInRange(txn, paginationToken.Key, t.keyUpperBound, false, func(key []byte, value []byte) (bool, error) {
			if !bytes.HasPrefix(key, prefix) {
				return false, nil
			}

			if bytes.Equal(key, paginationToken.Key) {
				return true, nil
			}

			result.NextPaginationToken = &PaginationToken{
				Key:     key,
				Reverse: false,
			}
			return false, nil
		})
		if err != nil {
			return nil, err
		}

		err = t.listInRange(txn, t.keyLowerBound, paginationToken.Key, true, func(key []byte, value []byte) (bool, error) {
			if !bytes.HasPrefix(key, prefix) {
				return false, nil
			}

			if len(result.Items) == limit {
				result.PreviousPaginationToken = &PaginationToken{
					Key:     key,
					Reverse: true,
				}
				return false, nil
			} else {
				result.Items = append(result.Items, value)
				return true, nil
			}
		})
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

func (t *table) getFullKey(key []byte) []byte {
	fullKey := monstera.ConcatBytes(t.tableId, key)
	panicIfOutOfRange(fullKey, t.tableKeyRange.Lower, t.tableKeyRange.Upper)
	return fullKey
}

func (t *table) GetTableKeyRange() monstera.KeyRange {
	return *t.tableKeyRange
}

// CompositeKeyTable is table with a composite key: primary key PK and secondary key SK
// and generic values (of proto.Message type).
// Get, Set, Delete operations use PK+SK to refer records. List operation uses PK as a prefix.
type CompositeKeyTable[T ptr[U], U any] struct {
	table
}

func NewCompositeKeyTable[T ptr[U], U any](tableId []byte, keyLowerBound []byte, keyUpperBound []byte) *CompositeKeyTable[T, U] {
	return &CompositeKeyTable[T, U]{
		table: newTable(tableId, keyLowerBound, keyUpperBound),
	}
}

func (t *CompositeKeyTable[T, U]) Get(txn *monstera.Txn, pk []byte, sk []byte) (T, error) {
	key := monstera.ConcatBytes(pk, sk)

	value, err := t.get(txn, key)
	if err != nil {
		return nil, err
	}

	var message U
	if err := proto.Unmarshal(value, T(&message)); err != nil {
		return nil, err
	}
	return &message, nil
}

func (t *CompositeKeyTable[T, U]) Set(txn *monstera.Txn, pk []byte, sk []byte, message T) error {
	key := monstera.ConcatBytes(pk, sk)

	value, err := proto.Marshal(message)
	if err != nil {
		return err
	}

	return t.set(txn, key, value)
}

func (t *CompositeKeyTable[T, U]) Delete(txn *monstera.Txn, pk []byte, sk []byte) error {
	key := monstera.ConcatBytes(pk, sk)
	return t.delete(txn, key)
}

func (t *CompositeKeyTable[T, U]) ListAll(txn *monstera.Txn, pk []byte) ([]T, error) {
	result := make([]T, 0)
	err := t.eachPrefix(txn, pk, func(key []byte, value []byte) (bool, error) {
		var message U
		if err := proto.Unmarshal(value, T(&message)); err != nil {
			return false, err
		}

		result = append(result, T(&message))
		return true, nil
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (t *CompositeKeyTable[T, U]) ListPaginated(txn *monstera.Txn, pk []byte, paginationToken *PaginationToken, limit int) (*ListPaginatedResult[T, U], error) {
	rawResult, err := t.listPrefixedPaginated(txn, pk, paginationToken, limit)
	if err != nil {
		return nil, err
	}

	result := &ListPaginatedResult[T, U]{
		Items:                   make([]T, len(rawResult.Items)),
		NextPaginationToken:     rawResult.NextPaginationToken,
		PreviousPaginationToken: rawResult.PreviousPaginationToken,
	}

	for i, item := range rawResult.Items {
		var message U
		if err := proto.Unmarshal(item, T(&message)); err != nil {
			return nil, err
		}
		result.Items[i] = T(&message)
	}

	return result, nil
}

// SimpleKeyTable is table with a simple key of primary key PK only and generic values (of proto.Message type).
// Get, Set, Delete operations use PK. List operation lists through ALL keys. ListInRange operation lists only
// between lowerBound and upperBound.
type SimpleKeyTable[T ptr[U], U any] struct {
	table
}

func NewSimpleKeyTable[T ptr[U], U any](tableId []byte, keyLowerBound []byte, keyUpperBound []byte) *SimpleKeyTable[T, U] {
	return &SimpleKeyTable[T, U]{
		table: newTable(tableId, keyLowerBound, keyUpperBound),
	}
}

func (t *SimpleKeyTable[T, U]) Get(txn *monstera.Txn, pk []byte) (T, error) {
	value, err := t.get(txn, pk)
	if err != nil {
		return nil, err
	}

	var message U
	if err := proto.Unmarshal(value, T(&message)); err != nil {
		return nil, err
	}
	return &message, nil
}

func (t *SimpleKeyTable[T, U]) Set(txn *monstera.Txn, pk []byte, message T) error {
	value, err := proto.Marshal(message)
	if err != nil {
		return err
	}

	return t.set(txn, pk, value)
}

func (t *SimpleKeyTable[T, U]) Delete(txn *monstera.Txn, pk []byte) error {
	return t.delete(txn, pk)
}

func (t *SimpleKeyTable[T, U]) ListAll(txn *monstera.Txn) ([]T, error) {
	result := make([]T, 0)
	err := t.listInRange(txn, t.keyLowerBound, t.keyUpperBound, false, func(key []byte, value []byte) (bool, error) {
		var message U
		if err := proto.Unmarshal(value, T(&message)); err != nil {
			return false, err
		}
		result = append(result, T(&message))
		return true, nil
	})
	return result, err
}

func (t *SimpleKeyTable[T, U]) ListPaginated(txn *monstera.Txn, paginationToken *PaginationToken, limit int) (*ListPaginatedResult[T, U], error) {
	rawResult, err := t.listPaginated(txn, paginationToken, limit)
	if err != nil {
		return nil, err
	}

	result := &ListPaginatedResult[T, U]{
		Items:                   make([]T, len(rawResult.Items)),
		NextPaginationToken:     rawResult.NextPaginationToken,
		PreviousPaginationToken: rawResult.PreviousPaginationToken,
	}

	for i, item := range rawResult.Items {
		var message U
		if err := proto.Unmarshal(item, T(&message)); err != nil {
			return nil, err
		}
		result.Items[i] = T(&message)
	}

	return result, nil
}

func (t *SimpleKeyTable[T, U]) ListInRange(txn *monstera.Txn, lowerBound []byte, upperBound []byte, reverse bool, fn func(message T) (bool, error)) error {
	return t.listInRange(txn, lowerBound, upperBound, reverse, eachPrefixProtoFunc(fn))
}

// UniqueUint64Index stores unique keys and returns uint64 as values.
type UniqueUint64Index struct {
	table
}

func NewUniqueUint64Index(tableId []byte, keyLowerBound []byte, keyUpperBound []byte) *UniqueUint64Index {
	return &UniqueUint64Index{
		table: newTable(tableId, keyLowerBound, keyUpperBound),
	}
}

func (i *UniqueUint64Index) Get(txn *monstera.Txn, pk []byte) (uint64, error) {
	value, err := i.get(txn, pk)
	if err != nil {
		return 0, err
	}
	return bytesToUint64(value), nil
}

func (i *UniqueUint64Index) Set(txn *monstera.Txn, pk []byte, value uint64) error {
	return i.set(txn, pk, uint64ToBytes(value))
}

func (i *UniqueUint64Index) Delete(txn *monstera.Txn, pk []byte) error {
	return i.delete(txn, pk)
}

// OneToManyUint64Index stores multiple items (uint64) per single key PK (arbitrary []byte).
type OneToManyUint64Index struct {
	table
}

func NewOneToManyUint64Index(tableId []byte, keyLowerBound []byte, keyUpperBound []byte) *OneToManyUint64Index {
	return &OneToManyUint64Index{
		table: newTable(tableId, keyLowerBound, keyUpperBound),
	}
}

func (i *OneToManyUint64Index) List(txn *monstera.Txn, pk []byte, fn func(item uint64) (bool, error)) error {
	return i.eachPrefixKeys(txn, pk, func(key []byte) (bool, error) {
		return fn(bytesToUint64(key[len(pk):]))
	})
}

func (i *OneToManyUint64Index) Add(txn *monstera.Txn, pk []byte, item uint64) error {
	key := monstera.ConcatBytes(pk, item)
	return i.set(txn, key, nil)
}

func (i *OneToManyUint64Index) Delete(txn *monstera.Txn, pk []byte, item uint64) error {
	key := monstera.ConcatBytes(pk, item)
	return i.delete(txn, key)
}

func (i *OneToManyUint64Index) NotEmpty(txn *monstera.Txn, pk []byte) (bool, error) {
	return i.prefixExists(txn, pk)
}

// CompositeKeyTableUint64 is table with a composite key: primary key PK and secondary key SK
// and uint64 values.
// Get, Set, Delete operations use PK+SK to refer records. List operation uses PK as a prefix.
type CompositeKeyTableUint64 struct {
	table
}

func NewCompositeKeyTableUint64(tableId []byte, keyLowerBound []byte, keyUpperBound []byte) *CompositeKeyTableUint64 {
	return &CompositeKeyTableUint64{
		table: newTable(tableId, keyLowerBound, keyUpperBound),
	}
}

func (t *CompositeKeyTableUint64) Get(txn *monstera.Txn, pk []byte, sk []byte) (uint64, error) {
	key := monstera.ConcatBytes(pk, sk)

	value, err := t.get(txn, key)
	if err != nil {
		return 0, err
	}
	return bytesToUint64(value), nil
}

func (t *CompositeKeyTableUint64) Set(txn *monstera.Txn, pk []byte, sk []byte, value uint64) error {
	key := monstera.ConcatBytes(pk, sk)
	return t.set(txn, key, uint64ToBytes(value))
}

func (t *CompositeKeyTableUint64) Delete(txn *monstera.Txn, pk []byte, sk []byte) error {
	key := monstera.ConcatBytes(pk, sk)
	return t.delete(txn, key)
}

func (t *CompositeKeyTableUint64) List(txn *monstera.Txn, pk []byte, fn func(value uint64) (bool, error)) error {
	return t.eachPrefix(txn, pk, func(key []byte, value []byte) (bool, error) {
		return fn(bytesToUint64(value))
	})
}

// OneToManySortedIndex stores multiple items (arbitrary []byte) per single key PK (arbitrary []byte).
type OneToManySortedIndex struct {
	table
}

func NewOneToManySortedIndex(tableId []byte, keyLowerBound []byte, keyUpperBound []byte) *OneToManySortedIndex {
	return &OneToManySortedIndex{
		table: newTable(tableId, keyLowerBound, keyUpperBound),
	}
}

func (i *OneToManySortedIndex) ListAll(txn *monstera.Txn, pk []byte, fn func(item []byte) (bool, error)) error {
	return i.eachPrefixKeys(txn, pk, func(key []byte) (bool, error) {
		return fn(key[len(pk):])
	})
}

func (i *OneToManySortedIndex) ListInRange(txn *monstera.Txn, pk []byte, lowerBound []byte, upperBound []byte, fn func(item []byte) (bool, error)) error {
	lower := monstera.ConcatBytes(pk, lowerBound)
	upper := monstera.ConcatBytes(pk, upperBound)

	return i.listInRange(txn, lower, upper, false, func(key []byte, value []byte) (bool, error) {
		return fn(key[len(pk):])
	})
}

func (i *OneToManySortedIndex) Add(txn *monstera.Txn, pk []byte, item []byte) error {
	key := monstera.ConcatBytes(pk, item)
	return i.set(txn, key, nil)
}

func (i *OneToManySortedIndex) Delete(txn *monstera.Txn, pk []byte, item []byte) error {
	key := monstera.ConcatBytes(pk, item)
	return i.delete(txn, key)
}

// SortedIndex stores multiple sorted items (arbitrary []byte) without any PK (global index)
type SortedIndex struct {
	table
}

func NewSortedIndex(tableId []byte, keyLowerBound []byte, keyUpperBound []byte) *SortedIndex {
	return &SortedIndex{
		table: newTable(tableId, keyLowerBound, keyUpperBound),
	}
}

func (i *SortedIndex) ListInRange(txn *monstera.Txn, lowerBound []byte, upperBound []byte, fn func(item []byte) (bool, error)) error {
	return i.listInRange(txn, lowerBound, upperBound, false, func(key []byte, value []byte) (bool, error) {
		return fn(key)
	})
}

func (i *SortedIndex) Add(txn *monstera.Txn, item []byte) error {
	return i.set(txn, item, nil)
}

func (i *SortedIndex) Delete(txn *monstera.Txn, item []byte) error {
	return i.delete(txn, item)
}

func (i *SortedIndex) GetTableKeyRange() monstera.KeyRange {
	return *i.tableKeyRange
}

func isWithingRange(key []byte, lowerBound []byte, upperBound []byte) bool {
	return bytes.Compare(key[:len(upperBound)], upperBound) <= 0 &&
		bytes.Compare(key[:len(lowerBound)], lowerBound) >= 0
}

func panicIfOutOfRange(key []byte, lowerBound []byte, upperBound []byte) {
	if !isWithingRange(key, lowerBound, upperBound) {
		panic("key is out of range!")
	}
}

// Converts bytes to an integer
func bytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// Converts uint64 to a byte slice
func uint64ToBytes(u uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, u)
	return buf
}

func eachPrefixProtoFunc[T ptr[U], U any](fn func(message T) (bool, error)) func(key []byte, value []byte) (bool, error) {
	return func(key []byte, value []byte) (bool, error) {
		var message U
		if err := proto.Unmarshal(value, T(&message)); err != nil {
			return false, err
		}
		return fn(&message)
	}
}
