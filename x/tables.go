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

// table helps working with a KV store without worrying about tableId prefixes.
type table struct {
	// keyLowerBound and keyUpperBound are used to define the range of keys that are stored in the table.
	// These bounds are inclusive and do not contain the tableId prefix.
	keyLowerBound []byte
	keyUpperBound []byte

	// tableId is a unique prefix that is used to isolate tables on a shared Badger store.
	tableId []byte

	// tableKeyRange is a range of keys that are stored in the table. It contains the tableId prefix.
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

func (t *table) listPrefixedPaginated(txn *monstera.Txn, prefix []byte, paginationToken *PaginationToken, limit int) (*rawListPaginatedResult, error) {
	result := &rawListPaginatedResult{
		Items: make([][]byte, 0),
	}

	if len(prefix) == 0 {
		prefix = t.keyLowerBound
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

// ProtobufTable is table with a composite key: primary key PK and secondary key SK
// and generic values (of proto.Message type).
// Get, Set, Delete operations use PK+SK to refer records. List operation uses PK as a prefix.
type ProtobufTable[T ptr[U], U any] struct {
	table
}

func NewProtobufTable[T ptr[U], U any](tableId []byte, keyLowerBound []byte, keyUpperBound []byte) *ProtobufTable[T, U] {
	return &ProtobufTable[T, U]{
		table: newTable(tableId, keyLowerBound, keyUpperBound),
	}
}

func (t *ProtobufTable[T, U]) Get(txn *monstera.Txn, key []byte) (T, error) {
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

func (t *ProtobufTable[T, U]) Set(txn *monstera.Txn, key []byte, message T) error {
	value, err := proto.Marshal(message)
	if err != nil {
		return err
	}

	return t.set(txn, key, value)
}

func (t *ProtobufTable[T, U]) Delete(txn *monstera.Txn, key []byte) error {
	return t.delete(txn, key)
}

func (t *ProtobufTable[T, U]) ListAll(txn *monstera.Txn, prefix []byte) ([]T, error) {
	result := make([]T, 0)
	err := t.eachPrefix(txn, prefix, func(key []byte, value []byte) (bool, error) {
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

func (t *ProtobufTable[T, U]) ListInRange(txn *monstera.Txn, lowerBound []byte, upperBound []byte, reverse bool, fn func(message T) (bool, error)) error {
	return t.listInRange(txn, lowerBound, upperBound, reverse, func(key []byte, value []byte) (bool, error) {
		var message U
		if err := proto.Unmarshal(value, T(&message)); err != nil {
			return false, err
		}

		return fn(&message)
	})
}

type ListPaginatedProtobufResult[T ptr[U], U any] struct {
	Items                   []T
	NextPaginationToken     *PaginationToken
	PreviousPaginationToken *PaginationToken
}

func (t *ProtobufTable[T, U]) ListPaginated(txn *monstera.Txn, prefix []byte, paginationToken *PaginationToken, limit int) (*ListPaginatedProtobufResult[T, U], error) {
	rawResult, err := t.listPrefixedPaginated(txn, prefix, paginationToken, limit)
	if err != nil {
		return nil, err
	}

	result := &ListPaginatedProtobufResult[T, U]{
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

// StringTable is table with a composite key: primary key PK and secondary key SK
// and string values.
// Get, Set, Delete operations use PK+SK to refer records. List operation uses PK as a prefix.
type StringTable struct {
	table
}

func NewStringTable(tableId []byte, keyLowerBound []byte, keyUpperBound []byte) *StringTable {
	return &StringTable{
		table: newTable(tableId, keyLowerBound, keyUpperBound),
	}
}

func (t *StringTable) Get(txn *monstera.Txn, key []byte) (string, error) {
	value, err := t.get(txn, key)
	if err != nil {
		return "", err
	}

	return string(value), nil
}

func (t *StringTable) Set(txn *monstera.Txn, key []byte, value string) error {
	return t.set(txn, key, []byte(value))
}

func (t *StringTable) Delete(txn *monstera.Txn, key []byte) error {
	return t.delete(txn, key)
}

func (t *StringTable) ListAll(txn *monstera.Txn, prefix []byte) ([]string, error) {
	result := make([]string, 0)
	err := t.eachPrefix(txn, prefix, func(key []byte, value []byte) (bool, error) {
		result = append(result, string(value))
		return true, nil
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (t *StringTable) ListInRange(txn *monstera.Txn, lowerBound []byte, upperBound []byte, reverse bool, fn func(value string) (bool, error)) error {
	return t.listInRange(txn, lowerBound, upperBound, reverse, func(key []byte, value []byte) (bool, error) {
		return fn(string(value))
	})
}

type ListPaginatedStringResult struct {
	Items                   []string
	NextPaginationToken     *PaginationToken
	PreviousPaginationToken *PaginationToken
}

func (t *StringTable) ListPaginated(txn *monstera.Txn, prefix []byte, paginationToken *PaginationToken, limit int) (*ListPaginatedStringResult, error) {
	rawResult, err := t.listPrefixedPaginated(txn, prefix, paginationToken, limit)
	if err != nil {
		return nil, err
	}

	result := &ListPaginatedStringResult{
		Items:                   make([]string, len(rawResult.Items)),
		NextPaginationToken:     rawResult.NextPaginationToken,
		PreviousPaginationToken: rawResult.PreviousPaginationToken,
	}

	for i, item := range rawResult.Items {
		result.Items[i] = string(item)
	}

	return result, nil
}

// Uint64Table is table with a composite key: primary key PK and secondary key SK
// and uint64 values.
// Get, Set, Delete operations use PK+SK to refer records. List operation uses PK as a prefix.
type Uint64Table struct {
	table
}

func NewUint64Table(tableId []byte, keyLowerBound []byte, keyUpperBound []byte) *Uint64Table {
	return &Uint64Table{
		table: newTable(tableId, keyLowerBound, keyUpperBound),
	}
}

func (t *Uint64Table) Get(txn *monstera.Txn, key []byte) (uint64, error) {
	value, err := t.get(txn, key)
	if err != nil {
		return 0, err
	}

	return bytesToUint64(value), nil
}

func (t *Uint64Table) Set(txn *monstera.Txn, key []byte, value uint64) error {
	return t.set(txn, key, uint64ToBytes(value))
}

func (t *Uint64Table) Delete(txn *monstera.Txn, key []byte) error {
	return t.delete(txn, key)
}

func (t *Uint64Table) ListAll(txn *monstera.Txn, prefix []byte) ([]uint64, error) {
	result := make([]uint64, 0)
	err := t.eachPrefix(txn, prefix, func(key []byte, value []byte) (bool, error) {
		result = append(result, bytesToUint64(value))
		return true, nil
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}

type ListPaginatedUint64Result struct {
	Items                   []uint64
	NextPaginationToken     *PaginationToken
	PreviousPaginationToken *PaginationToken
}

func (t *Uint64Table) ListPaginated(txn *monstera.Txn, prefix []byte, paginationToken *PaginationToken, limit int) (*ListPaginatedUint64Result, error) {
	rawResult, err := t.listPrefixedPaginated(txn, prefix, paginationToken, limit)
	if err != nil {
		return nil, err
	}

	result := &ListPaginatedUint64Result{
		Items:                   make([]uint64, len(rawResult.Items)),
		NextPaginationToken:     rawResult.NextPaginationToken,
		PreviousPaginationToken: rawResult.PreviousPaginationToken,
	}

	for i, item := range rawResult.Items {
		result.Items[i] = bytesToUint64(item)
	}

	return result, nil
}

// Uint32Table is table with a composite key: primary key PK and secondary key SK
// and uint32 values.
// Get, Set, Delete operations use PK+SK to refer records. List operation uses PK as a prefix.
type Uint32Table struct {
	table
}

func NewUint32Table(tableId []byte, keyLowerBound []byte, keyUpperBound []byte) *Uint32Table {
	return &Uint32Table{
		table: newTable(tableId, keyLowerBound, keyUpperBound),
	}
}

func (t *Uint32Table) Get(txn *monstera.Txn, key []byte) (uint32, error) {
	value, err := t.get(txn, key)
	if err != nil {
		return 0, err
	}

	return bytesToUint32(value), nil
}

func (t *Uint32Table) Set(txn *monstera.Txn, key []byte, value uint32) error {
	return t.set(txn, key, uint32ToBytes(value))
}

func (t *Uint32Table) Delete(txn *monstera.Txn, key []byte) error {
	return t.delete(txn, key)
}

func (t *Uint32Table) ListAll(txn *monstera.Txn, prefix []byte) ([]uint32, error) {
	result := make([]uint32, 0)
	err := t.eachPrefix(txn, prefix, func(key []byte, value []byte) (bool, error) {
		result = append(result, bytesToUint32(value))
		return true, nil
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}

type ListPaginatedUint32Result struct {
	Items                   []uint32
	NextPaginationToken     *PaginationToken
	PreviousPaginationToken *PaginationToken
}

func (t *Uint32Table) ListPaginated(txn *monstera.Txn, prefix []byte, paginationToken *PaginationToken, limit int) (*ListPaginatedUint32Result, error) {
	rawResult, err := t.listPrefixedPaginated(txn, prefix, paginationToken, limit)
	if err != nil {
		return nil, err
	}

	result := &ListPaginatedUint32Result{
		Items:                   make([]uint32, len(rawResult.Items)),
		NextPaginationToken:     rawResult.NextPaginationToken,
		PreviousPaginationToken: rawResult.PreviousPaginationToken,
	}

	for i, item := range rawResult.Items {
		result.Items[i] = bytesToUint32(item)
	}

	return result, nil
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
		return fn(key)
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
		return fn(key[len(i.tableId):])
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

// isWithinRange checks if a key is within a given range, both sides inclusive
func isWithinRange(key []byte, lowerBound []byte, upperBound []byte) bool {
	return bytes.Compare(key[:len(upperBound)], upperBound) <= 0 &&
		bytes.Compare(key[:len(lowerBound)], lowerBound) >= 0
}

func panicIfOutOfRange(key []byte, lowerBound []byte, upperBound []byte) {
	if !isWithinRange(key, lowerBound, upperBound) {
		panic("key is out of range!")
	}
}

// Converts bytes to uint64
func bytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// Converts uint64 to a byte slice
func uint64ToBytes(u uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, u)
	return buf
}

// Converts bytes to uint32
func bytesToUint32(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}

// Converts uint32 to a byte slice
func uint32ToBytes(u uint32) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, u)
	return buf
}
