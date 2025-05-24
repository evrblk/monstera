package monsterax

import (
	"bytes"
	"encoding/binary"

	"github.com/evrblk/monstera"
	"google.golang.org/protobuf/proto"
)

func ListKeys(txn *monstera.Txn, tableId []byte, pk []byte, fn func(key []byte) (bool, error)) error {
	prefix := monstera.ConcatBytes(tableId, pk)

	return txn.EachPrefixKeys(prefix, fn)
}

func ListRange(txn *monstera.Txn, tableId []byte, pk []byte, lowerBound []byte, upperBound []byte, fn func(key []byte, value []byte) (bool, error)) error {
	lower := monstera.ConcatBytes(tableId, pk, lowerBound)

	var upper []byte
	if upperBound != nil {
		upper = monstera.ConcatBytes(tableId, pk, upperBound)
	}

	return txn.EachRange(lower, upper, fn)
}

// ptr is a generic constraint for proto messages (pointers)
type ptr[T any] interface {
	*T
	proto.Message
}

// CompositeKeyTable is table with a composite key: primary key PK and secondary key SK
// and generic values (of proto.Message type).
// Get, Set, Delete operations use PK+SK to refer records. List operation uses PK as a prefix.
type CompositeKeyTable[T ptr[U], U any] struct {
	keyLowerBound []byte
	keyUpperBound []byte
	tableId       []byte
	tableKeyRange *monstera.KeyRange
}

func NewCompositeKeyTable[T ptr[U], U any](tableId []byte, keyLowerBound []byte, keyUpperBound []byte) *CompositeKeyTable[T, U] {
	tableKeyRange := &monstera.KeyRange{
		Lower: monstera.ConcatBytes(tableId, keyLowerBound),
		Upper: monstera.ConcatBytes(tableId, keyUpperBound),
	}

	return &CompositeKeyTable[T, U]{
		keyLowerBound: keyLowerBound,
		keyUpperBound: keyUpperBound,
		tableId:       tableId,
		tableKeyRange: tableKeyRange,
	}
}

func (t *CompositeKeyTable[T, U]) Get(txn *monstera.Txn, pk []byte, sk []byte) (T, error) {
	key := monstera.ConcatBytes(t.tableId, pk, sk)
	if !isWithingRange(key, t.tableKeyRange.Lower, t.tableKeyRange.Upper) {
		panic("key is out of range!")
	}

	value, err := txn.Get(key)
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
	key := monstera.ConcatBytes(t.tableId, pk, sk)
	if !isWithingRange(key, t.tableKeyRange.Lower, t.tableKeyRange.Upper) {
		panic("key is out of range!")
	}

	value, err := proto.Marshal(message)
	if err != nil {
		return err
	}

	return txn.Set(key, value)
}

func (t *CompositeKeyTable[T, U]) Delete(txn *monstera.Txn, pk []byte, sk []byte) error {
	key := monstera.ConcatBytes(t.tableId, pk, sk)
	if !isWithingRange(key, t.tableKeyRange.Lower, t.tableKeyRange.Upper) {
		panic("key is out of range!")
	}

	return txn.Delete(key)
}

func (t *CompositeKeyTable[T, U]) List(txn *monstera.Txn, pk []byte, fn func(message T) (bool, error)) error {
	prefix := monstera.ConcatBytes(t.tableId, pk)

	return txn.EachPrefix(prefix, func(key []byte, value []byte) (bool, error) {
		if !isWithingRange(key, t.tableKeyRange.Lower, t.tableKeyRange.Upper) {
			panic("key is out of range!")
		}

		var message U
		if err := proto.Unmarshal(value, T(&message)); err != nil {
			return false, err
		}
		return fn(&message)
	})
}

func (t *CompositeKeyTable[T, U]) GetTableKeyRange() monstera.KeyRange {
	return *t.tableKeyRange
}

// SimpleKeyTable is table with a simple key of primary key PK only and generic values (of proto.Message type).
// Get, Set, Delete operations use PK. List operation lists through ALL keys. ListInRange operation lists only
// between lowerBound and upperBound.
type SimpleKeyTable[T ptr[U], U any] struct {
	keyLowerBound []byte
	keyUpperBound []byte
	tableId       []byte
	tableKeyRange *monstera.KeyRange
}

func NewSimpleKeyTable[T ptr[U], U any](tableId []byte, keyLowerBound []byte, keyUpperBound []byte) *SimpleKeyTable[T, U] {
	tableKeyRange := &monstera.KeyRange{
		Lower: monstera.ConcatBytes(tableId, keyLowerBound),
		Upper: monstera.ConcatBytes(tableId, keyUpperBound),
	}

	return &SimpleKeyTable[T, U]{
		keyLowerBound: keyLowerBound,
		keyUpperBound: keyUpperBound,
		tableId:       tableId,
		tableKeyRange: tableKeyRange,
	}
}

func (t *SimpleKeyTable[T, U]) Get(txn *monstera.Txn, pk []byte) (T, error) {
	key := monstera.ConcatBytes(t.tableId, pk)
	if !isWithingRange(key, t.tableKeyRange.Lower, t.tableKeyRange.Upper) {
		panic("key is out of range!")
	}

	value, err := txn.Get(key)

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
	key := monstera.ConcatBytes(t.tableId, pk)
	if !isWithingRange(key, t.tableKeyRange.Lower, t.tableKeyRange.Upper) {
		panic("key is out of range!")
	}

	value, err := proto.Marshal(message)
	if err != nil {
		return err
	}

	return txn.Set(key, value)
}

func (t *SimpleKeyTable[T, U]) Delete(txn *monstera.Txn, pk []byte) error {
	key := monstera.ConcatBytes(t.tableId, pk)
	if !isWithingRange(key, t.tableKeyRange.Lower, t.tableKeyRange.Upper) {
		panic("key is out of range!")
	}

	return txn.Delete(key)
}

func (t *SimpleKeyTable[T, U]) List(txn *monstera.Txn, fn func(message T) (bool, error)) error {
	prefix := monstera.ConcatBytes(t.tableId)

	return txn.EachPrefix(prefix, func(key []byte, value []byte) (bool, error) {
		if !isWithingRange(key, t.tableKeyRange.Lower, t.tableKeyRange.Upper) {
			panic("key is out of range!")
		}

		var message U
		if err := proto.Unmarshal(value, T(&message)); err != nil {
			return false, err
		}
		return fn(&message)
	})
}

func (t *SimpleKeyTable[T, U]) ListInRange(txn *monstera.Txn, lowerBound []byte, upperBound []byte, fn func(message T) (bool, error)) error {
	return ListRange(txn, t.tableId, []byte{}, lowerBound, upperBound, func(key []byte, value []byte) (bool, error) {
		if !isWithingRange(key, t.tableKeyRange.Lower, t.tableKeyRange.Upper) {
			panic("key is out of range!")
		}

		var message U
		if err := proto.Unmarshal(value, T(&message)); err != nil {
			return false, err
		}
		return fn(&message)
	})
}

func (t *SimpleKeyTable[T, U]) GetTableKeyRange() monstera.KeyRange {
	return *t.tableKeyRange
}

// UniqueUint64Index stores unique keys and returns uint64 as values.
type UniqueUint64Index struct {
	keyLowerBound []byte
	keyUpperBound []byte
	tableId       []byte
	tableKeyRange *monstera.KeyRange
}

func NewUniqueUint64Index(tableId []byte, keyLowerBound []byte, keyUpperBound []byte) *UniqueUint64Index {
	tableKeyRange := &monstera.KeyRange{
		Lower: monstera.ConcatBytes(tableId, keyLowerBound),
		Upper: monstera.ConcatBytes(tableId, keyUpperBound),
	}

	return &UniqueUint64Index{
		keyLowerBound: keyLowerBound,
		keyUpperBound: keyUpperBound,
		tableId:       tableId,
		tableKeyRange: tableKeyRange,
	}
}

func (i *UniqueUint64Index) Get(txn *monstera.Txn, pk []byte) (uint64, error) {
	key := monstera.ConcatBytes(i.tableId, pk)
	if !isWithingRange(key, i.tableKeyRange.Lower, i.tableKeyRange.Upper) {
		panic("key is out of range!")
	}

	value, err := txn.Get(key)
	if err != nil {
		return 0, err
	}
	return bytesToUint64(value), nil
}

func (i *UniqueUint64Index) Set(txn *monstera.Txn, pk []byte, value uint64) error {
	key := monstera.ConcatBytes(i.tableId, pk)
	if !isWithingRange(key, i.tableKeyRange.Lower, i.tableKeyRange.Upper) {
		panic("key is out of range!")
	}

	return txn.Set(key, uint64ToBytes(value))
}

func (i *UniqueUint64Index) Delete(txn *monstera.Txn, pk []byte) error {
	key := monstera.ConcatBytes(i.tableId, pk)
	if !isWithingRange(key, i.tableKeyRange.Lower, i.tableKeyRange.Upper) {
		panic("key is out of range!")
	}

	return txn.Delete(key)
}

func (i *UniqueUint64Index) GetTableKeyRange() monstera.KeyRange {
	return *i.tableKeyRange
}

// OneToManyUint64Index stores multiple items (uint64) per single key PK (arbitrary []byte).
type OneToManyUint64Index struct {
	keyLowerBound []byte
	keyUpperBound []byte
	tableId       []byte
	tableKeyRange *monstera.KeyRange
}

func NewOneToManyUint64Index(tableId []byte, keyLowerBound []byte, keyUpperBound []byte) *OneToManyUint64Index {
	tableKeyRange := &monstera.KeyRange{
		Lower: monstera.ConcatBytes(tableId, keyLowerBound),
		Upper: monstera.ConcatBytes(tableId, keyUpperBound),
	}

	return &OneToManyUint64Index{
		keyLowerBound: keyLowerBound,
		keyUpperBound: keyUpperBound,
		tableId:       tableId,
		tableKeyRange: tableKeyRange,
	}
}

func (i *OneToManyUint64Index) List(txn *monstera.Txn, pk []byte, fn func(item uint64) (bool, error)) error {
	return ListKeys(txn, i.tableId, pk, func(key []byte) (bool, error) {
		if !isWithingRange(key, i.tableKeyRange.Lower, i.tableKeyRange.Upper) {
			panic("key is out of range!")
		}

		return fn(bytesToUint64(key[len(key)-8:]))
	})
}

func (i *OneToManyUint64Index) Add(txn *monstera.Txn, pk []byte, item uint64) error {
	key := monstera.ConcatBytes(i.tableId, pk, item)
	if !isWithingRange(key, i.tableKeyRange.Lower, i.tableKeyRange.Upper) {
		panic("key is out of range!")
	}

	return txn.Set(key, nil)
}

func (i *OneToManyUint64Index) Delete(txn *monstera.Txn, pk []byte, item uint64) error {
	key := monstera.ConcatBytes(i.tableId, pk, item)
	if !isWithingRange(key, i.tableKeyRange.Lower, i.tableKeyRange.Upper) {
		panic("key is out of range!")
	}

	return txn.Delete(key)
}

func (i *OneToManyUint64Index) NotEmpty(txn *monstera.Txn, pk []byte) (bool, error) {
	prefix := monstera.ConcatBytes(i.tableId, pk)

	if !isWithingRange(prefix, i.tableKeyRange.Lower, i.tableKeyRange.Upper) {
		panic("key is out of range!")
	}

	return txn.PrefixExists(prefix)
}

func (i *OneToManyUint64Index) GetTableKeyRange() monstera.KeyRange {
	return *i.tableKeyRange
}

// CompositeKeyTableUint64 is table with a composite key: primary key PK and secondary key SK
// and uint64 values.
// Get, Set, Delete operations use PK+SK to refer records. List operation uses PK as a prefix.
type CompositeKeyTableUint64 struct {
	keyLowerBound []byte
	keyUpperBound []byte
	tableId       []byte
	tableKeyRange *monstera.KeyRange
}

func NewCompositeKeyTableUint64(tableId []byte, keyLowerBound []byte, keyUpperBound []byte) *CompositeKeyTableUint64 {
	tableKeyRange := &monstera.KeyRange{
		Lower: monstera.ConcatBytes(tableId, keyLowerBound),
		Upper: monstera.ConcatBytes(tableId, keyUpperBound),
	}

	return &CompositeKeyTableUint64{
		keyLowerBound: keyLowerBound,
		keyUpperBound: keyUpperBound,
		tableId:       tableId,
		tableKeyRange: tableKeyRange,
	}
}

func (t *CompositeKeyTableUint64) Get(txn *monstera.Txn, pk []byte, sk []byte) (uint64, error) {
	key := monstera.ConcatBytes(t.tableId, pk, sk)
	if !isWithingRange(key, t.tableKeyRange.Lower, t.tableKeyRange.Upper) {
		panic("key is out of range!")
	}

	value, err := txn.Get(key)
	if err != nil {
		return 0, err
	}
	return bytesToUint64(value), nil
}

func (t *CompositeKeyTableUint64) Set(txn *monstera.Txn, pk []byte, sk []byte, value uint64) error {
	key := monstera.ConcatBytes(t.tableId, pk, sk)
	if !isWithingRange(key, t.tableKeyRange.Lower, t.tableKeyRange.Upper) {
		panic("key is out of range!")
	}

	return txn.Set(key, uint64ToBytes(value))
}

func (t *CompositeKeyTableUint64) Delete(txn *monstera.Txn, pk []byte, sk []byte) error {
	key := monstera.ConcatBytes(t.tableId, pk, sk)
	if !isWithingRange(key, t.tableKeyRange.Lower, t.tableKeyRange.Upper) {
		panic("key is out of range!")
	}

	return txn.Delete(key)
}

func (t *CompositeKeyTableUint64) List(txn *monstera.Txn, pk []byte, fn func(value uint64) (bool, error)) error {
	prefix := monstera.ConcatBytes(t.tableId, pk)

	return txn.EachPrefix(prefix, func(key []byte, value []byte) (bool, error) {
		if !isWithingRange(key, t.tableKeyRange.Lower, t.tableKeyRange.Upper) {
			panic("key is out of range!")
		}

		return fn(bytesToUint64(value))
	})
}

func (t *CompositeKeyTableUint64) GetTableKeyRange() monstera.KeyRange {
	return *t.tableKeyRange
}

// OneToManySortedIndex stores multiple items (arbitrary []byte) per single key PK (arbitrary []byte).
type OneToManySortedIndex struct {
	keyLowerBound []byte
	keyUpperBound []byte
	tableId       []byte
	tableKeyRange *monstera.KeyRange
}

func NewOneToManySortedIndex(tableId []byte, keyLowerBound []byte, keyUpperBound []byte) *OneToManySortedIndex {
	tableKeyRange := &monstera.KeyRange{
		Lower: monstera.ConcatBytes(tableId, keyLowerBound),
		Upper: monstera.ConcatBytes(tableId, keyUpperBound),
	}

	return &OneToManySortedIndex{
		keyLowerBound: keyLowerBound,
		keyUpperBound: keyUpperBound,
		tableId:       tableId,
		tableKeyRange: tableKeyRange,
	}
}

func (i *OneToManySortedIndex) ListAll(txn *monstera.Txn, pk []byte, fn func(item []byte) (bool, error)) error {
	return ListKeys(txn, i.tableId, pk, func(key []byte) (bool, error) {
		if !isWithingRange(key, i.tableKeyRange.Lower, i.tableKeyRange.Upper) {
			panic("key is out of range!")
		}

		return fn(key[len(pk):])
	})
}

func (i *OneToManySortedIndex) ListInRange(txn *monstera.Txn, pk []byte, lowerBound []byte, upperBound []byte, fn func(item []byte) (bool, error)) error {
	return ListRange(txn, i.tableId, pk, lowerBound, upperBound, func(key []byte, value []byte) (bool, error) {
		if !isWithingRange(key, i.tableKeyRange.Lower, i.tableKeyRange.Upper) {
			panic("key is out of range!")
		}

		return fn(key[len(pk):])
	})
}

func (i *OneToManySortedIndex) Add(txn *monstera.Txn, pk []byte, item []byte) error {
	key := monstera.ConcatBytes(i.tableId, pk, item)
	if !isWithingRange(key, i.tableKeyRange.Lower, i.tableKeyRange.Upper) {
		panic("key is out of range!")
	}

	return txn.Set(key, nil)
}

func (i *OneToManySortedIndex) Delete(txn *monstera.Txn, pk []byte, item []byte) error {
	key := monstera.ConcatBytes(i.tableId, pk, item)
	if !isWithingRange(key, i.tableKeyRange.Lower, i.tableKeyRange.Upper) {
		panic("key is out of range!")
	}

	return txn.Delete(key)
}

func (i *OneToManySortedIndex) GetTableKeyRange() monstera.KeyRange {
	return *i.tableKeyRange
}

// SortedIndex stores multiple sorted items (arbitrary []byte) without any PK (global index)
type SortedIndex struct {
	keyLowerBound []byte
	keyUpperBound []byte
	tableId       []byte
	tableKeyRange *monstera.KeyRange
}

func NewSortedIndex(tableId []byte, keyLowerBound []byte, keyUpperBound []byte) *SortedIndex {
	tableKeyRange := &monstera.KeyRange{
		Lower: monstera.ConcatBytes(tableId, keyLowerBound),
		Upper: monstera.ConcatBytes(tableId, keyUpperBound),
	}

	return &SortedIndex{
		keyLowerBound: keyLowerBound,
		keyUpperBound: keyUpperBound,
		tableId:       tableId,
		tableKeyRange: tableKeyRange,
	}
}

func (i *SortedIndex) ListInRange(txn *monstera.Txn, lowerBound []byte, upperBound []byte, fn func(item []byte) (bool, error)) error {
	return ListRange(txn, i.tableId, []byte{}, lowerBound, upperBound, func(key []byte, value []byte) (bool, error) {
		if !isWithingRange(key, i.tableKeyRange.Lower, i.tableKeyRange.Upper) {
			panic("key is out of range!")
		}

		return fn(key)
	})
}

func (i *SortedIndex) Add(txn *monstera.Txn, item []byte) error {
	key := monstera.ConcatBytes(i.tableId, item)
	if !isWithingRange(key, i.tableKeyRange.Lower, i.tableKeyRange.Upper) {
		panic("key is out of range!")
	}

	return txn.Set(key, nil)
}

func (i *SortedIndex) Delete(txn *monstera.Txn, item []byte) error {
	key := monstera.ConcatBytes(i.tableId, item)
	if !isWithingRange(key, i.tableKeyRange.Lower, i.tableKeyRange.Upper) {
		panic("key is out of range!")
	}

	return txn.Delete(key)
}

func (i *SortedIndex) GetTableKeyRange() monstera.KeyRange {
	return *i.tableKeyRange
}

func isWithingRange(key []byte, lowerBound []byte, upperBound []byte) bool {
	return bytes.Compare(key[:len(upperBound)], upperBound) <= 0 &&
		bytes.Compare(key[:len(lowerBound)], lowerBound) >= 0
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
