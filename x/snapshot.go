package monsterax

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"errors"

	"github.com/evrblk/monstera"
	"google.golang.org/protobuf/proto"
)

type BadgerStoreSnapshot struct {
	ranges []monstera.KeyRange
	txn    *monstera.Txn
}

func (s BadgerStoreSnapshot) Write(w io.Writer) error {
	for _, r := range s.ranges {
		s.txn.EachRange(r.Lower, r.Upper, func(key []byte, value []byte) (bool, error) {
			// Write key size and key itself
			err := WriteUint32IntoStream(w, uint32(len(key)))
			if err != nil {
				return false, err
			}
			_, err = w.Write(key)
			if err != nil {
				return false, err
			}

			// Write value size and value itself
			err = WriteUint32IntoStream(w, uint32(len(value)))
			if err != nil {
				return false, err
			}
			_, err = w.Write(value)
			if err != nil {
				return false, err
			}

			return true, nil
		})

		return nil
	}

	return nil
}

func (s BadgerStoreSnapshot) Release() {
	// close transaction when finish writing snapshot
	s.txn.Discard()
}

func iterateKeyRange(lowerBound, upperBound []byte, iter func(prefix []byte) error) error {
	i := len(lowerBound)
	for ; i > 0; i-- {
		if lowerBound[i-1] != 0x00 || upperBound[i-1] != 0xff {
			break
		}
	}

	current := make([]byte, i)
	copy(current, lowerBound[:i])

	for bytes.Compare(current, upperBound[:i]) <= 0 {
		err := iter(current)
		if err != nil {
			return err
		}

		// Increment the key
		for i := len(current) - 1; i >= 0; i-- {
			current[i]++
			if current[i] != 0 {
				break
			}
			// If we've wrapped around to 0 and this is the first byte,
			// we've exceeded the maximum possible value
			if i == 0 && current[i] == 0 {
				return nil
			}
		}
	}

	return nil
}

func Restore(s *monstera.BadgerStore, ranges []monstera.KeyRange, reader io.ReadCloser) error {
	// clear DB in all ranges before writing a snapshot
	for _, r := range ranges {
		err := iterateKeyRange(r.Lower, r.Upper, func(prefix []byte) error {
			return s.DropPrefix(prefix)
		})

		if err != nil {
			return err
		}
	}

	err := s.BatchUpdate(func(batch *monstera.Batch) error {
		for {
			keySize, err := ReadUint32FromStream(reader)
			if err != nil {
				if err == io.EOF {
					break // EOF reached
				}
				return err
			}
			if keySize == 0 {
				return errors.New("empty key")
			}
			key := make([]byte, keySize)
			_, err = io.ReadFull(reader, key)
			if err != nil {
				return err
			}

			valueSize, err := ReadUint32FromStream(reader)
			if err != nil {
				return err
			}
			value := make([]byte, valueSize)
			if valueSize > 0 {
				_, err = io.ReadFull(reader, value)
				if err != nil {
					return err
				}
			}

			err = batch.Set(key, value)
			if err != nil {
				return err
			}
		}

		return nil
	})

	err = s.Flatten()
	if err != nil {
		return err
	}

	return nil
}

func Snapshot(s *monstera.BadgerStore, ranges []monstera.KeyRange) BadgerStoreSnapshot {
	return BadgerStoreSnapshot{
		txn:    s.View(),
		ranges: ranges,
	}
}

func WriteProtoMessageIntoStream(w io.Writer, msg proto.Message) error {
	// Serialize the message to binary format
	data, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("proto.Marshal: %v", err)
	}

	// Write the size of the serialized data
	if err := WriteUint32IntoStream(w, uint32(len(data))); err != nil {
		return fmt.Errorf("WriteUint32IntoStream: %v", err)
	}

	// Write the actual serialized ProtoBuf message
	if _, err := w.Write(data); err != nil {
		return fmt.Errorf("io.Write: %v", err)
	}

	return nil
}

func ReadProtoMessageFromStream[T ptr[U], U any](r io.Reader) (T, error) {
	// Read the size of the next message
	size, err := ReadUint32FromStream(r)
	if err != nil {
		if err == io.EOF {
			return nil, nil // EOF indicates no more messages
		}
		return nil, fmt.Errorf("ReadUint32FromStream: %v", err)
	}

	// Prepare a buffer to hold the serialized data
	data := make([]byte, size)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, fmt.Errorf("io.ReadFull: %v", err)
	}

	// Unmarshal the data
	var msg U
	if err := proto.Unmarshal(data, T(&msg)); err != nil {
		return nil, fmt.Errorf("proto.Unmarshal: %v", err)
	}

	return &msg, nil
}

func WriteUint32IntoStream(w io.Writer, i uint32) error {
	if err := binary.Write(w, binary.BigEndian, i); err != nil {
		return fmt.Errorf("binary.Write: %v", err)
	}

	return nil
}

func ReadUint32FromStream(r io.Reader) (uint32, error) {
	var i uint32
	if err := binary.Read(r, binary.BigEndian, &i); err != nil {
		if err == io.EOF {
			return 0, io.EOF // EOF indicates no more messages
		}
		return 0, fmt.Errorf("binary.Read: %v", err)
	}

	return i, nil
}
