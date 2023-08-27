/*
 * Copyright (c) 2023 Zander Schwid & Co. LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package pebblestore

import (
	"bytes"
	"context"
	"encoding/binary"
	"github.com/cockroachdb/pebble"
	"github.com/keyvalstore/store"
	"github.com/codeallergy/value"
	"io"
	"reflect"
	"time"
)

var PebbleStoreClass = reflect.TypeOf((*implPebbleStore)(nil))

type implPebbleStore struct {
	name  string
	db     *pebble.DB
}

func New(name string, dataDir string, opts *pebble.Options) (*implPebbleStore, error) {

	db, err := OpenDatabase(dataDir, opts)
	if err != nil {
		return nil, err
	}

	return &implPebbleStore{name: name, db: db}, nil
}

func FromDB(name string, db *pebble.DB) *implPebbleStore {
	return &implPebbleStore{name: name, db: db}
}

func (t*implPebbleStore) Interface() store.ManagedDataStore {
	return t
}

func (t*implPebbleStore) BeanName() string {
	return t.name
}

func (t*implPebbleStore) Destroy() error {
	return t.db.Close()
}

func (t*implPebbleStore) Get(ctx context.Context) *store.GetOperation {
	return &store.GetOperation{DataStore: t, Context: ctx}
}

func (t*implPebbleStore) Set(ctx context.Context) *store.SetOperation {
	return &store.SetOperation{DataStore: t, Context: ctx}
}

func (t*implPebbleStore) CompareAndSet(ctx context.Context) *store.CompareAndSetOperation {
	return &store.CompareAndSetOperation{DataStore: t, Context: ctx}
}

func (t *implPebbleStore) Increment(ctx context.Context) *store.IncrementOperation {
	return &store.IncrementOperation{DataStore: t, Context: ctx, Initial: 0, Delta: 1}
}

func (t *implPebbleStore) Touch(ctx context.Context) *store.TouchOperation {
	return &store.TouchOperation{DataStore: t, Context: ctx}
}

func (t*implPebbleStore) Remove(ctx context.Context) *store.RemoveOperation {
	return &store.RemoveOperation{DataStore: t, Context: ctx}
}

func (t*implPebbleStore) Enumerate(ctx context.Context) *store.EnumerateOperation {
	return &store.EnumerateOperation{DataStore: t, Context: ctx}
}

func (t*implPebbleStore) GetRaw(ctx context.Context, key []byte, ttlPtr *int, versionPtr *int64, required bool) ([]byte, error) {
	return t.getImpl(key, required)
}

func (t*implPebbleStore) SetRaw(ctx context.Context, key, value []byte, ttlSeconds int) error {
	return t.db.Set(key, value, WriteOptions)
}

func (t *implPebbleStore) IncrementRaw(ctx context.Context, key []byte, initial, delta int64, ttlSeconds int) (prev int64, err error) {
	err = t.UpdateRaw(ctx, key, func(entry *store.RawEntry) bool {
		counter := initial
		if len(entry.Value) >= 8 {
			counter = int64(binary.BigEndian.Uint64(entry.Value))
		}
		prev = counter
		counter += delta
		entry.Value = make([]byte, 8)
		binary.BigEndian.PutUint64(entry.Value, uint64(counter))
		entry.Ttl = ttlSeconds
		return true
	})
	return
}

func (t *implPebbleStore) UpdateRaw(ctx context.Context, key []byte, cb func(entry *store.RawEntry) bool) error {

	rawEntry := &store.RawEntry {
		Key: key,
		Ttl: store.NoTTL,
		Version: 0,
	}

	value, closer, err := t.db.Get(key)
	if err != nil {
		if err != pebble.ErrNotFound {
			return err
		}
	}
	defer closer.Close()

	rawEntry.Value = value

	if !cb(rawEntry) {
		return ErrOperationCanceled
	}

	return t.db.Set(key, rawEntry.Value, WriteOptions)
}

func (t*implPebbleStore) CompareAndSetRaw(ctx context.Context, key, value []byte, ttlSeconds int, version int64) (bool, error) {
	return true, t.SetRaw(ctx, key, value, ttlSeconds)
}

func (t *implPebbleStore) TouchRaw(ctx context.Context, fullKey []byte, ttlSeconds int) error {
	return nil
}

func (t*implPebbleStore) RemoveRaw(ctx context.Context, key []byte) error {
	return t.db.Delete(key, WriteOptions)
}

func (t*implPebbleStore) getImpl(key []byte, required bool) ([]byte, error) {

	value, closer, err := t.db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			if required {
				return nil, store.ErrNotFound
			}
		}
		return nil, err
	}

	dst := make([]byte, len(value))
	copy(dst, value)
	return dst, closer.Close()
}

func (t*implPebbleStore) EnumerateRaw(ctx context.Context, prefix, seek []byte, batchSize int, onlyKeys bool, reverse bool, cb func(entry *store.RawEntry) bool) error {
	if reverse {
		var cache []*store.RawEntry
		err := t.doEnumerateRaw(ctx, prefix, seek, batchSize, onlyKeys, func(entry *store.RawEntry) bool {
			cache = append(cache, entry)
			return true
		})
		if err != nil {
			return err
		}
		n := len(cache)
		for j := n-1; j >= 0; j-- {
			if !cb(cache[j]) {
				break
			}
		}
		return nil
	} else {
		return t.doEnumerateRaw(ctx, prefix, seek, batchSize, onlyKeys, cb)
	}
}

func (t*implPebbleStore) doEnumerateRaw(ctx context.Context, prefix, seek []byte, batchSize int, onlyKeys bool, cb func(entry *store.RawEntry) bool) (err error) {

	iter := t.db.NewIter(&pebble.IterOptions{
		LowerBound:  seek,
	})

	for iter.Valid() {

		err = ctx.Err()
		if err != nil {
			return err
		}

		if !bytes.HasPrefix(iter.Key(), prefix) {
			break
		}

		re := store.RawEntry{
			Key:     iter.Key(),
			Value:   iter.Value(),
			Ttl:     0,
			Version: 0,
		}

		if !cb(&re) {
			break
		}

		if !iter.Next() {
			break
		}

	}

	return iter.Close()
}

func (t*implPebbleStore) First() ([]byte, error) {
	iter := t.db.NewIter(&pebble.IterOptions{})
	defer iter.Close()
	if !iter.First() {
		return nil, nil
	}
	key := iter.Key()
	dst := make([]byte, len(key))
	copy(dst, key)
	return dst, nil
}

func (t*implPebbleStore) Last() ([]byte, error) {
	iter := t.db.NewIter(&pebble.IterOptions{})
	defer iter.Close()
	if !iter.Last() {
		return nil, nil
	}
	key := iter.Key()
	dst := make([]byte, len(key))
	copy(dst, key)
	return dst, nil
}

func (t*implPebbleStore) Compact(discardRatio float64) error {
	first, err := t.First()
	if err != nil {
		return err
	}
	last, err := t.Last()
	if err != nil {
		return err
	}
	return t.db.Compact(first, last, true)
}

func (t*implPebbleStore) Backup(w io.Writer, since uint64) (uint64, error) {
	snap := t.db.NewSnapshot()
	defer snap.Close()
	iter := snap.NewIter(&pebble.IterOptions{})

	packer := value.MessagePacker(w)
	for iter.Valid() {

		k, v := iter.Key(), iter.Value()
		if k != nil && v != nil {
			packer.PackBin(k)
			packer.PackBin(v)
		}

		if !iter.Next() {
			break
		}
	}

	return uint64(time.Now().Unix()), iter.Close()
}

func (t*implPebbleStore) Restore(r io.Reader) error {

	if err := t.DropAll(); err != nil {
		return err
	}

	unpacker := value.MessageReader(r)
	parser := value.MessageParser()

	readBinary := func() ([]byte, error) {
		fmt, header := unpacker.Next()
		if fmt == value.EOF {
			return nil, io.EOF
		}
		if fmt != value.BinHeader {
			return nil, ErrInvalidFormat
		}
		size := parser.ParseBin(header)
		if parser.Error() != nil {
			return nil, parser.Error()
		}
		return unpacker.Read(size)
	}

	for {

		key, err := readBinary()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		value, err := readBinary()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		err = t.db.Set(key, value, WriteOptions)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t*implPebbleStore) DropAll() error {
	first, err := t.First()
	if err != nil {
		return err
	}
	last, err := t.Last()
	if err != nil {
		return err
	}
	return t.db.DeleteRange(first, append(last, 0xFF), WriteOptions)
}

func (t*implPebbleStore) DropWithPrefix(prefix []byte) error {

	last := append(prefix, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF)

	return t.db.DeleteRange(prefix, last, WriteOptions)
}

func (t*implPebbleStore) Instance() interface{} {
	return t.db
}