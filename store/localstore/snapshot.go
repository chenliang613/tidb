// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package localstore

import (
	"math"

	"github.com/ngaut/log"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/localstore/engine"
)

var (
	_ kv.Snapshot = (*dbSnapshot)(nil)
	_ kv.Iterator = (*dbIter)(nil)
)

type dbSnapshot struct {
	engine.Snapshot
}

func (s *dbSnapshot) Get(k kv.Key) ([]byte, error) {
	// engine Snapshot return nil, nil for value not found,
	// so here we will check nil and return kv.ErrNotExist.

	// get newest version, (0, MaxUint64)
	// Key arrangement:
	// Key_MaxVer
	// ...
	// Key_ver
	// Key_ver-1
	// Key_ver-2
	// ...
	// Key_ver-n
	// Key_0
	// NextKey...
	startKey := MvccEncodeVersionKey(k, kv.Version{math.MaxUint64})
	endKey := MvccEncodeVersionKey(k, kv.Version{0})

	// get raw iterator
	it := s.Snapshot.NewIterator(startKey)
	defer it.Release()

	var rawKey []byte
	var v []byte
	if it.Next() && IsValidKey(it.Key()) {
		// If scan exceed this key's all versions
		// it.Key() > endKey.
		if kv.EncodedKey(it.Key()).Cmp(endKey) < 0 {
			// Check newest version of this key.
			// If it's tombstone, just skip it.
			if !IsTombstone(it.Value()) {
				rawKey = it.Key()
				v = it.Value()
			}
		}
	}

	// No such key or v is nil.
	if rawKey == nil || v == nil {
		return nil, kv.ErrNotExist
	}

	return v, nil
}

func (s *dbSnapshot) NewIterator(param interface{}) kv.Iterator {
	k, ok := param.([]byte)
	if !ok {
		log.Errorf("leveldb iterator parameter error, %+v", param)
		return nil
	}
	// start with newest version of this key
	startKey := MvccEncodeVersionKey(k, kv.Version{math.MaxUint64})
	log.Error(startKey)
	it := s.Snapshot.NewIterator(startKey)
	return newDBIter(it)
}

func (s *dbSnapshot) Release() {
	if s.Snapshot != nil {
		s.Snapshot.Release()
		s.Snapshot = nil
	}
}

type dbIter struct {
	engine.Iterator
	valid bool
}

func newDBIter(it engine.Iterator) *dbIter {
	return &dbIter{
		Iterator: it,
		valid:    it.Next(),
	}
}

func (it *dbIter) Next(fn kv.FnKeyCmp) (kv.Iterator, error) {
	it.valid = it.Iterator.Next()
	return it, nil
}

func (it *dbIter) Valid() bool {
	return it.valid
}

func (it *dbIter) Key() string {
	return string(it.Iterator.Key())
}

func (it *dbIter) Value() []byte {
	return it.Iterator.Value()
}

func (it *dbIter) Close() {
	if it.Iterator != nil {
		it.Iterator.Release()
		it.Iterator = nil
	}
}
