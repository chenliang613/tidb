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

package kv

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/util/codec"
)

var (
	_ Index         = (*kvIndex)(nil)
	_ IndexIterator = (*IndexIter)(nil)
)

func encodeHandle(h int64) []byte {
	buf := &bytes.Buffer{}
	err := binary.Write(buf, binary.BigEndian, h)
	if err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func decodeHandle(data []byte) (int64, error) {
	var h int64
	buf := bytes.NewBuffer(data)
	err := binary.Read(buf, binary.BigEndian, &h)
	return h, err
}

// IndexIter is for KV store index iterator.
type IndexIter struct {
	it     Iterator
	idx    *kvIndex
	prefix string
}

// Close does the clean up works when KV store index iterator is closed.
func (c *IndexIter) Close() {
	if c.it != nil {
		c.it.Close()
		c.it = nil
	}
}

// Next returns current key and moves iterator to the next step.
func (c *IndexIter) Next() (k []interface{}, h int64, err error) {
	if !c.it.Valid() {
		return nil, 0, io.EOF
	}
	if !strings.HasPrefix(c.it.Key(), c.prefix) {
		return nil, 0, io.EOF
	}
	// get indexedValues
	buf := []byte(c.it.Key())[len(c.prefix):]
	vv, err := DecodeValue(buf)
	if err != nil {
		return nil, 0, err
	}
	// if index is *not* unique, the handle is in keybuf
	if !c.idx.unique {
		h = vv[len(vv)-1].(int64)
		k = vv[0 : len(vv)-1]
	} else {
		// otherwise handle is value
		h, err = decodeHandle(c.it.Value())
		if err != nil {
			return nil, 0, err
		}
		k = vv
	}
	// update new iter to next
	newIt, err := c.it.Next(hasPrefix([]byte(c.prefix)))
	if err != nil {
		return nil, 0, err
	}
	c.it = newIt
	return
}

// kvIndex is the data structure for index data in the KV store.
type kvIndex struct {
	indexName string
	unique    bool
	prefix    string
}

func genIndexPrefix(indexPrefix, indexName string) string {
	// Use EncodeBytes to guarantee generating different index prefix.
	// e.g, two indices c1 and c with index prefix p, if no EncodeBytes,
	// the index format looks p_c and p_c1, if c has an index value which the first encoded byte is '1',
	// we will meet an error, because p_c1 is for index c1.
	// If EncodeBytes, c1 -> c1\x00\x01 and c -> c\x00\x01, the prefixs are different.
	key := fmt.Sprintf("%s_%s", indexPrefix, indexName)
	return string(codec.EncodeBytes(nil, []byte(key)))
}

// NewKVIndex builds a new kvIndex object.
func NewKVIndex(indexPrefix, indexName string, unique bool) Index {
	return &kvIndex{
		indexName: indexName,
		unique:    unique,
		prefix:    genIndexPrefix(indexPrefix, indexName),
	}
}

func (c *kvIndex) genIndexKey(indexedValues []interface{}, h int64) ([]byte, error) {
	var (
		encVal []byte
		err    error
	)
	// only support single value index
	if !c.unique {
		encVal, err = EncodeValue(append(indexedValues, h)...)
	} else {
		/*
			See: https://dev.mysql.com/doc/refman/5.7/en/create-index.html
			A UNIQUE index creates a constraint such that all values in the index must be distinct.
			An error occurs if you try to add a new row with a key value that matches an existing row.
			For all engines, a UNIQUE index permits multiple NULL values for columns that can contain NULL.
		*/
		containsNull := false
		for _, cv := range indexedValues {
			if cv == nil {
				containsNull = true
			}
		}
		if containsNull {
			encVal, err = EncodeValue(append(indexedValues, h)...)
		} else {
			encVal, err = EncodeValue(indexedValues...)
		}
	}
	if err != nil {
		return nil, err
	}
	buf := append([]byte(nil), []byte(c.prefix)...)
	buf = append(buf, encVal...)
	return buf, nil
}

// Create creates a new entry in the kvIndex data.
// If the index is unique and there already exists an entry with the same key, Create will return ErrKeyExists
func (c *kvIndex) Create(txn Transaction, indexedValues []interface{}, h int64) error {
	keyBuf, err := c.genIndexKey(indexedValues, h)
	if err != nil {
		return err
	}
	if !c.unique {
		// TODO: reconsider value
		err = txn.Set(keyBuf, []byte("timestamp?"))
		return errors.Trace(err)
	}

	// unique index
	_, err = txn.Get(keyBuf)
	if IsErrNotFound(err) {
		err = txn.Set(keyBuf, encodeHandle(h))
		return errors.Trace(err)
	}

	return errors.Trace(ErrKeyExists)
}

// Delete removes the entry for handle h and indexdValues from KV index.
func (c *kvIndex) Delete(txn Transaction, indexedValues []interface{}, h int64) error {
	keyBuf, err := c.genIndexKey(indexedValues, h)
	if err != nil {
		return err
	}
	err = txn.Delete(keyBuf)
	return err
}

func hasPrefix(prefix []byte) FnKeyCmp {
	return func(k []byte) bool {
		return bytes.HasPrefix(k, prefix)
	}
}

// Drop removes the KV index from store.
func (c *kvIndex) Drop(txn Transaction) error {
	prefix := []byte(c.prefix)
	it, err := txn.Seek(prefix, hasPrefix(prefix))
	if err != nil {
		return err
	}
	defer it.Close()
	// remove all indices
	for it.Valid() {
		if !strings.HasPrefix(it.Key(), c.prefix) {
			break
		}
		err := txn.Delete([]byte(it.Key()))
		if err != nil {
			return err
		}
		it, err = it.Next(hasPrefix(prefix))
		if err != nil {
			return err
		}
	}
	return nil
}

// Seek searches KV index for the entry with indexedValues.
func (c *kvIndex) Seek(txn Transaction, indexedValues []interface{}) (iter IndexIterator, hit bool, err error) {
	keyBuf, err := c.genIndexKey(indexedValues, 0)
	if err != nil {
		return nil, false, err
	}
	it, err := txn.Seek(keyBuf, hasPrefix([]byte(c.prefix)))
	if err != nil {
		return nil, false, err
	}
	// check if hit
	hit = false
	if it.Valid() && it.Key() == string(keyBuf) {
		hit = true
	}
	return &IndexIter{it: it, idx: c, prefix: c.prefix}, hit, nil
}

// SeekFirst returns an iterator which points to the first entry of the KV index.
func (c *kvIndex) SeekFirst(txn Transaction) (iter IndexIterator, err error) {
	prefix := []byte(c.prefix)
	it, err := txn.Seek([]byte(c.prefix), hasPrefix(prefix))
	if err != nil {
		return nil, err
	}
	return &IndexIter{it: it, idx: c, prefix: c.prefix}, nil
}
