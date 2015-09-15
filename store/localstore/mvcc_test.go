package localstore

import (
	"bytes"

	"github.com/ngaut/log"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/localstore/goleveldb"
)

var _ = Suite(&testMvccSuite{})

type testMvccSuite struct {
	s kv.Storage
}

func (s *testMvccSuite) SetUpSuite(c *C) {
	path := "memory:"
	d := Driver{
		goleveldb.MemoryDriver{},
	}
	store, err := d.Open(path)
	c.Assert(err, IsNil)
	s.s = store

	// must in cache
	cacheS, _ := d.Open(path)
	c.Assert(cacheS, Equals, store)
}

func (s *testMvccSuite) TearDownSuite(c *C) {
	err := s.s.Close()
	c.Assert(err, IsNil)
}

func (t *testMvccSuite) TestMvccEncode(c *C) {
	encodedKey1 := MvccEncodeVersionKey([]byte("A"), kv.Version{1})
	encodedKey2 := MvccEncodeVersionKey([]byte("A"), kv.Version{2})
	// A_2
	// A_1
	c.Assert(encodedKey1.Cmp(encodedKey2), Greater, 0)

	// decode test
	key, ver, err := MvccDecode(encodedKey1)
	c.Assert(err, IsNil)
	c.Assert(bytes.Compare(key, []byte("A")), Equals, 0)
	c.Assert(ver.Ver, Equals, uint64(1))
}

func (t *testMvccSuite) scanRawEngine(c *C, f func([]byte, []byte)) {
	// scan raw db
	s, err := t.s.(*dbStore).db.GetSnapshot()
	c.Assert(err, IsNil)
	it := s.NewIterator(nil)
	for it.Next() {
		f(it.Key(), it.Value())
	}
}

func (t *testMvccSuite) TestMvccPutAndDel(c *C) {
	txn, err := t.s.Begin()
	c.Assert(err, IsNil)
	for i := 0; i < 5; i++ {
		val := encodeInt(i)
		err := txn.Set(val, val)
		c.Assert(err, IsNil)
	}
	txn.Commit()

	t.scanRawEngine(c, func(k, v []byte) {
		log.Info(k, v)
	})

	txn, err = t.s.Begin()
	c.Assert(err, IsNil)
	for i := 0; i < 3; i++ {
		val := encodeInt(i)
		err := txn.Delete(val)
		c.Assert(err, IsNil)
	}
	txn.Commit()

	t.scanRawEngine(c, func(k, v []byte) {
		log.Info(k, v)
	})

	txn, _ = t.s.Begin()
	_, err = txn.Get(encodeInt(0))
	c.Assert(err, NotNil)
	v, err := txn.Get(encodeInt(4))
	c.Assert(err, IsNil)
	c.Assert(len(v), Greater, 0)
	txn.Commit()

	txn, _ = t.s.Begin()
	txn.Set(encodeInt(0), []byte("v"))
	v, err = txn.Get(encodeInt(0))
	txn.Commit()

	t.scanRawEngine(c, func(k, v []byte) {
		log.Info(k, v)
	})
}

func (t *testMvccSuite) TestMvccNext(c *C) {
	txn, err := t.s.Begin()
	c.Assert(err, IsNil)
	for i := 0; i < 5; i++ {
		val := encodeInt(i)
		err := txn.Set(val, val)
		c.Assert(err, IsNil)
	}
	txn.Commit()

	txn, _ = t.s.Begin()
	it, err := txn.Seek(encodeInt(2), nil)
	c.Assert(err, IsNil)
	c.Assert(it.Valid(), IsTrue)
	for it.Valid() {
		log.Error("seeking", []byte(it.Key()))
		it, err = it.Next(nil)
		c.Assert(err, IsNil)
	}
	txn.Commit()
}
