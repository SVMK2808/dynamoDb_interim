package storage

import (
	"bytes"
	"encoding/json"
	"time"

	bolt "go.etcd.io/bbolt"
)

const bucketName = "items_v1"

type Context struct {
	Vc        map[string]uint64 `json:"vc"`
	Tombstone bool              `json:"tombstone"`
	Origin    string            `json:"origin"`
	Ts        int64             `json:"ts"`
}

type Item struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
	Ctx   Context `json:"ctx"`
}

type Store struct {
	db *bolt.DB
}

func Open(path string) (*Store, error) {
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		return err
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) Get(key string) ([]Item, error) {
	var res []Item
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		v := b.Get([]byte(key))
		if v == nil {
			// no value -> empty slice
			res = []Item{}
			return nil
		}
		return json.Unmarshal(v, &res)
	})
	return res, err
}

func (s *Store) Put(key string, items []Item) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		bs, err := json.Marshal(items)
		if err != nil {
			return err
		}
		return b.Put([]byte(key), bs)
	})
}

// ScanPrefix iterates keys starting with prefix and invokes cb on every stored item
func (s *Store) ScanPrefix(prefix string, cb func(Item) error) error {
	return s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		c := b.Cursor()
		for k, v := c.Seek([]byte(prefix)); k != nil && len(k) >= len(prefix) && string(k[:len(prefix)]) == prefix; k, v = c.Next() {
			var arr []Item
			if err := json.Unmarshal(v, &arr); err != nil {
				return err
			}
			for _, it := range arr {
				if err := cb(it); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// ListKeysForVnodeWithPrefix is a stub implementation used by Merkle helpers.
// This storage layer does not currently shard keys by vnode; we ignore the
// vnode parameter and simply return all keys matching the provided prefix.
// Returned map maps key -> concatenated value bytes (first sibling only).
func (s *Store) ListKeysForVnodeWithPrefix(vnode uint64, prefix []byte) map[string][]byte {
	out := make(map[string][]byte)
	_ = s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return nil
		}
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if len(prefix) > 0 && !bytes.HasPrefix(k, prefix) {
				continue
			}
			var arr []Item
			if err := json.Unmarshal(v, &arr); err != nil || len(arr) == 0 {
				continue
			}
			out[string(k)] = arr[0].Value
		}
		return nil
	})
	return out
}

// IterateAll invokes cb with key and slice of item siblings for every key in the store.
// Stops early if cb returns an error.
func (s *Store) IterateAll(cb func(key string, items []Item) error) error {
	return s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil { return nil }
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var arr []Item
			if err := json.Unmarshal(v, &arr); err != nil {
				// skip malformed entry
				continue
			}
			if err := cb(string(k), arr); err != nil {
				return err
			}
		}
		return nil
	})
}
