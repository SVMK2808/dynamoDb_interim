package hintstore

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/boltdb/bolt"
)

type HintEntry struct {
    Key     string
    Item    []byte // serialized pb.ItemProto (or whatever your internal bytes are)
    Origin  string // origin node ID
    TS      int64  // unix nanos
}

type Store struct {
    db *bolt.DB
    bucketName []byte
}

func New(path string) (*Store, error) {
    os.MkdirAll(filepath.Dir(path), 0755)
    db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 1 * time.Second})
    if err != nil { return nil, err }
    s := &Store{db: db, bucketName: []byte("hints")}
    err = s.db.Update(func(tx *bolt.Tx) error {
        _, err := tx.CreateBucketIfNotExists(s.bucketName)
        return err
    })
    if err != nil { db.Close(); return nil, err }
    return s, nil
}

func (s *Store) PutHint(nodeID string, entry *HintEntry) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucketName)
		// store keys using stable hint prefix so other subsystems can easily filter
		k := fmt.Sprintf("hint:%s:%s:%d", nodeID, entry.Key, entry.TS)
		v, _ := json.Marshal(entry)
		return b.Put([]byte(k), v)
	})
}

func (s *Store) GetHintsForNode(nodeID string) ([]*HintEntry, error) {
	var out []*HintEntry
	prefix := []byte(fmt.Sprintf("hint:%s:", nodeID))
	err := s.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(s.bucketName).Cursor()
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			var e HintEntry
			if err := json.Unmarshal(v, &e); err == nil {
				out = append(out, &e)
			}
		}
		return nil
	})
	return out, err
}

// DeleteHint deletes a hint by its full stored key (e.g. "hint:nodeA:user:101:1762...")
func (s *Store) DeleteHint(fullKey string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucketName)
		return b.Delete([]byte(fullKey))
	})
}

// DeleteHintNodeKey deletes a hint given node, logical key and timestamp string
func (s *Store) DeleteHintNodeKey(nodeID, key, ts string) error {
	full := fmt.Sprintf("hint:%s:%s:%s", nodeID, key, ts)
	return s.DeleteHint(full)
}

// IterateHints calls cb(fullKey, rawValue) for each hint whose key has the given prefix.
// If prefix is empty it iterates all hints.
func (s *Store) IterateHints(prefix string, cb func(string, []byte) error) error {
	p := []byte(prefix)
	return s.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(s.bucketName).Cursor()
		for k, v := c.Seek(p); k != nil && (len(p) == 0 || bytes.HasPrefix(k, p)); k, v = c.Next() {
			if err := cb(string(k), v); err != nil {
				return err
			}
		}
		return nil
	})
}

// GC: remove by ts older than cutoff
func (s *Store) GCOlderThan(cutoff int64) (int, error) {
    removed := 0
    err := s.db.Update(func(tx *bolt.Tx) error {
        c := tx.Bucket(s.bucketName).Cursor()
        for k, v := c.First(); k != nil; k, v = c.Next() {
            var e HintEntry
            if err := json.Unmarshal(v, &e); err == nil {
                if e.TS < cutoff {
                    if err := c.Delete(); err == nil { removed++ }
                }
            }
        }
        return nil
    })
    return removed, err
}

func (s *Store) Close() error { return s.db.Close() }