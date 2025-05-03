package raft

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"log"
	"sync"

	bolt "go.etcd.io/bbolt"
)

type LogEntry struct {
	Term    int
	Command any
}

type StableLog interface {
	Append(entries ...LogEntry) int // returns last index appended (1‑based)
	At(index int) (LogEntry, bool)  // returns false if index < firstIndex or > lastIndex
	LastIndexTerm() (int, int)
	LastIndex() int
	FirstIndex() int

	TruncateSuffix(idx int) error
	TruncateBefore(index int)
}

// ---------------- util ---------------------
func u64ToKey(i uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], i)
	return b[:]
}
func keyToU64(b []byte) uint64 { return binary.BigEndian.Uint64(b) }

// -------------- BoltLog --------------------
type boltLog struct {
	db        *bolt.DB
	mu        sync.Mutex
	base      uint64 // first index = base
	lastIndex uint64
	logger    *log.Logger
}

func NewBoltLog(db *bolt.DB) StableLog {
	l := &boltLog{
		db:     db,
		logger: log.New(log.Writer(), "[RAFT] ", log.LstdFlags),
	}

	// Initialize the log bucket and find the last index
	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("log"))
		if err != nil {
			return err
		}

		// Find the last index
		c := b.Cursor()
		k, _ := c.Last()
		if k != nil {
			l.lastIndex = binary.BigEndian.Uint64(k)
		}

		return nil
	})
	if err != nil {
		l.logger.Printf("failed to initialize log: %v", err)
	}

	return l
}

// --------------- StableLog interface -----------------------
func (l *boltLog) Append(entries ...LogEntry) int {
	l.mu.Lock()
	defer l.mu.Unlock()

	var last uint64
	err := l.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("log"))
		if b == nil {
			return fmt.Errorf("log bucket not found")
		}

		for _, e := range entries {
			last = uint64(l.LastIndex() + 1)
			var buf bytes.Buffer
			if err := gob.NewEncoder(&buf).Encode(e); err != nil {
				return err
			}
			if err := b.Put(u64ToKey(last), buf.Bytes()); err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		l.logger.Printf("failed to append entries: %v", err)
		return -1
	}

	return int(last)
}

func (l *boltLog) At(idx int) (LogEntry, bool) {
	var e LogEntry
	if idx < int(l.base) {
		return e, false
	}
	_ = l.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket([]byte("log")).Get(u64ToKey(uint64(idx)))
		if v == nil {
			return nil
		}
		_ = gob.NewDecoder(bytes.NewReader(v)).Decode(&e)
		return nil
	})
	return e, e.Term != 0 || e.Command != nil
}

func (l *boltLog) LastIndexTerm() (int, int) {
	var idx, term int
	_ = l.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte("log")).Cursor()
		k, v := c.Last()
		if k == nil {
			idx, term = int(l.base-1), 0
			return nil
		}
		idx = int(keyToU64(k))
		var e LogEntry
		_ = gob.NewDecoder(bytes.NewReader(v)).Decode(&e)
		term = e.Term
		return nil
	})
	return idx, term
}

func (l *boltLog) LastIndex() int {
	i, _ := l.LastIndexTerm()
	return i
}

func (l *boltLog) FirstIndex() int {
	return int(l.base)
}

// ------------ snapshot compaction ---------------
func (l *boltLog) TruncateBefore(index int) {
	l.mu.Lock()
	defer l.mu.Unlock()

	_ = l.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("log"))
		meta := tx.Bucket([]byte("meta"))
		c := b.Cursor()

		// Delete all entries before the cutoff
		for k, _ := c.First(); k != nil && keyToU64(k) < uint64(index); k, _ = c.Next() {
			if err := c.Delete(); err != nil {
				return err
			}
		}

		// Update the base index
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(index))
		if err := meta.Put([]byte("firstIndex"), buf[:]); err != nil {
			return err
		}
		l.base = uint64(index)

		return nil
	})
}

func (l *boltLog) TruncateSuffix(idx int) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("log"))
		c := b.Cursor()
		for k, _ := c.Seek(u64ToKey(uint64(idx))); k != nil; k, _ = c.Next() {
			if err := c.Delete(); err != nil {
				return err
			}
		}
		return nil
	})
}
