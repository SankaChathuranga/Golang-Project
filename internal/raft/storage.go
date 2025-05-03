package raft

// StableStore holds the Raft persistent metadata.

import (
	"encoding/binary"
	"errors"

	bolt "go.etcd.io/bbolt"
)

var (
	ErrStoreClosed = errors.New("store is closed")
	ErrInvalidData = errors.New("invalid data in store")
)

type StableStore interface {
	Term() (int, error)
	SetTerm(t int) error
	VotedFor() (string, error)
	SetVotedFor(id string) error
	LastApplied() (int, error)
	SetLastApplied(index int) error
	Close() error
}

// ------------------------------------------------------------
// Bolt-backed implementation
// ------------------------------------------------------------
const (
	bMeta       = "meta" // bucket name
	kTerm       = "term"
	kVotedFor   = "voted_for"
	LastApplied = "last_applied"
)

type boltStore struct {
	db *bolt.DB
}

func NewBoltStore(db *bolt.DB) (StableStore, error) {
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bMeta))
		return err
	}); err != nil {
		return nil, err
	}
	return &boltStore{db: db}, nil
}

func (s *boltStore) Term() (int, error) {
	var t uint64
	err := s.db.View(func(tx *bolt.Tx) error {
		if v := tx.Bucket([]byte(bMeta)).Get([]byte(kTerm)); v != nil {
			t = binary.BigEndian.Uint64(v)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return int(t), nil
}

func (s *boltStore) SetTerm(term int) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(term))
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(bMeta)).Put([]byte(kTerm), buf[:])
	})
}

func (s *boltStore) VotedFor() (string, error) {
	var id string
	err := s.db.View(func(tx *bolt.Tx) error {
		if v := tx.Bucket([]byte(bMeta)).Get([]byte(kVotedFor)); v != nil {
			id = string(v)
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return id, nil
}

func (s *boltStore) SetVotedFor(id string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(bMeta)).Put([]byte(kVotedFor), []byte(id))
	})
}

func (s *boltStore) LastApplied() (int, error) {
	var last uint64
	err := s.db.View(func(tx *bolt.Tx) error {
		if v := tx.Bucket([]byte(bMeta)).Get([]byte(LastApplied)); v != nil {
			last = binary.BigEndian.Uint64(v)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return int(last), nil
}

func (s *boltStore) SetLastApplied(index int) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(index))
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(bMeta)).Put([]byte(LastApplied), buf[:])
	})
}

func (s *boltStore) Close() error {
	return s.db.Close()
}
