package kv

import (
	"container/list"
	"sync"

	bolt "go.etcd.io/bbolt"
)

/* ──────────────── public commands ──────────────── */
type SetCmd struct{ Key, Value string }
type DelCmd struct{ Key string }
type GetCmd struct{ Key string }

/* ──────────────── Store struct ─────────────────── */
type Store struct {
	mu    sync.RWMutex
	db    *bolt.DB
	cache *list.List
	items map[string]*list.Element
	max   int
}

type item struct {
	key   string
	value string
}

// New creates a new Store with the given database and cache size
func New(db *bolt.DB, maxCache int) *Store {
	s := &Store{
		db:    db,
		cache: list.New(),
		items: make(map[string]*list.Element),
		max:   maxCache,
	}
	// Initialize the kv bucket
	_ = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("kv"))
		return err
	})
	return s
}

func (s *Store) Close() { _ = s.db.Close() }

/* ─────────────────── API ───────────────────────── */

func (s *Store) Get(key string) string {
	s.mu.RLock()
	if e, ok := s.items[key]; ok {
		s.cache.MoveToFront(e)
		s.mu.RUnlock()
		return e.Value.(*item).value
	}
	s.mu.RUnlock()

	// Not in cache, try DB
	var value string
	_ = s.db.View(func(tx *bolt.Tx) error {
		if v := tx.Bucket([]byte("kv")).Get([]byte(key)); v != nil {
			value = string(v)
		}
		return nil
	})

	// Cache the result if found
	if value != "" {
		s.mu.Lock()
		if e, ok := s.items[key]; ok {
			s.cache.MoveToFront(e)
			e.Value.(*item).value = value
		} else {
			s.addToCache(key, value)
		}
		s.mu.Unlock()
	}

	return value
}

func (s *Store) Set(key, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Update DB first
	err := s.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("kv"))
		if err != nil {
			return err
		}
		return b.Put([]byte(key), []byte(value))
	})
	if err != nil {
		return err
	}

	// Then update cache
	if e, ok := s.items[key]; ok {
		s.cache.MoveToFront(e)
		e.Value.(*item).value = value
	} else {
		s.addToCache(key, value)
	}

	return nil
}

func (s *Store) addToCache(key, value string) {
	if s.cache.Len() >= s.max {
		// Remove oldest item
		if e := s.cache.Back(); e != nil {
			s.cache.Remove(e)
			delete(s.items, e.Value.(*item).key)
		}
	}
	e := s.cache.PushFront(&item{key: key, value: value})
	s.items[key] = e
}

func (s *Store) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Remove from DB
	err := s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("kv")).Delete([]byte(key))
	})
	if err != nil {
		return err
	}

	// Remove from cache
	if e, ok := s.items[key]; ok {
		s.cache.Remove(e)
		delete(s.items, key)
	}

	return nil
}

func (s *Store) Apply(cmd any) any {
	switch c := cmd.(type) {
	case SetCmd:
		_ = s.Set(c.Key, c.Value)
	case DelCmd:
		_ = s.Delete(c.Key)
	case GetCmd:
		return s.Get(c.Key)
	}
	return nil
}
