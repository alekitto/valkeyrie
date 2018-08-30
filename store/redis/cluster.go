package redis

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/abronan/valkeyrie/store"
	"github.com/go-redis/redis"
)

// Redis implements valkeyrie.Store interface with redis backend
type Cluster struct {
	client *redis.ClusterClient
	script *redis.Script
	codec  defaultCodec
}

// Put a value at the specified key
func (r *Cluster) Put(key string, value []byte, options *store.WriteOptions) error {
	expirationAfter := noExpiration
	if options != nil && options.TTL != 0 {
		expirationAfter = options.TTL
	}

	return r.setTTL(normalize(key), &store.KVPair{
		Key:       key,
		Value:     value,
		LastIndex: sequenceNum(),
	}, expirationAfter)
}

func (r *Cluster) setTTL(key string, val *store.KVPair, ttl time.Duration) error {
	valStr, err := r.codec.encode(val)
	if err != nil {
		return err
	}

	return r.client.Set(key, valStr, ttl).Err()
}

// Get a value given its key
func (r *Cluster) Get(key string, opts *store.ReadOptions) (*store.KVPair, error) {
	return r.get(normalize(key))
}

func (r *Cluster) get(key string) (*store.KVPair, error) {
	reply, err := r.client.Get(key).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, store.ErrKeyNotFound
		}
		return nil, err
	}
	val := store.KVPair{}
	if err := r.codec.decode(string(reply), &val); err != nil {
		return nil, err
	}
	return &val, nil
}

// Delete the value at the specified key
func (r *Cluster) Delete(key string) error {
	return r.client.Del(normalize(key)).Err()
}

// Exists verify if a Key exists in the store
func (r *Cluster) Exists(key string, opts *store.ReadOptions) (bool, error) {
	cmd := r.client.Exists(normalize(key))
	return cmd.Val() != 0, cmd.Err()
}

func newClusterSubscribe(client *redis.ClusterClient, regex string) (*subscribe, error) {
	ch := client.PSubscribe(regex)
	return &subscribe{
		pubsub:  ch,
		closeCh: make(chan struct{}),
	}, nil
}

// Watch for changes on a key
// glitch: we use notified-then-retrieve to retrieve *store.KVPair.
// so the responses may sometimes inaccurate
func (r *Cluster) Watch(key string, stopCh <-chan struct{}, opts *store.ReadOptions) (<-chan *store.KVPair, error) {
	watchCh := make(chan *store.KVPair)
	nKey := normalize(key)

	get := getter(func() (interface{}, error) {
		pair, err := r.get(nKey)
		if err != nil {
			return nil, err
		}
		return pair, nil
	})

	push := pusher(func(v interface{}) {
		if val, ok := v.(*store.KVPair); ok {
			watchCh <- val
		}
	})

	sub, err := newClusterSubscribe(r.client, regexWatch(nKey, false))
	if err != nil {
		return nil, err
	}

	go func(sub *subscribe, stopCh <-chan struct{}, get getter, push pusher) {
		defer sub.Close()

		msgCh := sub.Receive(stopCh)
		if err := watchLoop(msgCh, stopCh, get, push); err != nil {
			log.Printf("watchLoop in Watch err:%v\n", err)
		}
	}(sub, stopCh, get, push)

	return watchCh, nil
}

// WatchTree watches for changes on child nodes under
// a given directory
func (r *Cluster) WatchTree(directory string, stopCh <-chan struct{}, opts *store.ReadOptions) (<-chan []*store.KVPair, error) {
	watchCh := make(chan []*store.KVPair)
	nKey := normalize(directory)

	get := getter(func() (interface{}, error) {
		pair, err := r.list(nKey)
		if err != nil {
			return nil, err
		}
		return pair, nil
	})

	push := pusher(func(v interface{}) {
		if _, ok := v.([]*store.KVPair); !ok {
			return
		}
		watchCh <- v.([]*store.KVPair)
	})

	sub, err := newClusterSubscribe(r.client, regexWatch(nKey, true))
	if err != nil {
		return nil, err
	}

	go func(sub *subscribe, stopCh <-chan struct{}, get getter, push pusher) {
		defer sub.Close()

		msgCh := sub.Receive(stopCh)
		if err := watchLoop(msgCh, stopCh, get, push); err != nil {
			log.Printf("watchLoop in WatchTree err:%v\n", err)
		}
	}(sub, stopCh, get, push)

	return watchCh, nil
}

// NewLock creates a lock for a given key.
// The returned Locker is not held and must be acquired
// with `.Lock`. The Value is optional.
func (r *Cluster) NewLock(key string, options *store.LockOptions) (store.Locker, error) {
	var (
		value []byte
		ttl   = defaultLockTTL
	)

	if options != nil && options.TTL != 0 {
		ttl = options.TTL
	}
	if options != nil && len(options.Value) != 0 {
		value = options.Value
	}

	return &redisClusterLock{
		redis:    r,
		last:     nil,
		key:      key,
		value:    value,
		ttl:      ttl,
		unlockCh: make(chan struct{}),
	}, nil
}

type redisClusterLock struct {
	redis    *Cluster
	last     *store.KVPair
	unlockCh chan struct{}

	key   string
	value []byte
	ttl   time.Duration
}

func (l *redisClusterLock) Lock(stopCh chan struct{}) (<-chan struct{}, error) {
	lockHeld := make(chan struct{})

	success, err := l.tryLock(lockHeld, stopCh)
	if err != nil {
		return nil, err
	}
	if success {
		return lockHeld, nil
	}

	// wait for changes on the key
	watch, err := l.redis.Watch(l.key, stopCh, nil)
	if err != nil {
		return nil, err
	}

	for {
		select {
		case <-stopCh:
			return nil, ErrAbortTryLock
		case <-watch:
			success, err := l.tryLock(lockHeld, stopCh)
			if err != nil {
				return nil, err
			}
			if success {
				return lockHeld, nil
			}
		}
	}
}

// tryLock return true, nil when it acquired and hold the lock
// and return false, nil when it can't lock now,
// and return false, err if any unespected error happened underlying
func (l *redisClusterLock) tryLock(lockHeld, stopChan chan struct{}) (bool, error) {
	success, new, err := l.redis.AtomicPut(
		l.key,
		l.value,
		l.last,
		&store.WriteOptions{
			TTL: l.ttl,
		})
	if success {
		l.last = new
		// keep holding
		go l.holdLock(lockHeld, stopChan)
		return true, nil
	}
	if err != nil && (err == store.ErrKeyNotFound || err == store.ErrKeyModified || err == store.ErrKeyExists) {
		return false, nil
	}
	return false, err
}

func (l *redisClusterLock) holdLock(lockHeld, stopChan chan struct{}) {
	defer close(lockHeld)

	hold := func() error {
		_, new, err := l.redis.AtomicPut(
			l.key,
			l.value,
			l.last,
			&store.WriteOptions{
				TTL: l.ttl,
			})
		if err == nil {
			l.last = new
		}
		return err
	}

	heartbeat := time.NewTicker(l.ttl / 3)
	defer heartbeat.Stop()

	for {
		select {
		case <-heartbeat.C:
			if err := hold(); err != nil {
				return
			}
		case <-l.unlockCh:
			return
		case <-stopChan:
			return
		}
	}
}

func (l *redisClusterLock) Unlock() error {
	l.unlockCh <- struct{}{}

	_, err := l.redis.AtomicDelete(l.key, l.last)
	if err != nil {
		return err
	}
	l.last = nil

	return err
}

// List the content of a given prefix
func (r *Cluster) List(directory string, opts *store.ReadOptions) ([]*store.KVPair, error) {
	return r.list(normalize(directory))
}

func (r *Cluster) list(directory string) ([]*store.KVPair, error) {
	var allKeys []string
	regex := scanRegex(directory) // for all keyed with $directory
	allKeys, err := r.keys(regex)
	if err != nil {
		return nil, err
	}
	// TODO: need to handle when #key is too large
	return r.mget(directory, allKeys...)
}

func (r *Cluster) keys(regex string) ([]string, error) {
	const (
		startCursor  = 0
		endCursor    = 0
		defaultCount = 10
	)

	var allKeys []string
	mux := &sync.Mutex{}

	err := r.client.ForEachMaster(func(client *redis.Client) error {
		var nodeKeys []string
		keys, nextCursor, err := r.client.Scan(startCursor, regex, defaultCount).Result()
		if err != nil {
			return err
		}

		nodeKeys = append(nodeKeys, keys...)
		for nextCursor != endCursor {
			keys, nextCursor, err = r.client.Scan(nextCursor, regex, defaultCount).Result()
			if err != nil {
				return err
			}

			nodeKeys = append(nodeKeys, keys...)
		}

		mux.Lock()
		allKeys = append(allKeys, nodeKeys...)
		mux.Unlock()

		return nil
	})

	if nil != err {
		return nil, err
	}

	if len(allKeys) == 0 {
		return nil, store.ErrKeyNotFound
	}

	fmt.Printf("%+v", allKeys)
	return allKeys, nil
}

// mget values given their keys
func (r *Cluster) mget(directory string, keys ...string) ([]*store.KVPair, error) {
	pipe := r.client.Pipeline()

	for _, key := range keys {
		pipe.Get(key)
	}

	cmds, err := pipe.Exec()
	if err != nil {
		return nil, err
	}

	pairs := []*store.KVPair{}
	for i := range keys {
		value := cmds[i].(*redis.StringCmd).Val()
		newkv := &store.KVPair{}

		if err := r.codec.decode(value, newkv); err != nil {
			return nil, err
		}

		if normalize(newkv.Key) != directory {
			pairs = append(pairs, newkv)
		}
	}

	return pairs, nil
}

// DeleteTree deletes a range of keys under a given directory
// glitch: we list all available keys first and then delete them all
// it costs two operations on redis, so is not atomicity.
func (r *Cluster) DeleteTree(directory string) error {
	var allKeys []string
	regex := scanRegex(normalize(directory)) // for all keyed with $directory
	allKeys, err := r.keys(regex)
	if err != nil {
		return err
	}
	return r.client.Del(allKeys...).Err()
}

// AtomicPut is an atomic CAS operation on a single value.
// Pass previous = nil to create a new key.
// we introduced script on this page, so atomicity is guaranteed
func (r *Cluster) AtomicPut(key string, value []byte, previous *store.KVPair, options *store.WriteOptions) (bool, *store.KVPair, error) {
	expirationAfter := noExpiration
	if options != nil && options.TTL != 0 {
		expirationAfter = options.TTL
	}

	newKV := &store.KVPair{
		Key:       key,
		Value:     value,
		LastIndex: sequenceNum(),
	}
	nKey := normalize(key)

	// if previous == nil, set directly
	if previous == nil {
		if err := r.setNX(nKey, newKV, expirationAfter); err != nil {
			return false, nil, err
		}
		return true, newKV, nil
	}

	if err := r.cas(
		nKey,
		previous,
		newKV,
		formatSec(expirationAfter),
	); err != nil {
		return false, nil, err
	}
	return true, newKV, nil
}

func (r *Cluster) setNX(key string, val *store.KVPair, expirationAfter time.Duration) error {
	valBlob, err := r.codec.encode(val)
	if err != nil {
		return err
	}

	if !r.client.SetNX(key, valBlob, expirationAfter).Val() {
		return store.ErrKeyExists
	}
	return nil
}

func (r *Cluster) cas(key string, old, new *store.KVPair, secInStr string) error {
	newVal, err := r.codec.encode(new)
	if err != nil {
		return err
	}

	oldVal, err := r.codec.encode(old)
	if err != nil {
		return err
	}

	return r.runScript(
		cmdCAS,
		key,
		oldVal,
		newVal,
		secInStr,
	)
}

// AtomicDelete is an atomic delete operation on a single value
// the value will be deleted if previous matched the one stored in db
func (r *Cluster) AtomicDelete(key string, previous *store.KVPair) (bool, error) {
	if err := r.cad(normalize(key), previous); err != nil {
		return false, err
	}
	return true, nil
}

func (r *Cluster) cad(key string, old *store.KVPair) error {
	oldVal, err := r.codec.encode(old)
	if err != nil {
		return err
	}

	return r.runScript(
		cmdCAD,
		key,
		oldVal,
	)
}

// Close the store connection
func (r *Cluster) Close() {
	r.client.Close()
}

func (r *Cluster) runScript(args ...interface{}) error {
	err := r.script.Run(
		r.client,
		nil,
		args...,
	).Err()
	if err != nil && strings.Contains(err.Error(), "redis: key is not found") {
		return store.ErrKeyNotFound
	}
	if err != nil && strings.Contains(err.Error(), "redis: value has been changed") {
		return store.ErrKeyModified
	}
	return err
}
