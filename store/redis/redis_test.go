package redis

import (
	"github.com/go-redis/redis"
	"testing"

	"github.com/abronan/valkeyrie"
	"github.com/abronan/valkeyrie/store"
	"github.com/abronan/valkeyrie/testutils"
	"github.com/stretchr/testify/assert"
)

var (
	client         = "localhost:6379"
	clusterClients = []string{"localhost:7000", "localhost:7001", "localhost:7002", "localhost:7003", "localhost:7004", "localhost:7005"}
)

func makeRedisClient(t *testing.T) store.Store {
	kv, err := newRedis([]string{client}, "")
	if err != nil {
		t.Fatalf("cannot create store: %v", err)
	}

	// NOTE: please turn on redis's notification
	// before you using watch/watchtree/lock related features
	kv.client.ConfigSet("notify-keyspace-events", "KA")

	return kv
}

func makeRedisClusterClient(t *testing.T) store.Store {
	kv, err := newRedisCluster(clusterClients, "")
	if err != nil {
		t.Fatalf("cannot create store: %v", err)
	}

	// NOTE: please turn on redis's notification
	// before you using watch/watchtree/lock related features
	kv.client.ForEachMaster(func(client *redis.Client) error {
		cmd := client.ConfigSet("notify-keyspace-events", "KA")

		return cmd.Err()
	})

	return kv
}

func TestRegister(t *testing.T) {
	Register()

	kv, err := valkeyrie.NewStore(store.REDIS, []string{client}, nil)
	assert.NoError(t, err)
	assert.NotNil(t, kv)

	if _, ok := kv.(*Redis); !ok {
		t.Fatal("Error registering and initializing redis")
	}

	kv, err = valkeyrie.NewStore(store.REDIS_CLUSTER, clusterClients, nil)
	assert.NoError(t, err)
	assert.NotNil(t, kv)

	if _, ok := kv.(*Cluster); !ok {
		t.Fatal("Error registering and initializing redis cluster")
	}
}

func TestRedisStore(t *testing.T) {
	kv := makeRedisClient(t)
	lockTTL := makeRedisClient(t)
	kvTTL := makeRedisClient(t)

	testutils.RunTestCommon(t, kv)
	testutils.RunTestAtomic(t, kv)
	testutils.RunTestWatch(t, kv)
	testutils.RunTestLock(t, kv)
	testutils.RunTestLockTTL(t, kv, lockTTL)
	testutils.RunTestTTL(t, kv, kvTTL)
	testutils.RunCleanup(t, kv)
}

func TestRedisClusterStore(t *testing.T) {
	kv := makeRedisClusterClient(t)
	lockTTL := makeRedisClusterClient(t)
	kvTTL := makeRedisClusterClient(t)

	testutils.RunTestCommon(t, kv)
	testutils.RunTestAtomic(t, kv)
	testutils.RunTestWatch(t, kv)
	testutils.RunTestLock(t, kv)
	testutils.RunTestLockTTL(t, kv, lockTTL)
	testutils.RunTestTTL(t, kv, kvTTL)
	testutils.RunCleanup(t, kv)
}
