package redis

import (
	"github.com/stretchr/testify/assert"
	"os"
	"sohucs/log"
	"sohucs/utils"
	"strconv"
	"testing"
	"time"
)

var (
	redispool *RedisPool
)

func setup() {
	redispool = NewRedisPoolWithAuth("10.16.54.6:19000", 5, 3, "7450a905d5ddeae0af663f214b380328")
}

func teardown() {

}

func TestMain(m *testing.M) {
	setup()
	ret := m.Run()
	teardown()
	os.Exit(ret)
}

func TestPing(t *testing.T) {
	err := redispool.Ping()
	assert.NoError(t, err)
}

func TestSetAndGet(t *testing.T) {
	prefix := utils.FormatInt64(time.Now().Unix())
	n := 10
	for i := 1; i < n; i++ {
		key := getSetKey(prefix, i)
		err := redispool.Set(key, []byte(key))
		assert.NoError(t, err)
	}

	for i := 1; i < n; i++ {
		key := getSetKey(prefix, i)
		res, err := redispool.Get(key)
		log.Info(string(res))
		assert.NoError(t, err)
		assert.Equal(t, []byte(key), res)
	}
}

func getSetKey(prefix string, no int) string {
	return prefix + "_" + strconv.Itoa(no)
}

func TestGetKey(t *testing.T) {
	for i := 0; i < 1000; i++ {
		startTime := time.Now()
		res, err := redispool.Get("6332760280573325399_l")
		dur := time.Since(startTime)
		log.Info(len(res), dur)
		assert.NoError(t, err)
	}

}
