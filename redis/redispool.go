package redis

import (
	"errors"
	"github.com/garyburd/redigo/redis"
	"time"
)

var (
	ErrRedisUnknown = errors.New("unknown error happen")
)

type RedisPool struct {
	redisServer string
	pool        *redis.Pool
}

func newPool(server string, maxActive int, maxIdle int, auth string, testOnBorrow bool) *redis.Pool {

	pool := &redis.Pool{
		MaxActive:   maxActive,
		MaxIdle:     maxIdle,
		IdleTimeout: 240 * time.Second,

		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				return nil, err
			}

			if auth != "" {
				if _, err = c.Do("AUTH", auth); err != nil {
					c.Close()
					return nil, err
				}
			}

			return c, err
		},
	}

	if testOnBorrow {
		pool.TestOnBorrow = func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		}
	}

	return pool
}

func NewRedisPoolWithAuth(server string, maxActive int, maxIdle int, auth string) *RedisPool {
	return &RedisPool{
		redisServer: server,
		pool:        newPool(server, maxActive, maxIdle, auth, false),
	}
}

func NewRedisPool(server string, maxActive int, maxIdle int) *RedisPool {
	return NewRedisPoolWithAuth(server, maxActive, maxIdle, "")
}

func (r *RedisPool) Ping() error {
	conn := r.pool.Get()
	defer conn.Close()

	_, err := conn.Do("PING")

	return err
}

func (r *RedisPool) Set(key string, value []byte) error {
	conn := r.pool.Get()
	defer conn.Close()

	_, err := conn.Do("SET", key, value)

	return err
}

func (r *RedisPool) Get(key string) ([]byte, error) {
	conn := r.pool.Get()
	defer conn.Close()

	return redis.Bytes(conn.Do("GET", key))
}

func (r *RedisPool) Incr(key string) (int, error) {
	conn := r.pool.Get()
	defer conn.Close()

	return redis.Int(conn.Do("INCR", key))
}

func (r *RedisPool) HMSet(key string, kv ...string) error {
	conn := r.pool.Get()
	defer conn.Close()

	_, err := conn.Do("HMSET", redis.Args{key}.AddFlat(kv)...)

	return err
}

func (r *RedisPool) HSet(key, column string, val interface{}) error {
	conn := r.pool.Get()
	defer conn.Close()

	_, err := conn.Do("HSET", key, column, val)

	return err
}

func (r *RedisPool) HMSetObject(key string, object interface{}) error {
	conn := r.pool.Get()
	defer conn.Close()

	_, err := conn.Do("HMSET", redis.Args{key}.AddFlat(object)...)

	return err
}

func (r *RedisPool) HMSetKvs(key string, kvs map[string]string) error {
	var kvList []string
	for k, v := range kvs {
		kvList = append(kvList, k, v)
	}

	return r.HMSet(key, kvList...)
}

func (r *RedisPool) HGetAll(key string) (map[string]string, error) {
	conn := r.pool.Get()
	defer conn.Close()

	return redis.StringMap(conn.Do("HGETALL", key))
}

func (r *RedisPool) HGetAllObject(key string, result interface{}) error {
	conn := r.pool.Get()
	defer conn.Close()

	values, err := redis.Values(conn.Do("HGETALL", key))
	if err != nil {
		return err
	}

	if err := redis.ScanStruct(values, result); err != nil {
		return err
	}

	return nil
}

func (r *RedisPool) HGet(key, column string) (string, error) {
	conn := r.pool.Get()
	defer conn.Close()

	return redis.String(conn.Do("HGET", key, column))
}

func (r *RedisPool) HMGet(key string, columns ...string) (map[string]string, error) {
	conn := r.pool.Get()
	defer conn.Close()

	resArr, err := redis.Strings(conn.Do("HMGET", redis.Args{key}.AddFlat(columns)...))
	if err != nil {
		return nil, err
	}

	res := make(map[string]string, len(columns))

	if len(resArr) < len(columns) {
		return nil, ErrRedisUnknown
	}

	for idx, val := range columns {
		res[val] = resArr[idx]
	}

	return res, nil
}

func (r *RedisPool) Del(key string) error {
	conn := r.pool.Get()
	defer conn.Close()

	_, err := conn.Do("DEL", key)

	return err
}

func (r *RedisPool) LPush(key string, val ...string) error {
	conn := r.pool.Get()
	defer conn.Close()

	_, err := conn.Do("LPUSH", redis.Args{key}.AddFlat(val)...)

	return err
}

func (r *RedisPool) RPop(key string, val ...string) (string, error) {
	conn := r.pool.Get()
	defer conn.Close

	return redis.String(conn.Do("RPop", redis.Args{key}.AddFlat(val)...))
}

func (r *RedisPool) RPush(key string, val ...string) error {
	conn := r.pool.Get()
	defer conn.Close()

	_, err := conn.Do("RPUSH", redis.Args{key}.AddFlat(val)...)

	return err
}

func (r *RedisPool) LRange(key string, start int, end int) ([]string, error) {
	conn := r.pool.Get()
	defer conn.Close()

	return redis.Strings(conn.Do("LRANGE", key, start, end))
}

func (r *RedisPool) Close() error {
	if r.pool != nil {
		return r.pool.Close()
	}

	return nil
}
