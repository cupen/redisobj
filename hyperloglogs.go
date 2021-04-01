package redisobj

import (
	"time"

	"github.com/go-redis/redis"
)

type HyperLogLogs struct {
	redis *redis.Client
	key   string
}

func NewHyperLogLogs(redis *redis.Client, key string) HyperLogLogs {
	return HyperLogLogs{
		redis: redis,
		key:   key,
	}
}

func (this HyperLogLogs) Add(elems ...interface{}) {
	this.redis.PFAdd(this.key, elems...)
}

func (this HyperLogLogs) Count() (int64, error) {
	c, err := this.redis.PFCount(this.key).Result()
	if err == redis.Nil {
		err = nil
	}
	return c, err
}

func (this HyperLogLogs) SetTTL(ttl time.Duration) (bool, error) {
	return this.redis.Expire(this.key, ttl).Result()
}
