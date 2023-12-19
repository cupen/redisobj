package redisobj

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
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
	c := context.TODO()
	this.redis.PFAdd(c, this.key, elems...)
}

func (this HyperLogLogs) Count() (int64, error) {
	c := context.TODO()
	count, err := this.redis.PFCount(c, this.key).Result()
	if err == redis.Nil {
		err = nil
	}
	return count, err
}

func (this HyperLogLogs) SetTTL(ttl time.Duration) (bool, error) {
	c := context.TODO()
	return this.redis.Expire(c, this.key, ttl).Result()
}
