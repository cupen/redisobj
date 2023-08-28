package redisobj

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
)

type core struct {
	redis *redis.Client
	key   string
}

func (this *core) GetKey() string {
	return this.key
}

func (this *core) SetTTL(ttl time.Duration) error {
	c := context.TODO()
	return this.redis.Expire(c, this.key, ttl).Err()
}

func (this *core) SetTTLAt(ts time.Time) error {
	c := context.TODO()
	return this.redis.ExpireAt(c, this.key, ts).Err()
}
