package redisobj

import (
	"time"

	"github.com/go-redis/redis"
)

type core struct {
	redis *redis.Client
	key   string
}

func (this *core) GetKey() string {
	return this.key
}

func (this *core) SetTTL(ttl time.Duration) error {
	return this.redis.Expire(this.key, ttl).Err()
}

func (this *core) SetTTLAt(ts time.Time) error {
	return this.redis.ExpireAt(this.key, ts).Err()
}
