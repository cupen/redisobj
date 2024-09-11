package redisobj

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	ErrNil            = redis.Nil
	ErrEmptyKey       = errors.New("empty key")
	ErrNullClient     = errors.New("null client")
	ErrNullSerializer = errors.New("nil serializer")
)

func IsNil(err error) bool {
	return err == redis.Nil
}

type core struct {
	redis *redis.Client
	key   string
}

func newCore(rds *redis.Client, key string) *core {
	if rds == nil {
		panic(ErrNullClient)
	}
	if key == "" {
		panic(ErrEmptyKey)
	}
	return &core{
		redis: rds,
		key:   key,
	}
}

func (c *core) buildKey(name ...string) string {
	return strings.Join(append([]string{c.key}, name...), ":")
}

func (c *core) GetKey() string {
	return c.key
}

func (c *core) SetTTL(ttl time.Duration) error {
	ctx := context.TODO()
	return c.redis.Expire(ctx, c.key, ttl).Err()
}

func (c *core) SetTTLAt(ts time.Time) error {
	ctx := context.TODO()
	return c.redis.ExpireAt(ctx, c.key, ts).Err()
}
