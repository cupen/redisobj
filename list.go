package redisobj

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type List core

func NewList(redis *redis.Client, key string) List {
	return List{
		redis: redis,
		key:   key,
	}
}

// func (this *List) Insert(key string, s string) error {
// 	_, err := this.redis.LInsert(c, this.key, key, s).Result()
// 	return err
// }
// func (this *List) Delete(key string, s string) error {
// 	_, err := this.redis.HSet(c, this.key, key, s).Result()
// 	return err
// }

func (this *List) Append(val ...interface{}) (int64, error) {
	c := context.TODO()
	return this.redis.RPush(c, this.key, val...).Result()
}

func (this *List) Pop(fromLeft ...bool) *redis.StringCmd {
	c := context.TODO()
	if len(fromLeft) > 0 && fromLeft[0] {
		return this.redis.LPop(c, this.key)
	}
	return this.redis.RPop(c, this.key)
}

func (this *List) PopWithBlocking(timeout time.Duration, fromLeft ...bool) *redis.StringSliceCmd {
	c := context.TODO()
	if len(fromLeft) > 0 && fromLeft[0] {
		return this.redis.BLPop(c, timeout, this.key)
	}
	return this.redis.BRPop(c, timeout, this.key)
}

func (this *List) SetTTL(ttl time.Duration) {
	c := context.TODO()
	this.redis.Expire(c, this.key, ttl)
}
