package redisobj

import (
	"time"

	"github.com/go-redis/redis"
)

type List core

func NewList(redis *redis.Client, key string) List {
	return List{
		redis: redis,
		key:   key,
	}
}

// func (this *List) Insert(key string, s string) error {
// 	_, err := this.redis.LInsert(this.key, key, s).Result()
// 	return err
// }
// func (this *List) Delete(key string, s string) error {
// 	_, err := this.redis.HSet(this.key, key, s).Result()
// 	return err
// }

func (this *List) Append(val ...interface{}) (int64, error) {
	return this.redis.RPush(this.key, val...).Result()
}

func (this *List) Pop(fromLeft ...bool) *redis.StringCmd {
	if len(fromLeft) > 0 && fromLeft[0] {
		return this.redis.LPop(this.key)
	}
	return this.redis.RPop(this.key)
}

func (this *List) PopWithBlocking(timeout time.Duration, fromLeft ...bool) *redis.StringSliceCmd {
	if len(fromLeft) > 0 && fromLeft[0] {
		return this.redis.BLPop(timeout, this.key)
	}
	return this.redis.BRPop(timeout, this.key)
}

func (this *List) SetTTL(ttl time.Duration) {
	this.redis.Expire(this.key, ttl)
}
