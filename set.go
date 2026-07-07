package redisobj

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type Set struct {
	*core
}

func NewSet(c *redis.Client, key string) Set {
	return Set{
		core: newCore(c, key),
	}
}

// func (this *Set) buildKey(key string) string {
// 	return strings.Join([]string{this.baseKey, key}, ":")
// }

func (this *Set) Add(elems ...interface{}) (int, error) {
	ctx := context.TODO()
	countAdded, err := this.redis.SAdd(ctx, this.key, elems...).Result()
	return int(countAdded), err
}

func (this *Set) Delete(elems ...interface{}) (int, error) {
	ctx := context.TODO()
	countDeleted, err := this.redis.SRem(ctx, this.key, elems...).Result()
	if err == redis.Nil {
		err = nil
	}
	return int(countDeleted), err
}

func (this *Set) Has(elem string) (bool, error) {
	ctx := context.TODO()
	ok, err := this.redis.SIsMember(ctx, this.key, elem).Result()
	if err == redis.Nil {
		err = nil
	}
	return ok, err
}

func (this *Set) ToList() ([]string, error) {
	ctx := context.TODO()
	rs, err := this.redis.SMembers(ctx, this.key).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}
	return rs, nil
}

func (this *Set) Size() (int64, error) {
	c := context.TODO()
	size, err := this.redis.SCard(c, this.key).Result()
	if err == redis.Nil {
		err = nil
	}
	return size, err
}

func (this *Set) Reset(elems []string) error {
	ctx := context.TODO()
	_, _ = this.redis.Del(ctx, this.key).Result()
	_, err := this.redis.SAdd(ctx, this.key, elems).Result()
	return err
}

func (this *Set) Clear() error {
	c := context.TODO()
	_, err := this.redis.Del(c, this.key).Result()
	return err
}

func (this *Set) Scan(match string, count int64, cb func(string) bool) error {
	var cursor = uint64(0)
	var isFirstLoop = true
	for cursor > 0 || isFirstLoop {
		isFirstLoop = false
		c := context.TODO()
		keys, _cursor, err := this.redis.SScan(c, this.key, cursor, match, count).Result()
		if err != nil {
			return err
		}
		cursor = _cursor
		for _, k := range keys {
			if !cb(k) {
				break
			}
		}
	}
	return nil
}

func (this *Set) SetTTL(ttl time.Duration) (exists bool, err error) {
	ctx := context.TODO()
	exists, err = this.redis.Expire(ctx, this.key, ttl).Result()
	return
}

func (this *Set) SetTTLAt(expiredAt time.Time) (exists bool, err error) {
	ctx := context.TODO()
	exists, err = this.redis.ExpireAt(ctx, this.key, expiredAt).Result()
	return
}
