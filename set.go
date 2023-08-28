package redisobj

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
)

// 需配合 grace.Recover 一起用
type Set struct {
	redis *redis.Client
	key   string
}

func NewSet(c *redis.Client, key string) Set {
	return Set{
		redis: c,
		key:   key,
	}
}

// func (this *Set) buildKey(key string) string {
// 	return strings.Join([]string{this.baseKey, key}, ":")
// }

func (this *Set) Add(elems ...interface{}) bool {
	c := context.TODO()
	countAdded, err := this.redis.SAdd(c, this.key, elems...).Result()
	if err != nil {
		panic(err)
	}
	return countAdded > 0
}

func (this *Set) Del(elems ...interface{}) bool {
	c := context.TODO()
	countDeleted, err := this.redis.SRem(c, this.key, elems...).Result()
	if err != nil {
		if err == redis.Nil {
			return false
		}
		panic(err)
	}
	return countDeleted > 0
}

func (this *Set) Has(elem string) bool {
	c := context.TODO()
	ok, err := this.redis.SIsMember(c, this.key, elem).Result()
	if err != nil {
		if err == redis.Nil {
			return false
		}
		panic(err)
	}
	return ok
}

func (this *Set) ToList() ([]string, error) {
	c := context.TODO()
	rs, err := this.redis.SMembers(c, this.key).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}
	return rs, nil
}

func (this *Set) Size() int {
	c := context.TODO()
	size, err := this.redis.SCard(c, this.key).Result()
	if err != nil {
		if err == redis.Nil {
			return 0
		}
		panic(err)
	}
	return int(size)
}

func (this *Set) Reset(elems []string) error {
	c := context.TODO()
	_, _ = this.redis.Del(c, this.key).Result()
	_, err := this.redis.SAdd(c, this.key, elems).Result()
	return err
}

func (this *Set) Clear() error {
	c := context.TODO()
	_, err := this.redis.Del(c, this.key).Result()
	return err
}

func (this *Set) SetTTL(ttl time.Duration) error {
	c := context.TODO()
	_, err := this.redis.Expire(c, this.key, ttl).Result()
	return err
}

func (this *Set) SetTTLAt(expiredAt time.Time) error {
	c := context.TODO()
	return this.redis.ExpireAt(c, this.key, expiredAt).Err()
}
