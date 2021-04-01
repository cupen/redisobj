package redisobj

import (
	"time"

	"github.com/go-redis/redis"
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
	countAdded, err := this.redis.SAdd(this.key, elems...).Result()
	if err != nil {
		panic(err)
	}
	return countAdded > 0
}

func (this *Set) Del(elems ...interface{}) bool {
	countDeleted, err := this.redis.SRem(this.key, elems...).Result()
	if err != nil {
		if err == redis.Nil {
			return false
		}
		panic(err)
	}
	return countDeleted > 0
}

func (this *Set) Has(elem string) bool {
	ok, err := this.redis.SIsMember(this.key, elem).Result()
	if err != nil {
		if err == redis.Nil {
			return false
		}
		panic(err)
	}
	return ok
}

func (this *Set) ToList() ([]string, error) {
	rs, err := this.redis.SMembers(this.key).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}
	return rs, nil
}

func (this *Set) Size() int {
	size, err := this.redis.SCard(this.key).Result()
	if err != nil {
		if err == redis.Nil {
			return 0
		}
		panic(err)
	}
	return int(size)
}

func (this *Set) Reset(elems []string) error {
	_, _ = this.redis.Del(this.key).Result()
	_, err := this.redis.SAdd(this.key, elems).Result()
	return err
}

func (this *Set) Clear() error {
	_, err := this.redis.Del(this.key).Result()
	return err
}

func (this *Set) SetTTL(ttl time.Duration) error {
	_, err := this.redis.Expire(this.key, ttl).Result()
	return err
}

func (this *Set) SetTTLAt(expiredAt time.Time) error {
	return this.redis.ExpireAt(this.key, expiredAt).Err()
}
