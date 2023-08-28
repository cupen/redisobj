package redisobj

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
)

type SetV2 struct {
	redis *redis.Client
	key   string
}

func NewSetV2(c *redis.Client, key string) SetV2 {
	return SetV2{
		redis: c,
		key:   key,
	}
}

// func (this *Set) buildKey(key string) string {
// 	return strings.Join([]string{this.baseKey, key}, ":")
// }

func (this *SetV2) Add(elems ...interface{}) (int, error) {
	c := context.TODO()
	countAdded, err := this.redis.SAdd(c, this.key, elems...).Result()
	return int(countAdded), err
}

func (this *SetV2) Del(elems ...interface{}) (int, error) {
	c := context.TODO()
	countDeleted, err := this.redis.SRem(c, this.key, elems...).Result()
	if err == redis.Nil {
		err = nil
	}
	return int(countDeleted), err
}

func (this *SetV2) Has(elem string) (bool, error) {
	c := context.TODO()
	ok, err := this.redis.SIsMember(c, this.key, elem).Result()
	if err == redis.Nil {
		err = nil
	}
	return ok, err
}

func (this *SetV2) ToList() ([]string, error) {
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

func (this *SetV2) Size() (int, error) {
	c := context.TODO()
	size, err := this.redis.SCard(c, this.key).Result()
	if err == redis.Nil {
		err = nil
	}
	return int(size), err
}

func (this *SetV2) Reset(elems []string) error {
	c := context.TODO()
	_, _ = this.redis.Del(c, this.key).Result()
	_, err := this.redis.SAdd(c, this.key, elems).Result()
	return err
}

func (this *SetV2) Clear() error {
	c := context.TODO()
	_, err := this.redis.Del(c, this.key).Result()
	return err
}

func (this *SetV2) Foreach(cb func(row string) bool) error {
	var MAX_LOOPS = 1000 // 最多 500w 吧.
	var cursor = uint64(0)
	for i := 0; i < MAX_LOOPS; i++ {
		c := context.TODO()
		keys, _cursor, err := this.redis.SScan(c, this.key, cursor, "", 5000).Result()
		if err != nil {
			if err == redis.Nil {
				return nil
			}
			return err
		}
		cursor = _cursor
		for _, k := range keys {
			if !cb(k) {
				break
			}
		}
		// 0 表示终止
		if cursor == 0 {
			return nil
		}
	}
	return nil
}

func (this *SetV2) SetTTL(ttl time.Duration) (isExists bool, err error) {
	c := context.TODO()
	isExists, err = this.redis.Expire(c, this.key, ttl).Result()
	return
}

func (this *SetV2) SetTTLAt(expiredAt time.Time) (isExists bool, err error) {
	c := context.TODO()
	isExists, err = this.redis.ExpireAt(c, this.key, expiredAt).Result()
	return
}
