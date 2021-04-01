package redisobj

import (
	"time"

	"github.com/go-redis/redis"
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
	countAdded, err := this.redis.SAdd(this.key, elems...).Result()
	return int(countAdded), err
}

func (this *SetV2) Del(elems ...interface{}) (int, error) {
	countDeleted, err := this.redis.SRem(this.key, elems...).Result()
	if err == redis.Nil {
		err = nil
	}
	return int(countDeleted), err
}

func (this *SetV2) Has(elem string) (bool, error) {
	ok, err := this.redis.SIsMember(this.key, elem).Result()
	if err == redis.Nil {
		err = nil
	}
	return ok, err
}

func (this *SetV2) ToList() ([]string, error) {
	rs, err := this.redis.SMembers(this.key).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}
	return rs, nil
}

func (this *SetV2) Size() (int, error) {
	size, err := this.redis.SCard(this.key).Result()
	if err == redis.Nil {
		err = nil
	}
	return int(size), err
}

func (this *SetV2) Reset(elems []string) error {
	_, _ = this.redis.Del(this.key).Result()
	_, err := this.redis.SAdd(this.key, elems).Result()
	return err
}

func (this *SetV2) Clear() error {
	_, err := this.redis.Del(this.key).Result()
	return err
}

func (this *SetV2) Foreach(cb func(row string) bool) error {
	var MAX_LOOPS = 1000 // 最多 500w 吧.
	var cursor = uint64(0)
	for i := 0; i < MAX_LOOPS; i++ {
		keys, _cursor, err := this.redis.SScan(this.key, cursor, "", 5000).Result()
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
	isExists, err = this.redis.Expire(this.key, ttl).Result()
	return
}

func (this *SetV2) SetTTLAt(expiredAt time.Time) (isExists bool, err error) {
	isExists, err = this.redis.ExpireAt(this.key, expiredAt).Result()
	return
}
