package redisobj

import (
	"time"

	"github.com/go-redis/redis"
)

type CountorWithSet struct {
	core
	min int
	max int
	ttl time.Duration
}

func NewCountorWithSet(redis *redis.Client, key string, max int, ttl time.Duration) CountorWithSet {
	return CountorWithSet{
		core: core{redis, key},
		max:  max,
		ttl:  ttl,
	}
}

func (this *CountorWithSet) WithTTL(ttl time.Duration, do func(c *CountorWithSet)) error {
	if ttl > 0 {
		defer this.SetTTL(ttl)
	}
	do(this)
	return nil
}

func (this *CountorWithSet) Inc(elems ...interface{}) error {
	_, err := this.redis.SAdd(this.key, elems...).Result()
	if err != nil {
		return err
	}
	// log.Printf("key:%s inc:%#v", this.key, elems)
	return nil
	// size, err := this.redis.SCard(this.key).Result()
	// if err != nil {
	// 	if err != redis.Nil {
	// 		return 0, err
	// 	}
	// }
	// return int(size), nil
}

func (this *CountorWithSet) Dec(elems ...interface{}) error {
	_, err := this.redis.SRem(this.key, elems...).Result()
	if err != nil {
		if err == redis.Nil {
			err = nil
		}
		return err
	}
	// log.Printf("key:%s dec:%#v", this.key, elems)
	// size, err := this.redis.SCard(this.key).Result()
	// if err != nil {
	// 	if err != redis.Nil {
	// 		return 0, err
	// 	}
	// }
	return nil
}

func (this *CountorWithSet) Get() (int, error) {
	return this.Size()
}

func (this *CountorWithSet) Reset() error {
	_, err := this.redis.Unlink(this.key).Result()
	return err
}

func (this *CountorWithSet) SetTTL(ttl time.Duration) {
	this.redis.Expire(this.key, ttl)
}

// func (this *CountorWithSet) SetTTLAt(expiredAt time.Time) error {
// 	return this.redis.ExpireAt(this.key, expiredAt).Err()
// }

func (this *CountorWithSet) Size() (int, error) {
	size, err := this.redis.SCard(this.key).Result()
	if err != nil && err != redis.Nil {
		return 0, err
	}
	return int(size), nil
}
