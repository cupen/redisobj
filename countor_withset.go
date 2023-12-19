package redisobj

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
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
	c := context.TODO()
	_, err := this.redis.SAdd(c, this.key, elems...).Result()
	if err != nil {
		return err
	}
	// log.Printf("key:%s inc:%#v", this.key, elems)
	return nil
	// size, err := this.redis.SCard(c, this.key).Result()
	// if err != nil {
	// 	if err != redis.Nil {
	// 		return 0, err
	// 	}
	// }
	// return int(size), nil
}

func (this *CountorWithSet) Dec(elems ...interface{}) error {
	c := context.TODO()
	_, err := this.redis.SRem(c, this.key, elems...).Result()
	if err != nil {
		if err == redis.Nil {
			err = nil
		}
		return err
	}
	// log.Printf("key:%s dec:%#v", this.key, elems)
	// size, err := this.redis.SCard(c, this.key).Result()
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
	c := context.TODO()
	_, err := this.redis.Unlink(c, this.key).Result()
	return err
}

func (this *CountorWithSet) SetTTL(ttl time.Duration) {
	c := context.TODO()
	this.redis.Expire(c, this.key, ttl)
}

// func (this *CountorWithSet) SetTTLAt(expiredAt time.Time) error {
// 	return this.redis.ExpireAt(c, this.key, expiredAt).Err()
// }

func (this *CountorWithSet) Size() (int, error) {
	c := context.TODO()
	size, err := this.redis.SCard(c, this.key).Result()
	if err != nil && err != redis.Nil {
		return 0, err
	}
	return int(size), nil
}
