package redisobj

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type Countor core

func NewCountor(redis *redis.Client, key string) Countor {
	return Countor{
		redis: redis,
		key:   key,
	}
}

func (this *Countor) Inc() (int, error) {
	c := context.TODO()
	rs, err := this.redis.Incr(c, this.key).Result()
	return int(rs), err
}

func (this *Countor) IncBy(inc int) (int, error) {
	c := context.TODO()
	rs, err := this.redis.IncrBy(c, this.key, int64(inc)).Result()
	return int(rs), err
}

func (this *Countor) DecBy(dec int) (int, error) {
	c := context.TODO()
	rs, err := this.redis.DecrBy(c, this.key, int64(dec)).Result()
	return int(rs), err
}

func (this *Countor) IncWithTTL(ttl time.Duration) (int, error) {
	v, err := this.Inc()
	this.SetTTL(ttl)
	return v, err
}

func (this *Countor) Dec() (int, error) {
	c := context.TODO()
	rs, err := this.redis.Decr(c, this.key).Result()
	return int(rs), err
}

func (this *Countor) DecWithTTL(ttl time.Duration) (int, error) {
	v, err := this.Dec()
	this.SetTTL(ttl)
	return v, err
}

func (this *Countor) Set(val int) error {
	c := context.TODO()
	_, err := this.redis.Set(c, this.key, val, 0).Result()
	return err
}

// setnx with ttl
func (this *Countor) InitOnce(val int, ttl time.Duration) error {
	c := context.TODO()
	return this.redis.SetNX(c, this.key, val, ttl).Err()
}

func (this *Countor) SetWithTTL(val int, ttl time.Duration) error {
	c := context.TODO()
	_, err := this.redis.Set(c, this.key, val, ttl).Result()
	return err
}

func (this *Countor) Get() (int, error) {
	c := context.TODO()
	rs, err := this.redis.Get(c, this.key).Result()
	if err != nil {
		return 0, err
	}
	rsInt, err := strconv.Atoi(rs)
	return rsInt, err
}

func (this *Countor) Reset() {
	c := context.TODO()
	this.redis.Del(c, this.key)
}

func (this *Countor) SetTTL(ttl time.Duration) {
	c := context.TODO()
	this.redis.Expire(c, this.key, ttl)
}

func (this *Countor) GetTTL() (time.Duration, error) {
	c := context.TODO()
	return this.redis.TTL(c, this.key).Result()
}

func (this *Countor) SetTTLAt(ts time.Time) error {
	c := context.TODO()
	return this.redis.ExpireAt(c, this.key, ts).Err()
}
