package redisobj

import (
	"strconv"
	"time"

	"github.com/go-redis/redis"
)

type Countor core

func NewCountor(redis *redis.Client, key string) Countor {
	return Countor{
		redis: redis,
		key:   key,
	}
}

func (this *Countor) Inc() (int, error) {
	rs, err := this.redis.Incr(this.key).Result()
	return int(rs), err
}

func (this *Countor) IncBy(inc int) (int, error) {
	rs, err := this.redis.IncrBy(this.key, int64(inc)).Result()
	return int(rs), err
}

func (this *Countor) DecBy(dec int) (int, error) {
	rs, err := this.redis.DecrBy(this.key, int64(dec)).Result()
	return int(rs), err
}

func (this *Countor) IncWithTTL(ttl time.Duration) (int, error) {
	v, err := this.Inc()
	this.SetTTL(ttl)
	return v, err
}

func (this *Countor) Dec() (int, error) {
	rs, err := this.redis.Decr(this.key).Result()
	return int(rs), err
}

func (this *Countor) DecWithTTL(ttl time.Duration) (int, error) {
	v, err := this.Dec()
	this.SetTTL(ttl)
	return v, err
}

func (this *Countor) Set(val int) error {
	_, err := this.redis.Set(this.key, val, 0).Result()
	return err
}

// setnx with ttl
func (this *Countor) InitOnce(val int, ttl time.Duration) error {
	return this.redis.SetNX(this.key, val, ttl).Err()
}

func (this *Countor) SetWithTTL(val int, ttl time.Duration) error {
	_, err := this.redis.Set(this.key, val, ttl).Result()
	return err
}

func (this *Countor) Get() (int, error) {
	rs, err := this.redis.Get(this.key).Result()
	if err != nil {
		return 0, err
	}
	rsInt, err := strconv.Atoi(rs)
	return rsInt, err
}

func (this *Countor) Reset() {
	this.redis.Del(this.key)
}

func (this *Countor) SetTTL(ttl time.Duration) {
	this.redis.Expire(this.key, ttl)
}

func (this *Countor) GetTTL() (time.Duration, error) {
	return this.redis.TTL(this.key).Result()
}

func (this *Countor) SetTTLAt(ts time.Time) error {
	return this.redis.ExpireAt(this.key, ts).Err()
}
