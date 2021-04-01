package redisobj

import (
	"encoding/json"
	"time"

	"github.com/go-redis/redis"
)

type HashSet core

func NewHashSet(redis *redis.Client, key string) HashSet {
	return HashSet{
		redis: redis,
		key:   key,
	}
}

func (this *HashSet) Set(key string, s string) error {
	_, err := this.redis.HSet(this.key, key, s).Result()
	return err
}

func (this *HashSet) Get(key string) (string, error) {
	return this.redis.HGet(this.key, key).Result()
}

func (this *HashSet) Del(field string) error {
	_, err := this.redis.HDel(this.key, field).Result()
	return err
}

func (this *HashSet) SetObject(key string, obj interface{}) error {
	data, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	_, err = this.redis.HSet(this.key, key, string(data)).Result()
	return err
}

func (this *HashSet) GetObject(key string, obj interface{}) error {
	data, err := this.redis.HGet(this.key, key).Result()
	if err != nil {
		return err
	}
	return json.Unmarshal([]byte(data), &obj)
}

func (this *HashSet) SetTTL(ttl time.Duration) {
	this.redis.Expire(this.key, ttl)
}

func (this *HashSet) GetKey() string {
	return this.key
}
