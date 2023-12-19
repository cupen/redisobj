package redisobj

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
)

type HashSet core

func NewHashSet(redis *redis.Client, key string) HashSet {
	return HashSet{
		redis: redis,
		key:   key,
	}
}

func (this *HashSet) Set(key string, s string) error {
	c := context.TODO()
	_, err := this.redis.HSet(c, this.key, key, s).Result()
	return err
}

func (this *HashSet) Get(key string) (string, error) {
	c := context.TODO()
	return this.redis.HGet(c, this.key, key).Result()
}

func (this *HashSet) Del(field string) error {
	c := context.TODO()
	_, err := this.redis.HDel(c, this.key, field).Result()
	return err
}

func (this *HashSet) SetObject(key string, obj interface{}) error {
	data, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	c := context.TODO()
	_, err = this.redis.HSet(c, this.key, key, string(data)).Result()
	return err
}

func (this *HashSet) GetObject(key string, obj interface{}) error {
	c := context.TODO()
	data, err := this.redis.HGet(c, this.key, key).Result()
	if err != nil {
		return err
	}
	return json.Unmarshal([]byte(data), &obj)
}

func (this *HashSet) SetTTL(ttl time.Duration) {
	c := context.TODO()
	this.redis.Expire(c, this.key, ttl)
}

func (this *HashSet) GetKey() string {
	return this.key
}
