package redisobj

import (
	"errors"
	"time"

	"github.com/go-redis/redis"
)

type Serializer interface {
	Marshal(interface{}) ([]byte, error)
	Unmarshal([]byte, interface{}) error
}

type Value struct {
	core
	serializer Serializer
}

func New(rds *redis.Client, key string, serializer Serializer) Value {
	if serializer == nil {
		panic(errors.New("nil serializer"))
	}
	return Value{
		core: core{
			redis: rds,
			key:   key,
		},
		serializer: serializer,
	}
}

func (this *Value) GetKey() string {
	return this.key
}

func (this *Value) Set(obj interface{}, ttl time.Duration) error {
	data, err := this.serializer.Marshal(obj)
	if err != nil {
		return err
	}
	key := this.key
	return this.redis.Set(key, data, ttl).Err()
}

func (this *Value) Get(obj interface{}) error {
	key := this.key
	data, err := this.redis.Get(key).Bytes()
	if err != nil {
		return err
	}
	return this.serializer.Unmarshal(data, obj)
}

func (this *Value) Delete() error {
	err := this.redis.Del(this.key).Err()
	if err == redis.Nil {
		return nil
	}
	return err
}
