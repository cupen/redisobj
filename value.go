package redisobj

import (
	"context"
	"errors"
	"time"

	"github.com/go-redis/redis/v8"
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
	c := context.TODO()
	return this.redis.Set(c, key, data, ttl).Err()
}

func (this *Value) Get(obj interface{}) error {
	key := this.key
	c := context.TODO()
	data, err := this.redis.Get(c, key).Bytes()
	if err != nil {
		return err
	}
	return this.serializer.Unmarshal(data, obj)
}

func (this *Value) Delete() error {
	c := context.TODO()
	err := this.redis.Del(c, this.key).Err()
	if err == redis.Nil {
		return nil
	}
	return err
}
