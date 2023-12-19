package redisobj

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type Serializer interface {
	Marshal(interface{}) ([]byte, error)
	Unmarshal([]byte, interface{}) error
}

type Value struct {
	*core
	serializer Serializer
}

func New(rds *redis.Client, key string, serializer Serializer) Value {
	if serializer == nil {
		panic(ErrNullSerializer)
	}
	return Value{
		core:       newCore(rds, key),
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
	ctx := context.TODO()
	return this.redis.Set(ctx, this.key, data, ttl).Err()
}

func (this *Value) Get(obj interface{}) error {
	ctx := context.TODO()
	data, err := this.redis.Get(ctx, this.key).Bytes()
	if err != nil {
		return err
	}
	return this.serializer.Unmarshal(data, obj)
}

func (this *Value) Delete() error {
	ctx := context.TODO()
	err := this.redis.Del(ctx, this.key).Err()
	if err == redis.Nil {
		return nil
	}
	return err
}
