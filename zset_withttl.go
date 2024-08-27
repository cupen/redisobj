package redisobj

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type ZSetItemString struct {
	ID    string
	Score int
}

type ZSetWithTTL struct {
	*ZSet
	ttl time.Duration
}

func NewZSetWithTTL(c *redis.Client, key string, ttl time.Duration) *ZSetWithTTL {
	zset := NewZSet(c, key)
	zset.SetOrdering(OrderingDesc)
	return &ZSetWithTTL{
		ZSet: zset,
		ttl:  ttl,
	}
}

func (this *ZSetWithTTL) SetOrdering(ordering int) {
	this.ordering = ordering
}

func (this *ZSetWithTTL) GetOrdering() int {
	return this.ordering
}

func (this *ZSetWithTTL) Add(member string, ts time.Time) bool {
	elem := redis.Z{Member: member, Score: float64(ts.Unix())}
	c := context.TODO()
	countAdded, err := this.redis.ZAddNX(c, this.key, elem).Result()
	if err != nil {
		panic(err)
	}
	return countAdded > 0
}

func (this *ZSetWithTTL) Set(member string, ts time.Time) error {
	key := this.key
	m := redis.Z{
		Score:  float64(ts.Unix()),
		Member: member,
	}
	c := context.TODO()
	_, err := this.redis.ZAdd(c, key, m).Result()
	return err
}

func (this *ZSetWithTTL) Size() (int, error) {
	c := context.TODO()
	size, err := this.redis.ZCard(c, this.key).Result()
	if err != nil {
		if err == redis.Nil {
			return 0, nil
		}
		// panic(err)
	}
	return int(size), err
}

func (this *ZSetWithTTL) Clear() error {
	c := context.TODO()
	_, err := this.redis.Unlink(c, this.key).Result()
	return err
}

func (this *ZSetWithTTL) getListByOrder(start int, end int, ordering int) ([]redis.Z, error) {
	c := context.TODO()
	if ordering == OrderingDesc {
		list, err := this.redis.ZRevRangeWithScores(c, this.key, int64(start), int64(end)).Result()
		return list, err
	}
	list, err := this.redis.ZRangeWithScores(c, this.key, int64(start), int64(end)).Result()
	return list, err
}

func (this *ZSetWithTTL) GetList(start int, count int) ([]redis.Z, error) {
	if count <= 0 {
		return nil, nil
	}
	end := start + count - 1
	if start > end {
		return nil, fmt.Errorf("Invalid params start(%d) > end(%d)", start, end)
	}

	return this.getListByOrder(start, count, this.ordering)
}

func (this *ZSetWithTTL) GetScore(member string) (int64, error) {
	key := this.key
	c := context.TODO()
	score, err := this.redis.ZScore(c, key, member).Result()
	if err == redis.Nil {
		return 0, nil
	}
	return int64(score), err
}

func (this *ZSetWithTTL) LimitIf(maxMembers int) (int64, error) {
	if maxMembers <= 0 {
		// unlimit
		return 0, nil
	}
	return this.DelByRanking(maxMembers+1, 3)
}

func (this *ZSetWithTTL) GetTop(count int, now time.Time) ([]redis.Z, error) {
	items, err := this.GetList(0, count)
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}

	var curIndex = -1
	var expiredAt = float64(now.Unix() - int64(this.ttl/time.Second))
	for i, item := range items {
		if item.Score < expiredAt {
			break
		}
		curIndex = i
	}
	if curIndex < 0 {
		return nil, nil
	}
	return items[0 : curIndex+1], nil
}

func (this *ZSetWithTTL) SetTTL(ttl time.Duration) error {
	c := context.TODO()
	return this.redis.Expire(c, this.key, ttl).Err()
}

func (this *ZSetWithTTL) Exists() (bool, error) {
	c := context.TODO()
	flag, err := this.redis.Exists(c, this.key).Result()
	if err == redis.Nil {
		err = nil
	}
	return flag > 0, err
}

func (this *ZSetWithTTL) DelByRanking(ranking int, count int) (int64, error) {
	if ranking <= 0 {
		return 0, fmt.Errorf("Invalid ranking:%d", ranking)
	}
	if count < 1 {
		return 0, nil
	}

	// 0 lowest
	// -1 highest
	var start int64
	var end int64
	if this.ordering == OrderingAsc {
		start = int64(ranking - 1)
		end = int64((ranking - 1) + (count - 1))
	} else {
		start = int64((-ranking) - (count - 1))
		end = int64(-ranking)
	}
	key := this.key
	rds := this.redis
	c := context.TODO()
	delCount, err := rds.ZRemRangeByRank(c, key, start, end).Result()
	if err == redis.Nil {
		err = nil
	}
	return delCount, err
}
