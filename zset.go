package redisobj

import (
	"fmt"
	"time"

	"github.com/go-redis/redis"
)

const (
	OrderingDesc = 1
	OrderingAsc  = 2
)

type ZSet struct {
	redis      *redis.Client
	key        string
	ordering   int
	maxMembers int
}

func NewZSet(c *redis.Client, key string) ZSet {
	return ZSet{
		redis:    c,
		ordering: OrderingDesc,
		key:      key,
	}
}

func NewZSetWithLimit(c *redis.Client, key string, maxMembers int) ZSet {
	return ZSet{
		redis:      c,
		ordering:   OrderingDesc,
		key:        key,
		maxMembers: maxMembers,
	}
}

func (this *ZSet) SetOrdering(ordering int) {
	this.ordering = ordering
}

func (this *ZSet) GetOrdering() int {
	return this.ordering
}

func (this *ZSet) Add(member interface{}, score int64) error {
	elem := redis.Z{Member: member, Score: float64(score)}
	_, err := this.redis.ZAdd(this.key, elem).Result()
	if err != nil {
		return err
	}
	if this.maxMembers > 0 {
		_, _ = this.LimitIf(this.maxMembers)
	}
	return nil
}

func (this *ZSet) AddBatch(elems ...redis.Z) bool {
	countAdded, err := this.redis.ZAdd(this.key, elems...).Result()
	if err != nil {
		panic(err)
	}
	if this.maxMembers > 0 {
		_, _ = this.LimitIf(this.maxMembers)
	}
	return countAdded > 0
}

func (this *ZSet) Del(member string) error {
	_, err := this.redis.ZRem(this.key, member).Result()
	if err == redis.Nil {
		err = nil
	}
	return err
}

func (this *ZSet) Has(elem string) bool {
	_, err := this.redis.ZScore(this.key, elem).Result()
	if err != nil {
		if err == redis.Nil {
			return false
		}
		panic(err)
	}
	return true
}

func (this *ZSet) Size() (int, error) {
	size, err := this.redis.ZCard(this.key).Result()
	if err != nil {
		if err == redis.Nil {
			return 0, nil
		}
		// panic(err)
	}
	return int(size), err
}

func (this *ZSet) Clear() error {
	_, err := this.redis.Del(this.key).Result()
	return err
}

func (this *ZSet) GetListByOrder(start int, end int, ordering int) ([]redis.Z, error) {
	if ordering == OrderingDesc {
		list, err := this.redis.ZRevRangeWithScores(this.key, int64(start), int64(end)).Result()
		return list, err
	}
	list, err := this.redis.ZRangeWithScores(this.key, int64(start), int64(end)).Result()
	return list, err
}

func (this *ZSet) GetList(start int, count int) ([]redis.Z, error) {
	if count <= 0 {
		return nil, nil
	}
	end := start + count - 1
	if start > end {
		return nil, fmt.Errorf("Invalid params start(%d) > end(%d)", start, end)
	}
	return this.GetListByOrder(start, count, this.ordering)
}

func (this *ZSet) GetScore(member string) (int64, error) {
	key := this.key
	score, err := this.redis.ZScore(key, member).Result()
	if err == redis.Nil {
		return 0, nil
	}
	return int64(score), err
}

func (this *ZSet) LimitIf(maxMembers int) (int64, error) {
	if maxMembers <= 0 {
		// unlimit
		return 0, nil
	}
	return this.DelByRanking(maxMembers+1, 3)
}

func (this *ZSet) GetTop(count int) ([]redis.Z, error) {
	return this.GetList(0, count)
}

func (this *ZSet) SetTTL(ttl time.Duration) error {
	return this.redis.Expire(this.key, ttl).Err()
}

func (this *ZSet) Exists() (bool, error) {
	flag, err := this.redis.Exists(this.key).Result()
	return flag > 0, err
}

func (this *ZSet) DelByRanking(ranking int, count int) (int64, error) {
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
	// log.Printf("key=%s start=%d end=%d\n", key, start, end)
	delCount, err := rds.ZRemRangeByRank(key, start, end).Result()
	if err == redis.Nil {
		err = nil
	}
	return delCount, err
}
