package redisobj

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
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

func NewZSet(c *redis.Client, key string) *ZSet {
	return &ZSet{
		redis:    c,
		ordering: OrderingDesc,
		key:      key,
	}
}

func (this *ZSet) SetOrdering(ordering int) {
	this.ordering = ordering
}

func (this *ZSet) GetOrdering() int {
	return this.ordering
}

func (this *ZSet) Set(member string, score int64) error {
	c := context.TODO()
	elem := redis.Z{Member: member, Score: float64(score)}
	_, err := this.redis.ZAdd(c, this.key, elem).Result()
	if err != nil {
		return err
	}
	if this.maxMembers > 0 {
		_, _ = this.LimitIf(this.maxMembers)
	}
	return nil
}

func (this *ZSet) Add(member string, score int64) error {
	return this.Set(member, score)
}

func (this *ZSet) AddBatch(elems ...redis.Z) error {
	c := context.TODO()
	_, err := this.redis.ZAdd(c, this.key, elems...).Result()
	if err != nil {
		return err
	}
	if this.maxMembers > 0 {
		_, _ = this.LimitIf(this.maxMembers)
	}
	return nil
}

func (this *ZSet) Del(member string) error {
	c := context.TODO()
	_, err := this.redis.ZRem(c, this.key, member).Result()
	if err != nil {
		if err == redis.Nil {
			return nil
		}
		return err
	}
	return err
}

func (this *ZSet) Has(elem string) (bool, error) {
	c := context.TODO()
	score, err := this.redis.ZScore(c, this.key, elem).Result()
	if err != nil {
		if err == redis.Nil {
			return false, nil
		}
		return false, err
	}
	return score > 0, nil
}

func (this *ZSet) Size() (int, error) {
	c := context.TODO()
	size, err := this.redis.ZCard(c, this.key).Result()
	if err != nil {
		if err == redis.Nil {
			return 0, nil
		}
		return 0, err
	}
	return int(size), nil
}

func (this *ZSet) Clear() error {
	c := context.TODO()
	_, err := this.redis.Unlink(c, this.key).Result()
	if err != nil {
		if err == redis.Nil {
			return nil
		}
		return err
	}
	return nil
}

func (this *ZSet) GetListByOrder(start int, count int, ordering int) ([]redis.Z, error) {
	c := context.TODO()
	end := start + count - 1
	if start > end {
		return nil, fmt.Errorf("invalid params: start(%d) > end(%d)", start, end)
	}

	if ordering == OrderingDesc {
		list, err := this.redis.ZRevRangeWithScores(c, this.key, int64(start), int64(end)).Result()
		return list, err
	}
	list, err := this.redis.ZRangeWithScores(c, this.key, int64(start), int64(end)).Result()
	return list, err
}

func (this *ZSet) GetList(start int, count int) ([]redis.Z, error) {
	if count <= 0 {
		return nil, nil
	}

	return this.GetListByOrder(start, count, this.ordering)
}

func (this *ZSet) GetScore(member string) (int64, error) {
	key := this.key
	c := context.TODO()
	score, err := this.redis.ZScore(c, key, member).Result()
	if err == redis.Nil {
		return 0, nil
	}
	return int64(score), err
}

func (this *ZSet) GetRanking(member string) (int64, error) {
	key := this.key
	var ranking int64 = 0
	var err error
	c := context.TODO()
	if this.ordering == OrderingDesc {
		ranking, err = this.redis.ZRevRank(c, key, member).Result()
	} else {
		ranking, err = this.redis.ZRank(c, key, member).Result()
	}
	// fmt.Printf("ranking = %d, err = %v", ranking, err)
	if err == nil {
		ranking += 1
	} else if err == redis.Nil {
		return 0, nil
	}
	return ranking, err
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
	c := context.TODO()
	return this.redis.Expire(c, this.key, ttl).Err()
}

func (this *ZSet) Exists() (bool, error) {
	c := context.TODO()
	flag, err := this.redis.Exists(c, this.key).Result()
	return flag > 0, err
}

func (this *ZSet) DelByRanking(ranking int, count int) (int64, error) {
	if ranking <= 0 {
		return 0, fmt.Errorf("invalid ranking:%d", ranking)
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
	// log.Printf("key=%s start=%d end=%d\n", key, start, end)
	delCount, err := rds.ZRemRangeByRank(c, key, start, end).Result()
	if err == redis.Nil {
		err = nil
	}
	return delCount, err
}

// shallow copy
func (this *ZSet) Clone() *ZSet {
	cloned := *this
	if &cloned == this {
		panic("clone failed")
	}
	return &cloned
}

func (this *ZSet) WithID(id string) *ZSet {
	cloned := this.Clone()
	cloned.key = strings.Join([]string{this.key, id}, ":")
	return cloned
}
