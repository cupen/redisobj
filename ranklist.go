package redisobj

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strings"

	"github.com/cupen/redisobj/encoders"
	"github.com/redis/go-redis/v9"
)

type Dict map[string]interface{}

// const OrderingAsc = 0
// const OrderingDesc = 1

// type RankItem struct {
// 	Member string
// 	Score  float64
// 	Data   Dict
// }

type RankList struct {
	redis   *redis.Client
	baseKey string
	key     string

	MaxMembers int // 暂时不用
	Order      int // 0-从高到低，1-从低到高
	enc        encoders.Score
}

func NewRankList(redis *redis.Client, baseKey string) *RankList {
	if baseKey == "" {
		panic(fmt.Errorf("empty baseKey for ranklist"))
	}
	rank := RankList{
		redis:      redis,
		baseKey:    baseKey,
		key:        strings.Join([]string{baseKey, "default"}, ":"),
		MaxMembers: 0,
	}
	return rank.WithOrdering("desc")
}

func (this *RankList) SetID(rankId string) {
	if this.baseKey == "" {
		panic(errors.New("baseKey was empty"))
	}
	this.key = strings.Join([]string{this.baseKey, rankId}, ":")
}

func (this *RankList) WithID(rankId string) *RankList {
	cloned := this.Clone()
	cloned.SetID(rankId)
	return cloned
}

func (this *RankList) WithOrdering(name string) *RankList {
	_parse := func(ordering string) int {
		switch ordering {
		case "asc":
			return OrderingAsc
		case "desc":
			return OrderingDesc
		default:
			panic(fmt.Errorf("invalid order: \"%s\"", ordering))
		}
	}
	this.Order = _parse(name)
	return this
}

func (this *RankList) WithEncoder(enc encoders.Score) *RankList {
	this.enc = enc
	return this
}

func (this *RankList) GetEncoder() encoders.Score {
	return this.enc
}

func (this *RankList) GetRanking(member string) (int64, error) {
	key := this.key
	var ranking int64 = 0
	var err error
	c := context.TODO()
	if this.Order == OrderingDesc {
		ranking, err = this.redis.ZRevRank(c, key, member).Result()
	} else {
		ranking, err = this.redis.ZRank(c, key, member).Result()
	}
	if err != nil {
		if err == redis.Nil {
			return 0, nil
		}
		return 0, err
	}
	// log.Printf("ranking = %d, err = %v", ranking, err)
	return ranking + 1, err
}

func (this *RankList) GetList(start int, count int) ([]redis.Z, error) {
	key := this.key
	end := start + count - 1
	if start > end {
		return nil, fmt.Errorf("invalid params: start(%d) > end(%d)", start, end)
	}

	var _list []redis.Z
	var err error
	c := context.TODO()
	if this.Order == OrderingDesc {
		_list, err = this.redis.ZRevRangeWithScores(c, key, int64(start), int64(end)).Result()
	} else {
		_list, err = this.redis.ZRangeWithScores(c, key, int64(start), int64(end)).Result()
	}
	if err != nil {
		if err == redis.Nil {
			err = nil
		}
		return nil, err
	}
	if this.enc != nil {
		this._decodeScores(_list)
	}
	return _list, nil
}

func (this *RankList) Size() (int64, error) {
	key := this.key
	c := context.TODO()
	size, err := this.redis.ZCard(c, key).Result()
	if err == redis.Nil {
		return 0, nil
	}
	return size, err
}

func (this *RankList) _decodeScores(items []redis.Z) {
	for i, item := range items {
		items[i].Score = float64(this.enc.Decode(int64(item.Score)))
	}
}

func (this *RankList) Set(member string, score float64, factor int32) (int64, error) {
	if this.enc != nil {
		if score > math.MaxInt32 {
			slog.Error("[redisobj.RankList] Set: score too large", "score", score, "factor", factor)
			return 0, fmt.Errorf("score too large: %f", score)
		}
		score = float64(this.enc.Encode(int32(score), factor))
	}
	key := this.key
	m := redis.Z{
		Score:  score,
		Member: member,
	}
	c := context.TODO()
	return this.redis.ZAdd(c, key, m).Result()
}

func (this *RankList) GetScore(member string) (float64, error) {
	key := this.key
	c := context.TODO()
	score, err := this.redis.ZScore(c, key, member).Result()
	if err != nil {
		if err == redis.Nil {
			return 0, nil
		}
		return 0, nil
	}

	if this.enc != nil {
		score = float64(this.enc.Decode(int64(score)))
	}
	return score, err
}

func (this *RankList) GetScoreByRanking(ranking int) (float64, error) {
	items, err := this.GetList(ranking, 1)
	if err != nil {
		return 0, err
	}
	if len(items) <= 0 {
		return 0, nil
	}
	score := items[0].Score
	if this.enc != nil {
		score = float64(this.enc.Decode(int64(score)))
	}
	return score, nil
}

func (this *RankList) LimitIf() (int64, error) {
	if this.MaxMembers <= 0 {
		// unlimit
		return 0, nil
	}
	outOfRanking := this.MaxMembers + 1
	return this.DeleteByRanking(outOfRanking, 3)
}

func (this *RankList) GetTop(count int) ([]redis.Z, error) {
	return this.GetList(0, count)
}

func (this *RankList) Clear() error {
	key := this.key
	c := context.TODO()
	this.redis.Unlink(c, key).Result()
	return nil
}

func (this *RankList) SetMaxSize(maxSize int) {
	this.MaxMembers = maxSize
}

func (this *RankList) Delete(member string) (bool, error) {
	key := this.key
	c := context.TODO()
	delCount, err := this.redis.ZRem(c, key, member).Result()
	return delCount > 0, err
}

func (this *RankList) DeleteByRanking(ranking int, count int) (int64, error) {
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
	if this.Order == OrderingAsc {
		start = int64(ranking - 1)
		end = int64((ranking - 1) + (count - 1))
	} else {
		start = int64((-ranking) - (count - 1))
		end = int64(-ranking)
	}
	key := this.key
	c := context.TODO()
	delCount, err := this.redis.ZRemRangeByRank(c, key, start, end).Result()
	if err == redis.Nil {
		err = nil
	}
	return delCount, err
}

func (this *RankList) Clone() *RankList {
	cloned := *this
	if &cloned == this {
		panic(fmt.Errorf("ranklist.Clone: clone failed"))
	}
	return &cloned
}

func (this *RankList) BaseKey() string {
	return this.baseKey
}

func (this *RankList) FullKey() string {
	return this.key
}
