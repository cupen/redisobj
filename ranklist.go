package redisobj

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

type Dict map[string]interface{}

// const OrderingAsc = 0
// const OrderingDesc = 1

type RankItem struct {
	Member string
	Score  int64
	Data   Dict
}

type RankList struct {
	Redis   *redis.Client
	baseKey string
	key     string

	MaxMembers int // 暂时不用
	Order      int // 0-从高到低，1-从低到高
}

func NewRankList(baseKey string, redis *redis.Client, orderName string) RankList {
	if baseKey == "" {
		panic(fmt.Errorf("empty baseKey for ranklist"))
	}
	rank := RankList{
		Redis:      redis,
		baseKey:    baseKey,
		key:        strings.Join([]string{baseKey, "default"}, ":"),
		MaxMembers: 0,
	}
	rank.SetOrdering(orderName)
	return rank
}

func (this *RankList) SetID(rankId string) {
	if this.baseKey == "" {
		panic(errors.New("baseKey was empty"))
	}
	this.key = strings.Join([]string{this.baseKey, rankId}, ":")
}

func (this *RankList) WithID(rankId string) *RankList {
	cloneObj := *this
	if &cloneObj == this {
		panic(fmt.Errorf("rankList.WithID: clone failed"))
	}
	cloneObj.SetID(rankId)
	return &cloneObj
}

// OrderingAsc or OrderingDesc
func (this *RankList) SetOrdering(orderName string) {
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
	this.Order = _parse(orderName)
}

func (this *RankList) GetRanking(member string) (int64, error) {
	key := this.key
	var ranking int64 = 0
	var err error
	c := context.TODO()
	if this.Order == OrderingDesc {
		ranking, err = this.Redis.ZRevRank(c, key, member).Result()
	} else {
		ranking, err = this.Redis.ZRank(c, key, member).Result()
	}
	// fmt.Printf("ranking = %d, err = %v", ranking, err)
	if err == nil {
		ranking += 1
	}
	return ranking, err
}

func (this *RankList) GetKeys(start int, count int) ([]string, error) {
	_list, err := this.GetList(start, count)
	if err != nil {
		if err == redis.Nil {
			err = nil
		}
		return nil, err
	}
	rs := make([]string, len(_list))
	for i, z := range _list {
		rs[i] = z.Member.(string)
	}
	return rs, nil
}

func (this *RankList) GetList(start int, count int) ([]redis.Z, error) {
	key := this.key
	end := start + count - 1
	if start > end {
		return nil, fmt.Errorf("Invalid params start(%d) > end(%d)", start, end)
	}

	c := context.TODO()
	if this.Order == OrderingDesc {
		list, err := this.Redis.ZRevRangeWithScores(c, key, int64(start), int64(end)).Result()
		return list, err
	}
	list, err := this.Redis.ZRangeWithScores(c, key, int64(start), int64(end)).Result()
	return list, err
}

func (this *RankList) Size() (int, error) {
	key := this.key
	c := context.TODO()
	size, err := this.Redis.ZCard(c, key).Result()
	if err == redis.Nil {
		return 0, nil
	}
	return int(size), err
}

func (this *RankList) Set(member string, factor int64) (int64, error) {
	key := this.key
	m := redis.Z{
		Score:  float64(factor),
		Member: member,
	}
	c := context.TODO()
	return this.Redis.ZAdd(c, key, m).Result()
}

func (this *RankList) GetScore(member string) (int64, error) {
	key := this.key
	c := context.TODO()
	score, err := this.Redis.ZScore(c, key, member).Result()
	if err == redis.Nil {
		return 0, nil
	}
	return int64(score), err
}

func (this *RankList) GetScoreByRanking(ranking int) (int64, error) {
	items, err := this.GetList(ranking, 1)
	if err != nil {
		return 0, err
	}
	if len(items) <= 0 {
		return 0, nil
	}
	return int64(items[0].Score), nil
}

func (this *RankList) SetX(member string, factor int64, data interface{}) (bool, error) {
	key := this.key
	m := redis.Z{
		Score:  float64(factor),
		Member: member,
	}
	c := context.TODO()
	_, err := this.Redis.ZAdd(c, key, m).Result()
	if err != nil {
		return false, err
	}

	if this.MaxMembers > 0 {
		defer func() {
			this.LimitIf()
		}()
	}

	// set 数据
	if data == nil {
		return true, nil
	}

	dataBytes, err := json.Marshal(data)
	if err != nil {
		return false, err
	}

	keyOfMemer := strings.Join([]string{key, "data"}, ":")
	flag, err := this.Redis.HSet(c, keyOfMemer, member, string(dataBytes)).Result()
	return flag > 0, err
}

func (this *RankList) LimitIf() (int64, error) {
	if this.MaxMembers <= 0 {
		// unlimit
		return 0, nil
	}
	outOfRanking := this.MaxMembers + 1
	return this.DelByRanking(outOfRanking, 3)
}

// 有条件的 set
func (this *RankList) SetXIf(member string, score int64, data interface{}) (bool, error) {
	if score <= 0 {
		return false, nil
	}

	oldScore, err := this.GetScore(member)
	if err != nil {
		return false, err
	}

	// 根据排序顺序，决定是留高分还还是低分
	if this.Order == OrderingDesc && score <= oldScore {
		return false, err
	}

	if this.Order == OrderingAsc && oldScore > 0 && oldScore <= score {
		return false, err
	}
	return this.SetX(member, score, data)
}

func (this *RankList) GetX(member string) (*RankItem, error) {
	key := this.key
	c := context.TODO()
	keyOfMemer := strings.Join([]string{key, "data"}, ":")
	score, err := this.Redis.ZScore(c, key, member).Result()
	if err == redis.Nil {
		return nil, nil
	}

	data := Dict{}
	if dataText, err := this.Redis.HGet(c, keyOfMemer, member).Result(); err == nil {
		err := json.Unmarshal([]byte(dataText), &data)
		if err != nil {
			data = nil
		}
	} else {
		data = nil
	}

	// fmt.Printf("ranklist.GetX  data=%#v\n", data)
	item := RankItem{
		Member: member,
		Score:  int64(score),
		Data:   data,
	}
	return &item, err
}

func (this *RankList) DelX(member string) error {
	this.Del(member)
	c := context.TODO()
	keyOfData := strings.Join([]string{this.key, "data"}, ":")
	_, err := this.Redis.Del(c, keyOfData).Result()
	if err == redis.Nil {
		return nil
	}
	return err
}

func (this *RankList) GetXTop(count int) (*[]*RankItem, error) {
	return this.GetXList(0, count)
}

func (this *RankList) GetXTopV2(count int) ([]*RankItem, error) {
	_items, err := this.GetXList(0, count)
	if _items == nil {
		return nil, err
	}
	return *_items, err
}

func (this *RankList) GetXList(start int, count int) (*[]*RankItem, error) {
	list, err := this.GetList(start, count)
	if err != nil {
		return nil, err
	}
	rs := []*RankItem{}
	for _, z := range list {
		item, _ := this.GetX(z.Member.(string))
		if item == nil {
			continue
		}
		rs = append(rs, item)
	}
	return &rs, nil
}

func (this *RankList) GetXListByIds(members *[]string) (*[]*RankItem, error) {
	rs := []*RankItem{}
	for _, member := range *members {
		item, _ := this.GetX(member)
		if item == nil {
			continue
		}
		rs = append(rs, item)
	}
	return &rs, nil
}

func (this *RankList) Clear(magic string) error {
	if magic != "I KNOW WHAT I AM DOING" {
		return errors.New("You don't know what you're doing.")
	}
	return this.ClearX()
}

func (this *RankList) ClearX() error {
	key := this.key
	c := context.TODO()
	this.Redis.Del(c, key).Result()

	keyOfData := strings.Join([]string{this.key, "data"}, ":")
	this.Redis.Del(c, keyOfData).Result()
	return nil
}

func (this *RankList) Del(member string) (bool, error) {
	key := this.key
	c := context.TODO()
	delCount, err := this.Redis.ZRem(c, key, member).Result()
	return delCount > 0, err
}

func (this *RankList) SetMaxSize(maxSize int) {
	this.MaxMembers = maxSize
}

func (this *RankList) DelByRanking(ranking int, count int) (int64, error) {
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
	delCount, err := this.Redis.ZRemRangeByRank(c, key, start, end).Result()
	if err == redis.Nil {
		err = nil
	}
	return delCount, err
}
