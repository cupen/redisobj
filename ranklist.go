package redisobj

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/go-redis/redis"
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
		panic(fmt.Errorf("Empty baseKey for ranklist"))
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

func (this RankList) WithID(rankId string) RankList {
	cloneObj := this
	cloneObj.SetID(rankId)
	return cloneObj
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
			panic(fmt.Errorf("Invalid Order: \"%s\"", ordering))
		}
	}
	this.Order = _parse(orderName)
}

func (this *RankList) GetRanking(member string) (int64, error) {
	key := this.key
	var ranking int64 = 0
	var err error
	if this.Order == OrderingDesc {
		ranking, err = this.Redis.ZRevRank(key, member).Result()
	} else {
		ranking, err = this.Redis.ZRank(key, member).Result()
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
		_val, ok := z.Member.(string)
		if !ok {
			// error
		}
		rs[i] = _val
	}
	return rs, nil
}

func (this *RankList) GetList(start int, count int) ([]redis.Z, error) {
	key := this.key
	end := start + count - 1
	if start > end {
		return nil, fmt.Errorf("Invalid params start(%d) > end(%d)", start, end)
	}

	if this.Order == OrderingDesc {
		list, err := this.Redis.ZRevRangeWithScores(key, int64(start), int64(end)).Result()
		return list, err
	}
	list, err := this.Redis.ZRangeWithScores(key, int64(start), int64(end)).Result()
	return list, err
}

func (this *RankList) Size() (int, error) {
	key := this.key
	size, err := this.Redis.ZCard(key).Result()
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
	return this.Redis.ZAddCh(key, m).Result()
}

func (this *RankList) GetScore(member string) (int64, error) {
	key := this.key
	score, err := this.Redis.ZScore(key, member).Result()
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
	_, err := this.Redis.ZAddCh(key, m).Result()
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
	flag, err := this.Redis.HSet(keyOfMemer, member, string(dataBytes)).Result()
	return flag, err
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
	keyOfMemer := strings.Join([]string{key, "data"}, ":")
	score, err := this.Redis.ZScore(key, member).Result()
	if err == redis.Nil {
		return nil, nil
	}

	data := Dict{}
	if dataText, err := this.Redis.HGet(keyOfMemer, member).Result(); err == nil {
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
	keyOfData := strings.Join([]string{this.key, "data"}, ":")
	_, err := this.Redis.Del(keyOfData).Result()
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
	this.Redis.Del(key).Result()

	keyOfData := strings.Join([]string{this.key, "data"}, ":")
	this.Redis.Del(keyOfData).Result()
	return nil
}

func (this *RankList) Del(member string) (bool, error) {
	key := this.key
	delCount, err := this.Redis.ZRem(key, member).Result()
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
	// log.Printf("key=%s start=%d end=%d\n", key, start, end)
	delCount, err := this.Redis.ZRemRangeByRank(key, start, end).Result()
	if err == redis.Nil {
		err = nil
	}
	return delCount, err
}
