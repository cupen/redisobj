package redisobj

import (
	"fmt"
	"testing"

	"github.com/redis/go-redis/v9"

	"github.com/stretchr/testify/assert"
)

func newTestObj(name string, orderName string) *RankList {
	url := "redis://127.0.0.1:6379/0"
	opt, _ := redis.ParseURL(url)
	client := redis.NewClient(opt)
	obj := NewRankList(name, client, orderName)
	return &obj
}

func TestRankList_MaxMemvers(t *testing.T) {
	assert := assert.New(t)
	rank := newTestObj("prefiex_test", "asc")
	defer rank.ClearX()

	// 从小到大
	rank.MaxMembers = 3
	data := map[string]interface{}{}
	rank.SetX("id1", 1, data)
	rank.SetX("id2", 2, data)
	rank.SetX("id3", 3, data)
	rank.SetX("id4", 4, data)
	rank.SetX("id5", 5, data)
	members, err := rank.GetXTop(10)
	assert.NoError(err)
	assert.Equal(3, len(*members))
	assert.Equal("id1", (*members)[0].Member)
	assert.Equal("id2", (*members)[1].Member)
	assert.Equal("id3", (*members)[2].Member)

	// 从大到小
	rank = newTestObj("prefix_test", "desc")
	rank.MaxMembers = 3
	data = map[string]interface{}{}
	rank.SetX("id1", 1, data)
	rank.SetX("id2", 2, data)
	rank.SetX("id3", 3, data)
	rank.SetX("id4", 4, data)
	rank.SetX("id5", 5, data)
	members, err = rank.GetXTop(10)
	assert.NoError(err)
	assert.Equal(3, len(*members))

	assert.Equal("id5", (*members)[0].Member)
	assert.Equal("id4", (*members)[1].Member)
	assert.Equal("id3", (*members)[2].Member)
}

func TestRankList_WithID(t *testing.T) {
	assert := assert.New(t)
	rank := newTestObj("prefiex_test", "asc")

	// 从小到大
	newRank := rank.WithID("suffix_id")
	assert.NotEqual(rank, newRank)
}

func TestRankList_Del(t *testing.T) {
	assert := assert.New(t)

	rank := newTestObj("prefiex_test_del", "asc")
	defer rank.ClearX()

	user1 := &RankItem{Member: "user1", Score: 1, Data: Dict{"a": 1.0}}
	user2 := &RankItem{Member: "user2", Score: 2, Data: nil}
	rank.SetX("user1", 1, user1.Data)
	rank.SetX("user2", 2, nil)
	items, err := rank.GetXTop(10)
	assert.NoError(err)
	assert.Equal(2, len(*items))
	assert.Equal(user1, (*items)[0])
	assert.Equal(user2, (*items)[1])

	rank.DelX(user1.Member)
	items, err = rank.GetXTop(10)
	assert.NoError(err)
	assert.Equal(1, len(*items))
	assert.Equal(user2, (*items)[0])
}

func TestRankList_GetScoreByRanking(t *testing.T) {
	rank := newTestObj("prefiex_test_del", "asc")
	defer rank.ClearX()

	// user1 := &RankItem{Member: "user1", Score: 1, Data: Dict{"a": 1.0}}
	// user2 := &RankItem{Member: "user2", Score: 2, Data: nil}
	for i := 0; i < 100; i++ {
		userId := fmt.Sprintf("user%d", i)
		rank.SetX(userId, int64(i), nil)
	}
	t.Run("in-range", func(t *testing.T) {
		assert := assert.New(t)
		for i := 0; i < 100; i++ {
			score, err := rank.GetScoreByRanking(i)
			actualScore := i
			assert.NoError(err)
			assert.Equal(int64(actualScore), score)
		}
		rank.Order = OrderingDesc
		for i := 0; i < 100; i++ {
			score, err := rank.GetScoreByRanking(i)
			actualScore := (100 - i - 1)
			assert.NoError(err)
			assert.Equal(int64(actualScore), score)
		}
	})

	t.Run("out-range", func(t *testing.T) {
		assert := assert.New(t)

		score, err := rank.GetScoreByRanking(1000)
		assert.NoError(err)
		assert.Equal(int64(0), score)
	})

}

func TestRankList_DelByRanking(t *testing.T) {
	newRank := func(size int, orderName string) *RankList {
		rank := newTestObj("prefiex_test_del", orderName)
		rank.ClearX()
		for i := 1; i <= size; i++ {
			userId := fmt.Sprintf("user%d", i)
			rank.SetX(userId, int64(i), nil)
		}
		return rank
	}

	// user1 := &RankItem{Member: "user1", Score: 1, Data: Dict{"a": 1.0}}
	// user2 := &RankItem{Member: "user2", Score: 2, Data: nil}

	t.Run("asc", func(t *testing.T) {
		assert := assert.New(t)
		rank := newRank(100, "asc")
		defer rank.ClearX()
		s, err := rank.Size()
		assert.NoError(err)
		assert.Equal(100, s)

		top100, err := rank.GetXTopV2(100)
		assert.NoError(err)
		assert.Equal(100, len(top100))

		c, err := rank.DelByRanking(50, 1)
		assert.NoError(err)
		assert.Equal(int64(1), c)

		top99, err := rank.GetXTopV2(100)
		assert.NoError(err)
		assert.Equal(99, len(top99))

		{
			assert.Equal(49, len(top100[0:49]))
			assert.Equal(50, len(top100[50:]))
			assert.Equal(int64(50), top100[0:50][49].Score)
		}
		top100fix := append(top100[0:49], top100[50:]...)
		assert.Equal(top100fix, top99)
	})

	t.Run("desc", func(t *testing.T) {
		assert := assert.New(t)
		rank := newRank(100, "desc")
		defer rank.ClearX()
		s, err := rank.Size()
		assert.NoError(err)
		assert.Equal(100, s)

		top100, err := rank.GetXTopV2(100)
		assert.NoError(err)
		assert.Equal(100, len(top100))

		c, err := rank.DelByRanking(50, 1)
		assert.NoError(err)
		assert.Equal(int64(1), c)

		top99, err := rank.GetXTopV2(100)
		assert.NoError(err)
		assert.Equal(99, len(top99))

		{
			assert.Equal(49, len(top100[0:49]))
			assert.Equal(50, len(top100[50:]))
		}
		top100fix := append(top100[0:49], top100[50:]...)
		assert.Equal(top100fix, top99)
	})
}
