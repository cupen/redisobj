package redisobj

import (
	"fmt"
	"math"
	"testing"

	"github.com/cupen/redisobj/encoders"
	"github.com/redis/go-redis/v9"

	"github.com/stretchr/testify/assert"
)

func newTestObj(t *testing.T, name string, orderName string) *RankList {
	url := "redis://127.0.0.1:6379/15"
	opt, _ := redis.ParseURL(url)
	client := redis.NewClient(opt)
	obj := NewRankList(client, name).WithOrdering(orderName)
	t.Cleanup(func() {
		obj.Clear()
	})
	return obj
}

func TestRankList_MaxMemvers(t *testing.T) {
	assert := assert.New(t)
	rank := newTestObj(t, "prefiex_test", "asc")

	// 从小到大
	rank.Set("id1", 1, 0)
	rank.Set("id2", 2, 0)
	rank.Set("id3", 3, 0)
	rank.Set("id4", 4, 0)
	rank.Set("id5", 5, 0)
	rank.MaxMembers = 3
	rank.LimitIf()

	members, err := rank.GetTop(10)
	assert.NoError(err)
	assert.Equal(3, len(members))
	assert.Equal("id1", (members)[0].Member)
	assert.Equal("id2", (members)[1].Member)
	assert.Equal("id3", (members)[2].Member)

	// 从大到小
	rank = newTestObj(t, "prefix_test", "desc")
	rank.MaxMembers = 3
	rank.Set("id1", 1, 0)
	rank.Set("id2", 2, 0)
	rank.Set("id3", 3, 0)
	rank.Set("id4", 4, 0)
	rank.Set("id5", 5, 0)
	rank.LimitIf()
	members, err = rank.GetTop(10)
	assert.NoError(err)
	assert.Equal(3, len(members))

	assert.Equal("id5", (members)[0].Member)
	assert.Equal("id4", (members)[1].Member)
	assert.Equal("id3", (members)[2].Member)
}

func TestRankList_WithID(t *testing.T) {
	assert := assert.New(t)
	rank := newTestObj(t, "prefiex_test", "asc")

	// 从小到大
	newRank := rank.WithID("suffix_id")
	assert.NotEqual(rank, newRank)
}

func TestRankList_Delete(t *testing.T) {
	assert := assert.New(t)

	rank := newTestObj(t, "prefiex_test_del", "asc")
	rank.Clear()
	t.Cleanup(func() {
		rank.Clear()
	})

	user1 := redis.Z{Member: "user1", Score: 1}
	user2 := redis.Z{Member: "user2", Score: 2}
	rank.Set("user1", 1, 0)
	rank.Set("user2", 2, 0)
	items, err := rank.GetTop(10)
	assert.NoError(err)
	assert.Equal(2, len(items))
	assert.Equal(user1, (items)[0])
	assert.Equal(user2, (items)[1])

	rank.Delete(user1.Member.(string))
	items, err = rank.GetTop(10)
	assert.NoError(err)
	assert.Equal(1, len(items))
	assert.Equal(user2, (items)[0])
}

func TestRankList_GetScoreByRanking(t *testing.T) {
	rank := newTestObj(t, "prefiex_test_del", "asc")
	defer rank.Clear()

	// user1 := &RankItem{Member: "user1", Score: 1, Data: Dict{"a": 1.0}}
	// user2 := &RankItem{Member: "user2", Score: 2, Data: nil}
	for i := 0; i < 100; i++ {
		userId := fmt.Sprintf("user%d", i)
		rank.Set(userId, float64(i), 0)
	}
	t.Run("in-range", func(t *testing.T) {
		assert := assert.New(t)
		for i := 0; i < 100; i++ {
			score, err := rank.GetScoreByRanking(i)
			actualScore := i
			assert.NoError(err)
			assert.Equal(float64(actualScore), score)
		}
		rank.Order = OrderingDesc
		for i := 0; i < 100; i++ {
			score, err := rank.GetScoreByRanking(i)
			actualScore := (100 - i - 1)
			assert.NoError(err)
			assert.Equal(float64(actualScore), score)
		}
	})

	t.Run("out-range", func(t *testing.T) {
		assert := assert.New(t)

		score, err := rank.GetScoreByRanking(1000)
		assert.NoError(err)
		assert.Equal(float64(0), score)
	})
}

func TestRankList_DeleteByRanking(t *testing.T) {
	newRank := func(size int, orderName string) *RankList {
		rank := newTestObj(t, "prefiex_test_del", orderName)
		rank.Clear()
		for i := 1; i <= size; i++ {
			userId := fmt.Sprintf("user%d", i)
			rank.Set(userId, float64(i), 0)
		}
		return rank
	}

	// user1 := &RankItem{Member: "user1", Score: 1, Data: Dict{"a": 1.0}}
	// user2 := &RankItem{Member: "user2", Score: 2, Data: nil}

	t.Run("asc", func(t *testing.T) {
		assert := assert.New(t)
		rank := newRank(100, "asc")
		defer rank.Clear()
		s, err := rank.Size()
		assert.NoError(err)
		assert.Equal(100, int(s))

		top100, err := rank.GetTop(100)
		assert.NoError(err)
		assert.Equal(100, len(top100))

		c, err := rank.DeleteByRanking(50, 1)
		assert.NoError(err)
		assert.Equal(int64(1), c)

		top99, err := rank.GetTop(100)
		assert.NoError(err)
		assert.Equal(99, len(top99))

		{
			assert.Equal(49, len(top100[0:49]))
			assert.Equal(50, len(top100[50:]))
			assert.Equal(int(50), int(top100[0:50][49].Score))
		}
		top100fix := append(top100[0:49], top100[50:]...)
		assert.Equal(top100fix, top99)
	})

	t.Run("desc", func(t *testing.T) {
		assert := assert.New(t)
		rank := newRank(100, "desc")
		defer rank.Clear()
		s, err := rank.Size()
		assert.NoError(err)
		assert.Equal(100, int(s))

		top100, err := rank.GetTop(100)
		assert.NoError(err)
		assert.Equal(100, len(top100))

		c, err := rank.DeleteByRanking(50, 1)
		assert.NoError(err)
		assert.Equal(int64(1), c)

		top99, err := rank.GetTop(100)
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

func TestRankList_WithEncoder(t *testing.T) {
	rank := newTestObj(t, "prefiex_test_WithEncoder", "desc")

	enc := encoders.LastInIsBigger
	rank = rank.WithEncoder(enc)
	factor := int32(math.MaxInt32)
	rank.Set("id1", 1, 0)
	rank.Set("id2", 2, 0)
	rank.Set("id3", 3, 0)
	rank.Set("id4", 4, 0)
	rank.Set("id5", 5, factor)
	rank2 := rank.Clone().WithEncoder(enc)
	rank2.Set("id6", 6, 0)

	score5, err := rank2.GetScore("id5")
	assert.NoError(t, err)
	assert.Equal(t, float64(5), score5)

	score6, err := rank2.GetScore("id6")
	assert.NoError(t, err)
	assert.Equal(t, float64(6), score6)
}

func TestRankList_WithEncoder_Mixed(t *testing.T) {
	rank := newTestObj(t, "prefiex_test_WithEncoder", "desc")

	enc := encoders.LastInIsBigger
	rank = rank.WithEncoder(enc)
	rank.Set("id1", 1, 0)
	rank.Set("id2", 2, 0)
	rank.Set("id3", 3, 0)
	rank.Set("id4", 4, 0)
	rank.Set("id5", 5, 0)
	rank2 := rank.Clone().WithEncoder(enc)
	rank2.Set("id6", 6, 0)

	score5, err := rank2.GetScore("id5")
	assert.NoError(t, err)
	assert.Equal(t, float64(5), score5)

	score6, err := rank2.GetScore("id6")
	assert.NoError(t, err)
	assert.Equal(t, float64(6), score6)

	items, err := rank2.GetTop(10)
	assert.NoError(t, err)
	if assert.Equal(t, 6, len(items)) {
		assert.Equal(t, "id6", items[0].Member)
		assert.Equal(t, "id5", items[1].Member)
		assert.Equal(t, "id4", items[2].Member)
		assert.Equal(t, "id3", items[3].Member)
		assert.Equal(t, "id2", items[4].Member)
		assert.Equal(t, "id1", items[5].Member)

	}

}
