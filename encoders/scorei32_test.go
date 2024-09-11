package encoders

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScoreI32(t *testing.T) {
	// 生成测试用例

	obj := &ScoreI32{}
	t.Run("Encode", func(t *testing.T) {
		assert.Equal(t, int64(0), obj.Encode(-1, 1))
		assert.Equal(t, int64(0), obj.Encode(0, 1))
		assert.Equal(t, int64(2147483650), obj.Encode(1, 2))
		assert.Equal(t, int64(2147483649), obj.Encode(1, 1))
		assert.Equal(t, int64(2147483650), obj.Encode(1, 2))
	})

	t.Run("Decode", func(t *testing.T) {
		require.Equal(t, int32(0), obj.Decode(obj.Encode(-1, 1)))
		require.Equal(t, int32(0), obj.Decode(obj.Encode(0, 1)))

		require.Equal(t, int32(2147483647), obj.Decode(obj.Encode(1<<31-1, 1)))
		require.Equal(t, int32(2147483647), obj.Decode(obj.Encode(1<<31-1, math.MaxInt32)))
		for i := 1; i < 31; i++ {
			now := time.Now()
			v := int32(1<<i - 1)
			for j := v; j < v+100; j++ {
				require.Equalf(t, j, obj.Decode(obj.Encode(j, int32(now.Unix()))), "i=%v j=%v", i, j)
				require.Equalf(t, j, obj.Decode(obj.Encode(j, int32(math.MaxInt32))), "i=%v j=%v", i, j)
			}
		}
	})
}
