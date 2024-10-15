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

	obj := &scoreI32{}
	t.Run("Encode", func(t *testing.T) {
		assert.Equal(t, int64(0), obj.Encode(-1, 1))
		assert.Equal(t, int64(0), obj.Encode(0, 1))
		assert.Equal(t, int64(2147483650), obj.Encode(1, 2))
		assert.Equal(t, int64(2147483649), obj.Encode(1, 1))
		assert.Equal(t, int64(2147483650), obj.Encode(1, 2))
	})

	t.Run("EncodeFloat64", func(t *testing.T) {
		now := time.Now()
		assert.Equal(t, int64(0), obj.Encode(-1, 1))
		assert.Equal(t, int64(0), obj.Encode(0, 1))

		t.Run("score32-min", func(t *testing.T) {
			for i := int32(1); i < 1000; i++ {
				f1 := obj.Encode(i, int32(now.Unix()))
				f2 := obj.Encode(i, int32(now.Add(1*time.Second).Unix()))
				assert.True(t, f1 < f2)

				d1 := obj.Decode(f1)
				d2 := obj.Decode(f2)
				assert.NotEqual(t, int32(0), d1)
				assert.Equal(t, d1, d2)
			}
		})

		t.Run("score32-max", func(t *testing.T) {
			for i := int32(math.MaxInt32 - 1000); i < math.MaxInt32; i++ {
				f1 := obj.Encode(i, int32(now.Unix()))
				f2 := obj.Encode(i, int32(now.Add(1*time.Second).Unix()))
				assert.True(t, f1 < f2)

				d1 := obj.Decode(f1)
				d2 := obj.Decode(f2)
				assert.NotEqual(t, int32(0), d1)
				assert.Equal(t, d1, d2)
			}
		})
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
