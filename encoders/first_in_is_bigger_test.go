package encoders

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFirstInIsBigger(t *testing.T) {
	enc := FirstInIsBigger

	t.Run("Encode/Decode", func(t *testing.T) {
		now := time.Now()
		ts := int32(now.Unix())
		tests := []struct {
			score  int32
			factor int32
			isSafe bool
		}{
			{1, ts, true},
			{2, ts, true},
			{3, ts, true},

			{1000, ts, true},
			{2000, ts, true},
			{3000, ts, true},

			{1<<21 - 3, ts, true},
			{1<<21 - 2, ts, true},
			{1<<21 - 1, ts, true},

			{math.MaxInt32 - 3, ts, false},
			{math.MaxInt32 - 2, ts, false},
			{math.MaxInt32 - 1, ts, false},
			{math.MaxInt32, ts, false},
		}

		for i, tt := range tests {
			name := fmt.Sprintf("val=%d", tt.score)
			t.Run(name, func(t *testing.T) {
				got := float64(enc.Encode(tt.score, tt.factor+int32(i)))
				if tt.isSafe {
					assert.Less(t, got, float64(1<<53-1), "last value should be less than MaxInt32")
				}
				newVal := enc.Decode(int64(got))
				assert.Equal(t, tt.score, newVal, "score should be the same")
			})
		}
	})

}
