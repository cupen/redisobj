package encoders

import (
	"math"
	"testing"
)

func TestFirstInIsBigger(t *testing.T) {
	enc := FirstInIsBigger

	t.Run("Encode", func(t *testing.T) {
		tests := []struct {
			score  int32
			factor int32
			want   int64
		}{
			{100, 50, enc.Encode(100, 50)},
			{200, 100, enc.Encode(200, math.MaxInt32-100)},
			{300, 150, enc.Encode(300, math.MaxInt32-150)},
		}

		for _, tt := range tests {
			t.Run("", func(t *testing.T) {
				if got := enc.Encode(tt.score, tt.factor); got != tt.want {
					t.Errorf("Encode() = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("Decode", func(t *testing.T) {
		tests := []struct {
			score int64
			want  int32
		}{
			{enc.Encode(100, math.MaxInt32-50), 100},
			{enc.Encode(200, math.MaxInt32-100), 200},
			{enc.Encode(300, math.MaxInt32-150), 300},
		}

		for _, tt := range tests {
			t.Run("", func(t *testing.T) {
				if got := enc.Decode(tt.score); got != tt.want {
					t.Errorf("Decode() = %v, want %v", got, tt.want)
				}
			})
		}
	})
}
