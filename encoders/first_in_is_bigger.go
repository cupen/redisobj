package encoders

import "math"

type firstInIsBigger struct {
	s *scoreI32
}

func newFirstInIsBigger() *firstInIsBigger {
	return &firstInIsBigger{s: &scoreI32{}}
}

func (se *firstInIsBigger) Encode(score int32, factor int32) int64 {
	_factor := math.MaxInt32 - factor
	if _factor < 0 {
		_factor = 0
	}
	return se.s.Encode(score, _factor)
}

func (se *firstInIsBigger) Decode(score int64) int32 {
	return se.s.Decode(score)
}
