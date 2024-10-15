package encoders

import "math"

type scoreI32 struct{}

func (se *scoreI32) Encode(score int32, factor int32) int64 {
	if score <= 0 {
		return 0
	}
	return int64(score)<<31 | int64(factor)
}

func (se *scoreI32) Decode(score int64) int32 {
	_score := int64(score)
	if score <= math.MaxInt32 {
		return int32(score)
	}
	rs := int32(_score >> 31)
	if rs < 0 {
		return 0
	}
	return rs
}
