package encoders

type lastInIsBigger struct {
	s *scoreI32
}

func newLastInIsBigger() *lastInIsBigger {
	return &lastInIsBigger{s: &scoreI32{}}
}

func (se *lastInIsBigger) Encode(score int32, factor int32) int64 {
	return se.s.Encode(score, factor)
}

func (se *lastInIsBigger) Decode(score int64) int32 {
	return se.s.Decode(score)
}
