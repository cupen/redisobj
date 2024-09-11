package encoders

type Score interface {
	Encode(score int32, factor int32) int64
	Decode(score int64) int32
}
