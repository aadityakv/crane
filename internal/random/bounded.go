package random

// Uint64n returns a uniformly distributed value in [0, bound). It panics when
// bound is zero.
func Uint64n(source interface{ Uint64() uint64 }, bound uint64) uint64 {
	if bound == 0 {
		panic("random: Uint64n bound is zero")
	}
	if bound&(bound-1) == 0 {
		return source.Uint64() & (bound - 1)
	}
	threshold := -bound % bound
	for {
		value := source.Uint64()
		if value >= threshold {
			return value % bound
		}
	}
}
