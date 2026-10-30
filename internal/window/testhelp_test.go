package window

// all open windows by (key, start), copied out of the per-key sets
func (o *Operator) allWins() map[wkey]acc {
	out := map[wkey]acc{}
	o.each(func(key string, start int64, a *acc) { out[wkey{key, start}] = *a })
	return out
}
