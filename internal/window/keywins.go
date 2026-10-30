package window

// Open tumbling/sliding windows grouped by key. A key rarely has more than a
// few open at once, so a linear scan of its slots beats hashing (key, start)
// together, and Add hashes the key once however many windows an event lands in.
type slot struct {
	start int64
	a     *acc
}

type keyWins struct {
	key string
	w   []slot
}

func (ks *keyWins) find(start int64) *acc {
	for i := range ks.w {
		if ks.w[i].start == start {
			return ks.w[i].a
		}
	}
	return nil
}

func (ks *keyWins) remove(start int64) bool {
	for i := range ks.w {
		if ks.w[i].start == start {
			last := len(ks.w) - 1
			ks.w[i] = ks.w[last]
			ks.w[last] = slot{}
			ks.w = ks.w[:last]
			return true
		}
	}
	return false
}

// what the due buckets hold: enough to reach the window without any hashing
type ref struct {
	ks    *keyWins
	start int64
}

func (o *Operator) keyFor(key string) *keyWins {
	ks := o.keys[key]
	if ks == nil {
		ks = &keyWins{key: key}
		o.keys[key] = ks
	}
	return ks
}

// open creates (or, on restore, replaces) the window and returns it
func (o *Operator) put(ks *keyWins, start int64, a *acc) {
	for i := range ks.w {
		if ks.w[i].start == start {
			ks.w[i].a = a
			return
		}
	}
	ks.w = append(ks.w, slot{start, a})
	o.nwins++
}

func (o *Operator) drop1(ks *keyWins, start int64) {
	if ks.remove(start) {
		o.nwins--
	}
	if len(ks.w) == 0 && o.keys[ks.key] == ks {
		delete(o.keys, ks.key)
	}
}

// every open window, for snapshots and tests
func (o *Operator) each(fn func(key string, start int64, a *acc)) {
	for _, ks := range o.keys {
		for _, s := range ks.w {
			fn(ks.key, s.start, s.a)
		}
	}
}
