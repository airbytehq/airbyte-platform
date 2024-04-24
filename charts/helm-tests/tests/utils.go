package test

type set[K comparable] map[K]struct{}

func toStringSet(keys ...string) set[string] {
	s := make(set[string])
	for _, k := range keys {
		s[k] = struct{}{}
	}

	return s
}

func (s set[string]) keys() []string {
	var ks []string
	for k := range s {
		ks = append(ks, k)
	}

	return ks
}

func (s set[string]) contains(k string) bool {
	_, ok := s[k]
	return ok
}

func (s set[string]) union(other set[string]) set[string] {
	ss := make(set[string])

	for k := range s {
		ss[k] = struct{}{}
	}

	for k := range other {
		ss[k] = struct{}{}
	}

	return ss
}
