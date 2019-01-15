package sorter

import "sort"

// A custom sort algorithm for logical operators so an array of objects
// gets sorted by the keys of each sub-object.
type Logical []map[string]interface{}

func Patternize(a []interface{}) Logical {
	b := make([]map[string]interface{}, len(a))
	for i := 0; i < len(a); i += 1 {
		b[i] = a[i].(map[string]interface{})
	}
	return b
}

func (p Logical) Interface() []interface{} {
	b := make([]interface{}, len(p))
	for i := 0; i < len(b); i += 1 {
		b[i] = func() map[string]interface{} { return p[i] }()
	}
	return b
}

func (p Logical) Len() int {
	return len(p)
}

func (p Logical) Less(i, j int) bool {
	a := make(Key, 0, len(p[i]))
	b := make(Key, 0, len(p[j]))

	for k := range p[i] {
		a = append(a, k)
	}
	for k := range p[j] {
		b = append(b, k)
	}

	sort.Sort(a)
	sort.Sort(b)
	for i := 0; i < len(a) && i < len(b); i += 1 {
		if a[i] < b[i] {
			return true
		} else if a[i] > b[i] {
			return false
		}
	}

	// At this point, all keys are identical so the shorter wins.
	if len(a) < len(b) {
		return true
	} else {
		return false
	}
}

func (p Logical) Swap(i, j int) {
	c := p[i]
	p[i] = p[j]
	p[j] = c
}
