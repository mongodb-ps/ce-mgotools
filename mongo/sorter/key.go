package sorter

// A custom sorting algorithm to keep keys starting with _ before $, and $
// before everything else.
type Key []string

func (k Key) Len() int {
	return len(k)
}

func (k Key) Less(i, j int) bool {
	a := k[i]
	b := k[j]
	la, lb := len(a), len(b)
	if la == 0 || lb == 0 {
		if a < b {
			return true
		} else {
			return false
		}
	}
	if (a[0] == '_' && b[0] != '_') || (a[0] == '$' && b[0] != '$') {
		return true
	} else if (a[0] != '_' && b[0] == '_') || (a[0] != '$' && b[0] == '$') {
		return false
	} else if (a[0] == '_' && b[0] == '_') || (a[0] == '$' && b[0] == '$') {
		if a[1:] < b[1:] {
			return true
		} else {
			return false
		}
	} else if a < b {
		return true
	}
	return false
}

func (k Key) Swap(i, j int) {
	c := k[i]
	k[i] = k[j]
	k[j] = c
}
