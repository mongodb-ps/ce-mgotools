package util

import (
	"fmt"
	"unicode"
)

// The RuneReader type is a helper structure for manually parsing through strings.
// It looks at a string by breaking it down into a set of _runes_. The runes are
// then examined using _start_ and _end_ pointers that move based on the
// method being used.
type RuneReader struct {
	start  int
	next   int
	length int
	runes  []rune
}

// Generates a RuneReader from a string.
func NewRuneReader(a string) *RuneReader {
	if a == "" {
		return nil
	}
	r := []rune(a)
	return &RuneReader{
		runes:  r,
		length: len(r),
		start:  0,
		next:   0,
	}
}

// ChompLeft removes all instances of _a_ from a rune set
// by moving the _start_ pointer to the first right-most position that
// does not match _a_. It is especially useful for removing trailing
// characters like whitespaces.
//
// The _a_ parameter is generic (interface{}) allowing for one of the
// following to be checked: unicode.RangeTable, byte, rune, or []rune.
// Any other type will result in a panic.
func (r *RuneReader) ChompLeft(a interface{}) *RuneReader {
	switch v := a.(type) {
	case *unicode.RangeTable:
		for ; r.next < r.length && unicode.Is(v, r.runes[r.next]); r.next += 1 {
		}
	case byte, rune:
		for ; r.next < r.length && v == r.runes[r.next]; r.next += 1 {
		}
	case []rune:
		if len(v) == 0 {
			panic("Unexpected empty rune array")
		}
		for _, b := range v {
			for ; r.next < r.length && b == r.runes[r.next]; r.next += 1 {
			}
		}
	default:
		panic(fmt.Sprintf("Unhandled type %t", a))
	}
	r.start = r.next
	return r
}

// ChompWS is a special case of ChompLeft where the only character
// being removed is a whitespace. It moves the _end_ pointer until
// a non-space or EOL is encountered. This method is slightly more
// efficient a generic _ChompLeft_ call.
func (r *RuneReader) ChompWS() *RuneReader {
	for ; r.next < r.length && unicode.IsSpace(r.runes[r.next]); r.next += 1 {
	}
	r.start = r.next
	return r
}

// CurrentWord() uses the _start_ and _end_ pointers to return a sub-string
// from the larger rune set. It contains the _start_ and _end_ pointers
// inclusively and exclusively respectively.
func (r *RuneReader) CurrentWord() string {
	if r.start >= r.length {
		return ""
	}
	return runeRange(r, r.start, r.next)
}

// NextRune() returns the next rune to be parsed, i.e. the rune
// pointed to by the _end_ pointer.
func (r *RuneReader) NextRune() rune {
	if r.next >= r.length {
		return -1
	}
	return r.runes[r.next]
}

func (r *RuneReader) EnclosedString(which rune) (string, error) {
	start, length, escaped := r.next, r.length, false
	for end := start + 1; end < length; end++ {
		switch r.runes[end] {
		case 0:
			return "", fmt.Errorf("unexpected null character at %d", end)
		case '\\':
			escaped = true
			continue
		case which:
			if !escaped {
				if length > end+1 && r.runes[end+1] == which {
					// Lookahead to catch cases of double quotes as method of escape (e.g. '""', "''")
					continue
				}
				r.start, r.next = start, end+1
				return string(r.runes[r.start+1: r.next-1]), nil
			}
		}
		escaped = false
	}
	return "", fmt.Errorf("unexpected end of string looking for quote (%c)", which)

}

// EOL() returns whether the _end_ pointer is beyond the last rune
// in the rune set. This is equivalent to checking for the _end_
// pointer being past the end of line.
func (r *RuneReader) EOL() bool {
	return r.next == r.length
}

// Expect(_a_) returns a bool based on whether one or more arguments
// match from the current _end_ pointer. It is most useful for previewing
// the next portion of the rune set that has not yet been examined.
func (r *RuneReader) Expect(a ...interface{}) bool {
	return checkReader(r, a...)
}

func (r *RuneReader) ExpectString(a string) bool {
	if length := StringLength(a); length > 0 &&
		r.next+length <= r.length &&
		a == string(r.runes[r.next:r.next+length]) {
		return true
	}
	return false
}

// Length() returns the total number of runes in the rune set. This may
// differ from the byte length in the original string.
func (r *RuneReader) Length() int {
	return r.length
}

// Next() advances the _end_ pointer without changing the _start_ pointer
// and returns the current value at _end_.
func (r *RuneReader) Next() (rune, bool) {
	if r.length < r.next+1 {
		return 0, false
	}
	r.next += 1
	return r.runes[r.next-1], true
}

// Peek(_length_) returns a string starting from and containing the _next_
// pointer, and includes _length_ runes (including the rune pointed to by
// the _end_ pointer).
func (r *RuneReader) Peek(length int) string {
	if length < 0 {
		if r.next == 0 {
			return ""
		} else if r.next+length < 0 {
			length = -r.next
		}
		return string(r.runes[r.next+length: r.next])
	} else if r.next == r.length {
		return ""
	} else if r.next+length > r.length {
		length = r.length - r.next
	}
	return string(r.runes[r.next: r.next+length])
}

// Pos() returns the current position of the _end_ pointer in the rune set.
func (r *RuneReader) Pos() int {
	return r.next
}

// Prefix(_count_) returns the first _count_ characters from the entire rune
// set. It *does not* reference the _start_ or _end_ pointers. A string
// representing the entire rune set is returned in _count_ is more than the
// length of the rune set.
func (r *RuneReader) Prefix(count int) string {
	if count > r.length {
		count = r.length
	} else if count <= 0 {
		return ""
	}
	return string(r.runes[0:count])
}

// Prev() moved the _end_ pointer backward one position. It returns the
// a rune and bool representing the new character pointed to by _end_ and
// whether the previous call moved successfully. A null rune is returned
// if the _end_ pointer does not move.
func (r *RuneReader) Prev() (rune, bool) {
	if r.next-1 < 0 {
		return 0, false
	}
	r.next -= 1
	return r.runes[r.next], true
}

// PreviewWord(_count_) returns a string with _count_ words beginning
// from the _start_ pointer. It uses unicode.Space to separate words.
func (r *RuneReader) PreviewWord(count int) string {
	var (
		start = r.next
		end   = start
	)
	for words := 0; words < count; words += 1 {
		for end += 1; end < r.length && !unicode.Is(unicode.Space, r.runes[end]); end += 1 {
		}
		end += 1
	}
	trimLeftRunes(r, unicode.Space)
	return runeRange(r, start, end-1)
}

// QuotedString() returns a string between two quote or double quote
// characters (', or "). It begins at the _end_ pointer, requiring that
// a quote or double quote character be referenced at the _end_ position.
// It then advances the _end_ pointer until a matching quote or double quote
// character is found. The returned string is everything between the two
// quote characters, excluding the quote characters.
//
// This method recognizes two types of escaping: slash (\) and repeated
// character ("" or ''). Both escapes are ignored when advancing toward
// the end of the string.
//
// An empty string with a false bool value is returned if a matching end
// quote is not found. A greedy back-reference will not occur on previously
// escaped characters.
func (r *RuneReader) QuotedString() (string, error) {
	which := r.NextRune()
	if which != '\'' && which != '"' {
		return "", fmt.Errorf("Unexpected character in quoted string '%c'", which)
	}
	return r.EnclosedString(which)
}

// Read(_start_, _length_) returns a string representing the rune set from
// _start_ of _length_ runes.
func (r *RuneReader) Read(start int, length int) (string, bool) {
	if start < 0 || start+length > r.length || length < 1 {
		return "", false
	}
	return string(r.runes[start: start+length]), true
}

// Remainder() returns a string of all runes from and including
// the _end_ pointer to the last rune in the rune set. That is,
// it returns everything from the _end_ pointer to the end.
func (r *RuneReader) Remainder() string {
	if r.next == r.length {
		return ""
	}
	r.start = r.next
	r.next = r.length
	return runeRange(r, r.start, r.next)
}

// Reset() returns the _start_ and _end_ pointers to the beginning
// of the set. Both pointers will point to the zeroth rune in the set.
func (r *RuneReader) Reset() {
	r.start, r.next = 0, 0
}

func (r *RuneReader) RewindSlurpWord() {
	r.next = r.start
	for r.start -= 1; r.start > 0; r.start -= 1 {
		if unicode.Is(unicode.Space, r.runes[r.start]) {
			r.start += 1
			break
		}
	}
}

// ScanForRune(_match_) searches the rune set for the occurance of _match_ and
// returns the string up to and including _match_. The _start_ pointer is
// reset and the _end_ pointer advances until it encounters _match_ or reaches
// the the last rune in the rune set. If no _match_ is found, the entire rune
// set beginning at _start_ is returned with a false value. The _end_ pointer
// is not reset on failure.
func (r *RuneReader) ScanForRune(match ...interface{}) (string, bool) {
	r.start = r.next
	for ; r.next < r.length; r.next += 1 {
		if checkReader(r, match...) {
			r.next += 1
			return runeRange(r, r.start, r.next), true
		}
	}
	return string(r.runes[r.start:]), false
}

func (r *RuneReader) ScanForRuneWhile(match ...interface{}) (string, bool) {
	r.start = r.next
	if !checkReader(r, match...) {
		return "", false
	}
	for ; r.next < r.length; r.next += 1 {
		if !checkReader(r, match...) {
			return runeRange(r, r.start, r.next), true
		}
	}
	return string(r.runes[r.start:]), true
}

func (r *RuneReader) Seek(pos int, length int) {
	if pos < 0 {
		panic("negative position on seek")
	}
	if length < 0 {
		panic("length cannot be less than zero")
	} else if length == 0 {
		r.next = pos
	} else if pos+length+1 > r.length {
		r.next = r.length
	} else {
		r.next = pos + length
	}
	r.start = pos
}

func (r *RuneReader) Skip(length int) *RuneReader {
	r.start += length
	r.next += length
	if r.next > r.length {
		r.next = r.length
	}
	if r.start < 0 {
		r.start = 0
	}
	if r.next < 0 {
		r.next = 0
	}
	return r
}

func (r *RuneReader) SkipWords(count int) *RuneReader {
	for ; count > 0 && r.next < r.length; r.next += 1 {
		if unicode.IsSpace(r.runes[r.next]) {
			trimLeftRunes(r, unicode.Space)
			count -= 1
		}
	}
	r.start = r.next
	return r
}

func (r *RuneReader) MultiSlurpWord(count int) []string {
	var out []string
	r.start = r.next
	for i := 0; i < count; i += 1 {
		if word, ok := r.SlurpWord(); ok {
			out = append(out, word)
		} else {
			break
		}
	}
	return out
}

func (r *RuneReader) SlurpWord() (string, bool) {
	if r.next > r.length {
		return "", false
	}
	r.start = r.next
	for r.next += 1; r.next <= r.length; r.next += 1 {
		if unicode.Is(unicode.Space, r.runes[r.next-1]) {
			if r.start == r.next-1 {
				if r.start < r.length && r.runes[r.start] == ' ' {
					r.start += 1
				}
				continue
			}
			break
		}
	}
	trimLeftRunes(r, unicode.Space)
	return runeRange(r, r.start, r.next-1), true
}

func (r *RuneReader) String() string {
	return string(r.runes)
}

// Sync() brings the _start_ and _end_ pointers in sync by
// setting the _start_ pointer to the current rune.
func (r *RuneReader) Sync() *RuneReader {
	if r.next-1 < 0 {
		r.start = 0
	} else {
		r.start = r.next - 1
	}
	return r
}

func checkReader(r *RuneReader, a ...interface{}) bool {
	if len(a) == 0 {
		panic("no arguments provided to check")
	}
	if r.next > r.length-1 {
		return false
	}
	for _, b := range a {
		switch v := b.(type) {
		case string:
			length := len(v)
			if length == 0 || r.next+length > r.length {
				continue
			} else if length == 1 && r.runes[r.next] == rune(v[0]) {
				return true
			} else if string(r.runes[r.next:r.next+length]) == v {
				return true
			}
		case rune:
			return r.runes[r.next] == v
		case []rune:
			for _, i := range v {
				if i == r.runes[r.next] {
					return true
				}
			}
		case byte:
		case int:
			if r.runes[r.next] == rune(v) {
				return true
			}
		case *unicode.RangeTable:
			if unicode.Is(v, r.runes[r.next]) {
				return true
			}
		default:
			panic(fmt.Sprintf("Unexpected match type: %T", v))
		}
	}
	return false
}

func checkRune(r rune, a ...interface{}) bool {
	for _, b := range a {
		switch v := b.(type) {
		case rune:
			return r == v
		case []rune:
			for _, i := range v {
				if r == i {
					return true
				}
			}
		case byte:
		case int:
			if r == rune(v) {
				return true
			}
		case *unicode.RangeTable:
			if unicode.Is(v, r) {
				return true
			}
		default:
			panic(fmt.Sprintf("unexpected match type: %T", v))
		}
	}
	return false
}

func runeRange(r *RuneReader, start int, end int) string {
	if start == end {
		return ""
	} else if start == end-1 {
		return string(r.runes[start])
	} else if end > r.length {
		return string(r.runes[start:r.length])
	} else if start > end {
		panic("start is more than end")
	} else {
		return string(r.runes[start:end])
	}
}

func trimLeftRunes(r *RuneReader, table *unicode.RangeTable) {
	for ; r.start < r.length && unicode.Is(table, r.runes[r.start]); r.start += 1 {
	}
}
