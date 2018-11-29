package util

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"
	//"golang.org/x/text/unicode/norm"
)

var debugMutex sync.Mutex

func Debug(format string, v ...interface{}) {
	debugMutex.Lock()
	defer debugMutex.Unlock()

	fmt.Fprintf(os.Stderr, format+"\n", v...)
}
func ArgumentSplit(r rune) bool {
	switch r {
	case ',', ';', ' ':
		return true
	default:
		return false
	}
}
func ArgumentMatchOptions(match []string, a string) bool {
	for _, value := range strings.FieldsFunc(a, ArgumentSplit) {
		if !ArrayInsensitiveMatchString(match, value) {
			return false
		}
	}
	return true
}
func ArrayBinarySearchString(a string, m []string) bool {
	p := sort.SearchStrings(m, a)
	if p >= len(m) {
		return false
	}
	return m[p] == a
}
func ArrayFilterString(a []string, match func(string) bool) []string {
	b := a[:0]
	for _, x := range a {
		if match(x) {
			b = append(b, x)
		}
	}
	return b
}
func ArrayInsensitiveMatchString(a []string, match string) bool {
	for _, x := range a {
		if StringInsensitiveMatch(x, match) {
			return true
		}
	}
	return false
}
func ArrayMatchString(a []string, match string) bool {
	for _, x := range a {
		if StringMatch(x, match) {
			return true
		}
	}
	return false
}
func StringDoubleSplit(s string, d rune) (string, string, bool) {
	pos := strings.IndexRune(s, d)
	if pos > -1 {
		return s[0:pos], s[pos+1:], true
	}

	return s, "", false
}

// String normalization is necessary for unicode processing of logs. Starting this project using basic UTF8 rune
// counting, but this should eventually translate to normalization (http://godoc.org/golang.org/x/text/unicode/norm).
// See also: https://blog.golang.org/normalization
func StringLength(s string) (n int) { return utf8.RuneCountInString(s) }
func StringToLower(s string) string { return strings.ToLower(s) }
func StringToUpper(s string) string { return strings.ToUpper(s) }

func StringMatch(a, b string) bool {
	if strings.Compare(a, b) == 0 {
		return true
	}
	return false
}

func StringInsensitiveMatch(s, t string) bool { return strings.EqualFold(s, t) }

// Cuts an element from an array, modifying the original array and returning the value cut.
func SliceCutString(a []string, index int) (string, []string) {
	switch index {
	case 0:
		return a[0], a[1:]

	case len(a) - 1:
		return a[index], a[:index-1]

	default:
		return a[index], append(a[:index-1], a[index+1:]...)
	}
}

func IsNumeric(a string) bool {
	if _, err := strconv.Atoi(a); err == nil {
		return true
	}

	return false
}

func IsNumericRune(r rune) bool {
	return unicode.IsNumber(r)
}
