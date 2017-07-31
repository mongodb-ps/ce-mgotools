package util

import (
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

// String normalization is necessary for unicode processing of logs. Starting this project using basic UTF8 rune
// counting, but this should eventually translate to normalization (http://godoc.org/golang.org/x/text/unicode/norm).
// See also: https://blog.golang.org/normalization
var StringLength = utf8.RuneCountInString

var StringToLower = unicode.ToLower

var StringToUpper = unicode.ToUpper

var StringMatch = func(a string, b string) bool {
	if strings.Compare(a, b) == 0 {
		return true
	}

	return false
}

var StringInsensitiveMatch = strings.EqualFold

func ArrayFilter(a []string, match func(string) bool) []string {
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
