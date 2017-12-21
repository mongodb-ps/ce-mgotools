package util

import (
	"strings"
	"testing"
)

func BenchmarkCopy(b *testing.B) {
	a := []rune(strings.Repeat("a", 1024*100))
	l := len(a)
	r := RuneReader{0, 0, l, a}
	for n := 0; n < b.N; n += 1 {
		testRuneReaderCopy(r)
		if r.Pos() != 0 {
			panic("unexpected position")
		}
	}
}
func BenchmarkPass(b *testing.B) {
	r := NewRuneReader(strings.Repeat("a", 1024*100))
	for n := 0; n < b.N; n += 1 {
		testRuneReaderPass(r)
		if r.Pos() != 0 {
			panic("unexpected position")
		}
	}
}
func testRuneReaderCopy(a RuneReader) {
	a.SlurpWord()
}
func testRuneReaderPass(a *RuneReader) {
	a.SlurpWord()
	a.Reset()
}
