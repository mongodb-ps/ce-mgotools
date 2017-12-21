package util_test

import (
	"mgotools/util"
	"testing"
	"unicode"
)

func TestNewRuneReaderEmpty(t *testing.T) {
	r := util.NewRuneReader("")
	if r != nil {
		t.Error("expected nil")
	}
}

func TestRuneReaderLength(t *testing.T) {
	r := util.NewRuneReader("abcd")
	if r.Length() != 4 {
		t.Errorf("expected length 4, got %d", r.Length())
	}
}

func TestRuneReaderEOL(t *testing.T) {
	r := util.NewRuneReader("a")
	if r.EOL() {
		t.Error("EOL reports true, expected false")
	}
	r.Next()
	if !r.EOL() {
		t.Error("EOL reported false, expected true")
	}
}

func TestRuneReaderExpect(t *testing.T) {
	r := util.NewRuneReader("abcdefg")
	if !r.Expect("abcdefg") {
		t.Error("Expect w/ string failed")
	}
	if !r.Expect('a') {
		t.Error("Expect w/ rune failed")
	}
	if !r.Expect([]rune{'x', 'a'}) {
		t.Error("Expect w/ rune array failed")
	}
	if !r.Expect(int('a')) {
		t.Error("Expect w/ int failed")
	}
	if !r.Expect("a") {
		t.Error("Expect w/ single char string failed")
	}
	if !r.Expect(unicode.Letter) {
		t.Error("Expect w/ unicode compare failed")
	}
	r.Next()
	if !r.Expect('b') {
		t.Error("Expect w/ offset and rune failed")
	}
	if r.Expect([]rune{'c', 'd'}) {
		t.Error("Expect w/ unmatched byte succeeded unexpectedly")
	}
	if !r.Expect("bcdefg") {
		t.Error("Expect w/ string and sub-portion failed")
	}
	if !r.Expect("bc") {
		t.Error("Expect w/ portion string and sub-portion failed")
	}
	r.Skip(6)
	if r.Expect([]rune{'a', 'b', 'c', 'd', 'e', 'f', 'g'}) {
		t.Error("Expect matched unexpectedly")
	}
	if r.Expect("") {
		t.Error("Expect w/ empty string succeeded unexpectedly")
	}
}

func TestRuneReaderExpectString(t *testing.T) {
	r := util.NewRuneReader("this is a string")
	if !r.ExpectString("this") {
		t.Errorf("ExpectString failed at 'this'")
	}
	if !r.ExpectString("this is a string") {
		t.Errorf("ExpectString failed at 'this is a string'")
	}
	if r.ExpectString("this is a string ") {
		t.Errorf("ExpectString succeeded incorrectly")
	}
	if !r.ExpectString("t") {
		t.Errorf("ExpectString failed at 't'")
	}
	if r.ExpectString("is a string") {
		t.Errorf("ExpectString succedded incorrectly")
	}
}

func TestRuneReaderNext(t *testing.T) {
	r := util.NewRuneReader("ab")
	if c, ok := r.Next(); c != 'a' {
		t.Errorf("expected 'a' got '%c'", c)
	} else if !ok {
		t.Error("expected ok=true, got ok=false")
	}
	if c, ok := r.Next(); c != 'b' {
		t.Errorf("expected 'b', got '%c'", c)
	} else if !ok {
		t.Error("expected ok=true, got ok=false")
	}
	if c, ok := r.Next(); ok {
		t.Error("expected ok=false, got ok=true")
	} else if c != 0 {
		t.Errorf("expected 0, got '%c'", c)
	}
}

func TestRuneReaderPeek(t *testing.T) {
	r := util.NewRuneReader("abc")
	if r.Peek(1) != "a" {
		t.Error("Peek length one returned incorrect values")
	}
	if r.Peek(3) != "abc" {
		t.Error("Peek length 3 returned incorrect values")
	}
	if r.Peek(5) != "abc" {
		t.Error("Peek w/ excess length returned incorrect values")
	}
	if r.Peek(-1) != "" {
		t.Error("Peek negative length at zero position returned a value")
	}
	r.Next()
	r.Next()
	r.Next()
	if r.Peek(1) != "" {
		t.Error("Peek at end position returned a non-empty string")
	}
	if r.Peek(-3) != "abc" {
		t.Errorf("Negative peek didn't return expected string (%s)", r.Peek(-3))
	}
	if r.Peek(-4) != "abc" {
		t.Error("Negative overloaded peek didn't return expected string")
	}
}

func TestRuneReaderPrefix(t *testing.T) {
	r := util.NewRuneReader("abcd")
	if r.Prefix(1) != "a" {
		t.Error("Peek length 1 mismatch")
	}
	if r.Prefix(4) != "abcd" {
		t.Error("Peek length 4 mismatch")
	}
	if r.Prefix(5) != "abcd" {
		t.Error("Peek exeeded length mismatch")
	}
	if r.Prefix(0) != "" {
		t.Error("Zero or less prefix matched unexpectedly")
	}
}

func TestRuneReaderQuotedString(t *testing.T) {
	for _, s := range []string{"'quoted string'", "\"quoted string\""} {
		r := util.NewRuneReader(s)
		if out, err := r.QuotedString(); err != nil || out != s[1:len(s)-1] {
			t.Errorf("Quoted string failed, expected '%s', got '%s'", s, out)
		}
	}
}

func TestRuneReaderRead(t *testing.T) {
	r := util.NewRuneReader("0123456789")
	if out, ok := r.Read(0, 10); out != "0123456789" || !ok {
		t.Errorf("Read failed, expected 0123456789 got %s", out)
	}
	if out, ok := r.Read(0, 1); out != "0" || !ok {
		t.Errorf("Read failed, expected 0 got %s", out)
	}
	if out, ok := r.Read(10, 1); out != "" || ok {
		t.Errorf("Read succeeded, expected '' got %s", out)
	}

}

func TestRuneReaderScanForRune(t *testing.T) {
	r := util.NewRuneReader("abcd")
	if s, ok := r.ScanForRune('a'); s != "a" || !ok {
		t.Errorf("scan for rune 'a' failed, '%s' returned", s)
	}
	if s, ok := r.ScanForRune('d'); s != "bcd" || !ok {
		t.Errorf("scan for rune 'd' failed, '%s' returned", s)
	}
	r.Seek(0, 0)
	if s, ok := r.ScanForRune('e'); s != "abcd" || ok {
		t.Errorf("scan for 'e' failed, '%s' returned", s)
	}
	r.Seek(1, 0)
	if s, ok := r.ScanForRune('a'); s != "bcd" || ok {
		t.Errorf("scan for 'a' past a, '%s' returned", s)
	}
}

func TestRuneReaderScanForRuneWhile(t *testing.T) {
	r := util.NewRuneReader("abc def")
	if s, ok := r.ScanForRuneWhile([]rune{'a', 'b', 'c'}); s != "abc" || !ok {
		t.Errorf("scan for rune a,b,c failed, '%s' returned", s)
	}
	r.Next()
	if s, ok := r.ScanForRuneWhile([]rune{'d', 'e', 'f'}); s != "def" || !ok {
		t.Errorf("scan for rune d,e,f succeeded, '%s' returned", s)
	}
	r.Seek(0, 0)
	if s, ok := r.ScanForRuneWhile('x'); s != "" || ok {
		t.Errorf("scan for rune 'x' succeeded, '%s' returned", s)
	}

}

func TestRuneReaderSkip(t *testing.T) {
	r := util.NewRuneReader("abc")
	r.Skip(0)
	if r.Pos() != 0 {
		t.Errorf("expected 0, got %d", r.Pos())
	}
	r.Skip(1)
	if r.Pos() != 1 {
		t.Errorf("expected 1, got %d", r.Pos())
	}
	r.Skip(2)
	if r.Pos() != 3 {
		t.Errorf("expected 3, got %d", r.Pos())
	}
	r = util.NewRuneReader("abc")
	r.Skip(5)
	if r.Pos() != 3 {
		t.Errorf("expected 3, got %d", r.Pos())
	}
	r.Skip(-1)
	if r.Pos() != 2 {
		t.Errorf("expected 2, got %d", r.Pos())
	}
	r.Skip(-5)
	if r.Pos() != 0 {
		t.Errorf("expected 0, got %d", r.Pos())
	}
}

func TestRuneReaderSkipWords(t *testing.T) {
	r := util.NewRuneReader("this is a string")
	r.SkipWords(1)
	if r.Pos() != 5 {
		t.Errorf("Expected 5, got %d", r.Pos())
	}
	r.SkipWords(2)
	if r.Pos() != 10 {
		t.Errorf("Expected 10, got %d", r.Pos())
	}
	r.SkipWords(1)
	if r.Pos() != 16 {
		t.Errorf("Expected 16, got %d", r.Pos())
	}

}

func TestRuneReaderSeek(t *testing.T) {
	// a = 0, b = 1, c = 2, d = 3, e = 4, f = 5, g = 6
	r := util.NewRuneReader("abcdefg")
	r.Seek(0, 0)
	if r.CurrentWord() != "" {
		t.Errorf("expected empty string, got '%s'", r.CurrentWord())
	}
	r.Seek(0, 7)
	if r.CurrentWord() != "abcdefg" {
		t.Errorf("expected 'abcdefg', got '%s'", r.CurrentWord())
	}
	r.Seek(0, 3)
	if r.CurrentWord() != "abc" {
		t.Errorf("expected 'abc', got '%s'", r.CurrentWord())
	}
	r.Seek(8, 1)
	if r.CurrentWord() != "" {
		t.Errorf("expected empty string")
	}
	r.Seek(4, 3)
	if r.CurrentWord() != "efg" {
		t.Errorf("expected 'efg', got '%s'", r.CurrentWord())
	}
	r.Seek(5, 3)
	if r.CurrentWord() != "fg" {
		t.Errorf("expected 'fg', 'got '%s'", r.CurrentWord())
	}
}
func TestRuneReaderSlurpWord(t *testing.T) {
	msg := util.NewRuneReader("this is a series of words")
	expect := []string{"this", "is", "a", "series", "of", "words"}
	for i := 0; i < len(expect); i++ {
		if word, ok := msg.SlurpWord(); word != expect[i] || !ok {
			t.Errorf("Got '%s', expected '%s'", word, expect[i])
		}
	}
	if word, ok := msg.SlurpWord(); word != "" && !ok {
		t.Errorf("Got '%s', expected !ok", word)
	}
	msg = util.NewRuneReader("a { x: xyz }  word a   xyz ")
	expect = []string{"a", "{", "x:", "xyz", "}", "word", "a", "xyz"}
	for i := 0; i < len(expect); i++ {
		if word, ok := msg.SlurpWord(); word != expect[i] || !ok {
			t.Errorf("Got '%s', expected '%s'", word, expect[i])
		}
	}
	if word, ok := msg.SlurpWord(); word != "" && !ok {
		t.Errorf("Got '%s', expected !ok", word)
	}
	msg = util.NewRuneReader("word")
	if word, ok := msg.SlurpWord(); word != "word" || !ok {
		t.Errorf("Got '%s', expected 'word'", word)
	}
}

func TestRuneReaderCurrentWord(t *testing.T) {
	// a = 0, b = 1, c = 2, ' ' = 3, d = 4, e = 5, f = 6
	r := util.NewRuneReader("abc def")
	if r.CurrentWord() != "" {
		t.Errorf("expected empty string, got '%c'", r.NextRune())
	}
	r.Seek(0, 2)
	if r.CurrentWord() != "ab" {
		t.Errorf("expected 'ab' got '%s'", r.CurrentWord())
	}
	r.Seek(4, 2)
	if r.CurrentWord() != "de" {
		t.Errorf("expected 'de' got '%s'", r.CurrentWord())
	}
	r.Seek(6, 1)
	if r.CurrentWord() != "f" {
		t.Errorf("expected empty string, got '%s'", r.CurrentWord())
	}
	r.Seek(7, 0)
	if r.CurrentWord() != "" {
		t.Errorf("expected empty string, got '%s'", r.CurrentWord())
	}
}

func TestRuneReaderCurrentRune(t *testing.T) {
	r := util.NewRuneReader("a")
	if r.NextRune() != 'a' {
		t.Fatal("expected 'a'")
	}
	r.Next()
	if r.NextRune() != -1 {
		t.Errorf("expected -1")
	}
}

func TestRuneReaderChompLeft(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected a panic but did not panic")
		}
	}()
	r := util.NewRuneReader("a bcd")
	r.ChompLeft('a')
	if r.NextRune() != ' ' {
		t.Errorf("expected ' ', got '%c'", r.NextRune())
	}
	r.ChompWS()
	if r.NextRune() != 'b' {
		t.Errorf("expected 'b', got '%c'", r.NextRune())
	}
	r.ChompLeft([]rune{'b', 'c'})
	if r.NextRune() != 'd' {
		t.Errorf("expected 'd', got '%c'", r.NextRune())
	}
	r.ChompLeft([]rune{})
	r.ChompLeft('x')
	if r.NextRune() != 'd' {
		t.Errorf("expected 'd, got '%c'", r.NextRune())
	}
	r.ChompLeft(nil)
}
