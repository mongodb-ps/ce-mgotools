package internal_test

import (
	"math/rand"
	"strings"
	"testing"
	"unicode"

	"mgotools/internal"
)

func BenchmarkRuneReader_Skip(b *testing.B) {
	r := internal.NewRuneReader("abc def xyz")

	b.Run("SkipWords1", func(b *testing.B) {
		for i := 0; i < b.N; i += 1 {
			r.Seek(0, 0)
			r.SkipWords(1)
			r.SkipWords(1)
			r.SkipWords(1)
		}
	})

	b.Run("SkipWords3", func(b *testing.B) {
		for i := 0; i < b.N; i += 1 {
			r.Seek(0, 0)
			r.SkipWords(3)
		}
	})

	b.Run("SlurpWord", func(b *testing.B) {
		for i := 0; i < b.N; i += 1 {
			r.Seek(0, 0)
			r.SlurpWord()
			r.SlurpWord()
			r.SlurpWord()
		}
	})
}

func BenchmarkRuneReader_SlurpWord(b *testing.B) {
	// Seed with a static variable to get the same randomness every run.
	rand.Seed(1)

	s := strings.Builder{}
	m := "abcdefghijklmnopqrstuvwxyz"
	for i := 0; i < b.N; i += 1 {
		c := rand.Intn(20)

		for j := 0; j < c; j += 1 {
			p := rand.Intn(26)
			s.WriteRune(rune(m[p]))
		}

		s.WriteRune(' ')
	}

	b.ResetTimer()
	r := internal.NewRuneReader(s.String())
	for i := 0; i < b.N; i += 1 {
		r.SlurpWord()
	}
}

func TestNewRuneReaderEmpty(t *testing.T) {
	r := internal.NewRuneReader("")
	if r == nil {
		t.Error("Unexpected nil")
	}
	if !r.EOL() {
		t.Errorf("Incorrect data size")
	}
}

func TestRuneReader_Length(t *testing.T) {
	r := internal.NewRuneReader("abcd")
	if r.Length() != 4 {
		t.Errorf("expected length 4, got %d", r.Length())
	}
}

func TestRuneReader_EnclosedString(t *testing.T) {
	r := internal.NewRuneReader(`"quote"`)
	if s, err := r.EnclosedString('"', true); err != nil || s != "quote" {
		t.Errorf("expected quote, got %s (%s)", s, err)
	}
	r = internal.NewRuneReader(`"quote"`)
	if s, err := r.EnclosedString('"', false); err != nil || s != `"quote"` {
		t.Errorf("expected \"quote\", got %s (%s)", s, err)
	}
	r = internal.NewRuneReader(`'quote'`)
	if s, err := r.EnclosedString('\'', true); err != nil || s != "quote" {
		t.Errorf("expected quote, got %s (%s)", s, err)
	}
	r = internal.NewRuneReader("[quote]")
	if s, err := r.EnclosedString(']', true); err != nil || s != "quote" {
		t.Errorf("expected quote, got %s (%s)", s, err)
	}
	r = internal.NewRuneReader("quote")
	if s, err := r.EnclosedString('"', true); err == nil || s == "quote" {
		t.Errorf("expected error, got nil")
	}
}

func TestRuneReader_EOL(t *testing.T) {
	r := internal.NewRuneReader("a")
	if r.EOL() {
		t.Error("EOL reports true, expected false")
	}
	r.Next()
	if !r.EOL() {
		t.Error("EOL reported false, expected true")
	}
}

func TestRuneReader_Expect(t *testing.T) {
	r := internal.NewRuneReader("abcdefg")
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

func TestRuneReader_ExpectRune(t *testing.T) {

	if r := internal.NewRuneReader("ab"); !r.ExpectRune('a') {
		t.Errorf("ExpectRune failed at 'a'")
	} else if r.ExpectRune('b') {
		t.Errorf("ExpectRune failed by returning 'b'")
	}

	if r := internal.NewRuneReader(""); r.ExpectRune('a') {
		t.Errorf("ExpectRune failed by returning 'a'")
	}

	r := internal.NewRuneReader("ab")
	r.SlurpWord()

	if r.ExpectRune('b') {
		t.Errorf("ExpectRune failed by returning 'b' after slurp")
	}

}

func TestRuneReader_ExpectString(t *testing.T) {
	r := internal.NewRuneReader("this is a string")
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

func TestRuneReader_Insert(t *testing.T) {
	r := internal.NewRuneReader("bd")
	r.Insert('c', 1)
	if r.String() != "bcd" {
		t.Errorf("expected 'bcd', got '%s'", r.String())
	}
	r.Insert('e', 3)
	if r.String() != "bcde" {
		t.Errorf("expected 'bcde', got '%s'", r.String())
	}
	r.Insert('a', 0)
	if r.String() != "abcde" {
		t.Errorf("expected 'abcde', got '%s'", r.String())
	}
}

func TestRuneReader_Next(t *testing.T) {
	r := internal.NewRuneReader("ab")
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

func TestRuneReader_Peek(t *testing.T) {
	r := internal.NewRuneReader("abc")
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

func TestRuneReader_Prefix(t *testing.T) {
	r := internal.NewRuneReader("abcd")
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

func TestRuneReader_PreviewWord(t *testing.T) {
	s := internal.NewRuneReader("abc def")
	if s.PreviewWord(1) != "abc" {
		t.Errorf("Expected 'abc', got '%s'", s.PreviewWord(1))
	}
	if s.PreviewWord(2) != "abc def" {
		t.Errorf("Expected 'abc def', got '%s'", s.PreviewWord(2))
	}

	s = internal.NewRuneReader("")
	if s.PreviewWord(1) != "" {
		t.Errorf("Expected empty string, got '%s'", s.PreviewWord(1))
	}
	if s.PreviewWord(2) != "" {
		t.Errorf("Expected empty string, got '%s'", s.PreviewWord(2))
	}
}

func TestRuneReader_QuotedString(t *testing.T) {
	s := map[string]string{
		"'quoted string'":                  "quoted string",
		"\"quoted string\"":                "quoted string",
		"\"quotes \\\"within\\\" quotes\"": "quotes \\\"within\\\" quotes",
	}
	for d := range s {
		r := internal.NewRuneReader(d)
		if out, err := r.QuotedString(); err != nil || out != s[d] {
			t.Errorf("quoted string failed, expected '%s', got '%s' (%s)", s[d], out, err)
		}
	}
}

func TestRuneReader_Substr(t *testing.T) {
	r := internal.NewRuneReader("0123456789")
	if out, ok := r.Substr(0, 10); out != "0123456789" || !ok {
		t.Errorf("Substr failed, expected 0123456789 got %s", out)
	}
	if out, ok := r.Substr(0, 1); out != "0" || !ok {
		t.Errorf("Substr failed, expected 0 got %s", out)
	}
	if out, ok := r.Substr(10, 1); out != "" || ok {
		t.Errorf("Substr succeeded, expected '' got %s", out)
	}

}

func TestRuneReader_ScanUntilRune(t *testing.T) {
	r := internal.NewRuneReader("abcd")
	if ok := r.ScanUntilRune('a'); !ok {
		t.Error("scan for rune 'a' failed")
	}
	if ok := r.ScanUntilRune('d'); !ok {
		t.Error("scan for rune 'd' failed")
	}
	r.Seek(0, 0)
	if ok := r.ScanUntilRune('e'); ok {
		t.Errorf("scan for 'e' succeeded, should have failed")
	}
	r.Seek(1, 0)
	if ok := r.ScanUntilRune('a'); ok {
		t.Error("scan for 'a' past a succeeded, should have failed'")
	}
}

func TestRuneReader_ScanUntil(t *testing.T) {
	r := internal.NewRuneReader("abcd")
	if s, ok := r.ScanFor('a'); s != "a" || !ok {
		t.Errorf("scan for rune 'a' failed, '%s' returned", s)
	}
	if s, ok := r.ScanFor('d'); s != "bcd" || !ok {
		t.Errorf("scan for rune 'd' failed, '%s' returned", s)
	}
	r.Seek(0, 0)
	if s, ok := r.ScanFor('e'); s != "abcd" || ok {
		t.Errorf("scan for 'e' failed, '%s' returned", s)
	}
	r.Seek(1, 0)
	if s, ok := r.ScanFor('a'); s != "bcd" || ok {
		t.Errorf("scan for 'a' past a, '%s' returned", s)
	}
}

func TestRuneReader_ScanWhile(t *testing.T) {
	r := internal.NewRuneReader("abc def")
	if s, ok := r.ScanWhile([]rune{'a', 'b', 'c'}); s != "abc" || !ok {
		t.Errorf("scan for rune a,b,c failed, '%s' returned", s)
	}
	r.Next()
	if s, ok := r.ScanWhile([]rune{'d', 'e', 'f'}); s != "def" || !ok {
		t.Errorf("scan for rune d,e,f succeeded, '%s' returned", s)
	}
	r.Seek(0, 0)
	if s, ok := r.ScanWhile('x'); s != "" || ok {
		t.Errorf("scan for rune 'x' succeeded, '%s' returned", s)
	}

}

func TestRuneReader_Skip(t *testing.T) {
	r := internal.NewRuneReader("abc")
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
	r = internal.NewRuneReader("abc")
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

func TestRuneReader_SkipWords(t *testing.T) {
	r := internal.NewRuneReader("this is a string")
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

	r = internal.NewRuneReader("a  b   c")
	r.SkipWords(1)
	if r.Pos() != 3 {
		t.Errorf("Expected 3, got %d", r.Pos())
	} else if !r.ExpectString("b") {
		t.Errorf("Did not get 'b'")
	} else if !r.SkipWords(1).ExpectString("c") {
		t.Errorf("Did not get 'c'")
	}
}

func TestRuneReader_String(t *testing.T) {
	r := internal.NewRuneReader("abc")
	if r.String() != "abc" {
		t.Errorf("Expected 'abc', got '%s'", r.String())
	}
	r = internal.NewRuneReader("a")
	if r.String() != "a" {
		t.Errorf("Expected 'a', got '%s'", r.String())
	}
	r = internal.NewRuneReader("")
	if r.String() != "" {
		t.Errorf("Expected empty string, got '%s'", r.String())
	}
}

func TestRuneReader_Seek(t *testing.T) {
	// a = 0, b = 1, c = 2, d = 3, e = 4, f = 5, g = 6
	r := internal.NewRuneReader("abcdefg")
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
func TestRuneReader_SlurpWord(t *testing.T) {
	msg := internal.NewRuneReader("this is a series of words")
	expect := []string{"this", "is", "a", "series", "of", "words"}
	for i := 0; i < len(expect); i++ {
		if word, ok := msg.SlurpWord(); word != expect[i] || !ok {
			t.Errorf("Got '%s', expected '%s'", word, expect[i])
		}
	}
	if word, ok := msg.SlurpWord(); word != "" && !ok {
		t.Errorf("Got '%s', expected !ok", word)
	}
	msg = internal.NewRuneReader("  a  b  c  ")
	if word, ok := msg.SlurpWord(); !ok || word != "a" {
		t.Errorf("Got '%s', expected 'a'", word)
	} else if word, ok := msg.SlurpWord(); !ok || word != "b" {
		t.Errorf("Got '%s', expected 'b'", word)
	} else if word, ok := msg.SlurpWord(); !ok || word != "c" {
		t.Errorf("Got '%s', expected 'c'", word)
	} else if word, ok := msg.SlurpWord(); ok || word != "" {
		t.Errorf("Got '%s', expected nothing.", word)
	}

	msg = internal.NewRuneReader("a { x: xyz }  word a   xyz ")
	expect = []string{"a", "{", "x:", "xyz", "}", "word", "a", "xyz"}
	for i := 0; i < len(expect); i++ {
		if word, ok := msg.SlurpWord(); word != expect[i] || !ok {
			t.Errorf("Got '%s', expected '%s'", word, expect[i])
		}
	}
	if word, ok := msg.SlurpWord(); word != "" && !ok {
		t.Errorf("Got '%s', expected !ok", word)
	}
	msg = internal.NewRuneReader("word")
	if word, ok := msg.SlurpWord(); word != "word" || !ok {
		t.Errorf("Got '%s', expected 'word'", word)
	}
}

func TestRuneReader_CurrentWord(t *testing.T) {
	// a = 0, b = 1, c = 2, ' ' = 3, d = 4, e = 5, f = 6
	r := internal.NewRuneReader("abc def")
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

func TestRuneReader_CurrentRune(t *testing.T) {
	r := internal.NewRuneReader("a")
	if r.NextRune() != 'a' {
		t.Fatal("expected 'a'")
	}
	r.Next()
	if r.NextRune() != -1 {
		t.Errorf("expected -1")
	}
}

func TestRuneReader_ChompLeft(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected a panic but did not panic")
		}
	}()
	r := internal.NewRuneReader("a bcd")
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
