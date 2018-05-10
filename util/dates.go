package util

import (
	//"github.com/olebedev/when"
	"sync"
	"time"
	"unicode"
)

// ctime-no-ms
// Wed Dec 31 19:00:00

// ctme
// Wed Dec 31 19:00:00.000

// iso8601-utc
// 1970-01-01T00:00:00.000Z

// iso8601-local
// 1969-12-31T19:00:00.000+0500

// Some arbitrary length that is enforced before parsing date strings. In this case, a value of 10 includes the day,
// month, and two number date as a minimum. The time could, theoretically, be included in the minimum but it will all
// come out in the wash later.
const (
	DATE_FORMAT_CTIMENOMS     = "Mon Jan 02 15:04:05"
	DATE_FORMAT_CTIME         = "Mon Jan 02 15:04:05.000"
	DATE_FORMAT_CTIMEYEAR     = "Mon Jan 02 2006 15:04:05.000"
	DATE_FORMAT_ISO8602_UTC   = "2006-01-02T15:04:05.000Z"
	DATE_FORMAT_ISO8602_LOCAL = "2006-01-02T15:04:05.000-0700"

	// RFC3339 (which is stricter than ISO8601 and, incidentally, used by MongoDB)
	DATE_REGEX_RFC3339 = `^(\d{4})-(?:(0[13578]|1[02])-([12]\d|0[1-9]|3[01])|(0[469]|11)-([12]\d|0[1-9]|3[0])|(02)-([12]\d|0[1-9]))(?:[Tt\s])([01]\d|2[0-3]):([0-5]\d):([0-5]\d|60)(\.\d{1,4})?(?:([-+](?:\d{2}|\d{4}|\d{2}:\d{2}?|Z)))?$`
	DATE_REGEX_TIME    = `(?:[01][0-9]|2[0123]):[0-5][0-9]:(?:[0-5][0-9]|60)(?:\.[0-9]{1,8})?(?:Z|[-+][01]\d:?\d{2})?`
	DATE_REGEX_OFFSET  = `^[+-]?(0\d|1[012]):?(\d{2})$`
)

type DateParser struct {
	formatCount int
	formatOrder []string
	lock        sync.Mutex
}

var DATE_DAYS = []string{"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"}
var DATE_MONTHS = []string{"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"}
var DATE_YEAR = time.Now().Year()

func NewDateParser(formats []string) *DateParser {
	return &DateParser{
		formatCount: len(formats),
		formatOrder: formats,
	}
}
func (d *DateParser) ParseDate(value string) (time.Time, error) {
	var (
		date  time.Time
		err   error
		index int
		order = d.formatOrder
	)
	for index = 0; index < d.formatCount; index += 1 {
		if date, err = time.Parse(order[index], value); err == nil {
			if index == 0 {
				return date, err
			}
			break
		}
	}
	if err == nil {
		d.reorderFormat(index)
	}
	return date, err
}

func (d *DateParser) reorderFormat(index int) {
	if index > 0 {
		var (
			format  string
			reorder = make([]string, d.formatCount)
		)
		d.lock.Lock()
		copy(reorder, d.formatOrder)
		if index == d.formatCount {
			format, reorder = reorder[d.formatCount-1], reorder[:d.formatCount-1]
			reorder = append([]string{format}, reorder...)
		} else {
			format := reorder[index]
			reorder = append([]string{format}, append(reorder[:index], reorder[index+1:]...)...)
		}
		d.formatOrder = reorder
		d.lock.Unlock()
	}
}

func IsDay(match string) bool {
	return ArrayInsensitiveMatchString(DATE_DAYS, match)
}

func IsMonth(match string) bool {
	return ArrayInsensitiveMatchString(DATE_MONTHS, match)
}

/*func IsTime(match string) bool {
	check, _ := GetRegexRegistry().Compile(DATE_REGEX_TIME)
	return check.MatchString(match)
}*/
func IsTime(match string) bool {
	// 00:00:00.000
	check := []rune(match)
	size := len(check)

	if size == 8 {
		return unicode.IsNumber(check[0]) &&
			unicode.IsNumber(check[1]) &&
			check[2] == ':' &&
			unicode.IsNumber(check[3]) &&
			unicode.IsNumber(check[4]) &&
			check[5] == ':' &&
			unicode.IsNumber(check[6]) &&
			unicode.IsNumber(check[7])
	} else if size == 12 {
		return unicode.IsNumber(check[0]) &&
			unicode.IsNumber(check[1]) &&
			check[2] == ':' &&
			unicode.IsNumber(check[3]) &&
			unicode.IsNumber(check[4]) &&
			check[5] == ':' &&
			unicode.IsNumber(check[6]) &&
			unicode.IsNumber(check[7]) &&
			check[8] == '.' &&
			unicode.IsNumber(check[9]) &&
			unicode.IsNumber(check[10]) &&
			unicode.IsNumber(check[11])
	} else {
		return false
	}
}
