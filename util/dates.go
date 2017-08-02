package util

import (
	"strings"
	"sync"
	"time"
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
	DATE_LENGTH_MIN  = 10
	DATE_UNIX_NOYEAR = 365*86400 - 1

	DATE_FORMAT_CTIMENOMS     = "Mon Jan 02 15:04:05"
	DATE_FORMAT_CTIME         = "Mon Jan 02 15:04:05.000"
	DATE_FORMAT_CTIMEYEAR     = "Mon Jan 02 2006 15:04:05.000"
	DATE_FORMAT_ISO8602_UTC   = "2006-01-02T15:04:05.000Z"
	DATE_FORMAT_ISO8602_LOCAL = "2006-01-02T15:04:05.000-0700"
	DATE_FORMAT_COUNT         = 5

	// RFC3339 (which is stricter than ISO8601 and, incidentally, used by MongoDB)
	DATE_REGEX_RFC3339 = `^(\d{4})-(?:(0[13578]|1[02])-([12]\d|0[1-9]|3[01])|(0[469]|11)-([12]\d|0[1-9]|3[0])|(02)-([12]\d|0[1-9]))(?:[Tt\s])([01]\d|2[0-3]):([0-5]\d):([0-5]\d|60)(\.\d{1,4})?(?:([-+](?:\d{2}|\d{4}|\d{2}:\d{2}?|Z)))?$`
	DATE_REGEX_TIME    = `(?:[01][1-9]|2[0123]):[0-5][0-9]:(?:[0-5][0-9]|60)(?:\.[0-9]{2,8})?(?:Z|[-+][01]\d:?\d{2})?`
	DATE_REGEX_OFFSET  = `^[+-]?(0\d|1[012]):?(\d{2})$`
)

type logDate struct {
	formatOrder []string
	lock        sync.Mutex
}

var DATE_DAYS = []string{"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"}
var DATE_MONTHS = []string{"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"}
var DATE_YEAR = time.Now().Year()

var logDateSingleton *logDate = &logDate{[]string{DATE_FORMAT_ISO8602_UTC, DATE_FORMAT_ISO8602_LOCAL, DATE_FORMAT_CTIME, DATE_FORMAT_CTIMENOMS, DATE_FORMAT_CTIMEYEAR}, sync.Mutex{}}

func DateParse(value string) (time.Time, bool) {
	var (
		date    time.Time
		err     error
		index   int
		order   []string = logDateSingleton.formatOrder
		success bool     = false
	)

	for index = 0; index < DATE_FORMAT_COUNT; index += 1 {
		if date, err = time.Parse(order[index], value); err == nil {
			if index == 0 {
				return date, true
			}

			success = true
			break
		}
	}

	if success && index > 0 {
		var (
			format  string
			reorder []string = make([]string, DATE_FORMAT_COUNT)
		)

		logDateSingleton.lock.Lock()
		copy(reorder, logDateSingleton.formatOrder)

		if index == DATE_FORMAT_COUNT {
			format, reorder = reorder[DATE_FORMAT_COUNT-1], reorder[:DATE_FORMAT_COUNT-1]
			reorder = append([]string{format}, reorder...)
		} else {
			format := reorder[index]
			reorder = append([]string{format}, append(reorder[:index], reorder[index+1:]...)...)
		}

		logDateSingleton.formatOrder = reorder
		logDateSingleton.lock.Unlock()
	}

	return date, success
}

// Take a parts array ([]string { "Sun", "Jan", "02", "15:04:05" }) and combined into a single element
// ([]string { "Sun Jan 02 15:04:05" }) with all trailing elements appended to the array.
func DateStringFromArray(target []string) (bool, []string) {
	switch {
	case !IsDay(target[0]):
	case !IsMonth(target[1]):
	case !IsNumeric(target[2]):
	case !IsTime(target[3]):
		return false, target
	}

	if len(target) == 4 {
		target = []string{strings.Join(target, " ")}
	} else if len(target) > 4 {
		target[0] = strings.Join(target[:4], " ")
		target = append([]string{target[0]}, target[4:]...)
	} else {
		return false, target
	}

	return true, target
}

func IsDay(match string) bool {
	return ArrayInsensitiveMatchString(DATE_DAYS, match)
}

func IsMonth(match string) bool {
	return ArrayInsensitiveMatchString(DATE_MONTHS, match)
}

func IsTime(match string) bool {
	check, _ := GetRegexRegistry().Compile(DATE_REGEX_TIME)
	return check.MatchString(match)
}
