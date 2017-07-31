package util

import (
	"fmt"
	"strconv"
	"strings"
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
const DATE_LENGTH_MIN = 10

// RFC3339 (which is stricter than ISO8601 and, incidentally, used by MongoDB)
const DATE_REGEX_RFC3339 = `^(\d{4})-(?:(0[13578]|1[02])-([12]\d|0[1-9]|3[01])|(0[469]|11)-([12]\d|0[1-9]|3[0])|(02)-([12]\d|0[1-9]))(?:[Tt\s])([01]\d|2[0-3]):([0-5]\d):([0-5]\d|60)(\.\d{1,4})?(?:([-+](?:\d{2}|\d{4}|\d{2}:\d{2}?|Z)))?$`
const DATE_REGEX_TIME = `(?:[01][1-9]|2[0123]):[0-5][0-9]:(?:[0-5][0-9]|60)(?:\.[0-9]{2,8})?(?:Z|[-+][01]\d:?\d{2})?`
const DATE_REGEX_OFFSET = `^[+-]?(0\d|1[012]):?(\d{2})$`

var DATE_DAYS = []string{"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"}
var DATE_MONTHS = []string{"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"}

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

func DateParse(datestring string) (time.Time, bool) {
	regexRegistry := GetRegexRegistry()
	defer fmt.Println("Ending DateParse")

	if match, err := regexRegistry.CompileAndMatch(DATE_REGEX_RFC3339, datestring); err == nil {
		match = ArrayFilter(match, func(a string) bool { return a != "" })
		match = match[1:]

		var (
			count      int = len(match)
			loc            = time.FixedZone("UTC", 0)
			offset     int
			ok         bool
			timeValues [7]int
		)

		for i := 0; i < 6; i += 1 {
			if timeValues[i], err = strconv.Atoi(match[i]); err != nil {
				fmt.Println("Failed to parse date: ", err)
				return time.Time{}, false
			}
		}

		fmt.Println("Regex matched: ", strings.Join(match, "|"), count)

		if count == 6 {
			// Set the millisecond portion to zero if it doesn't exist.
			timeValues[6] = 0
		} else if count == 7 && match[6][0] == '.' {
			if timeValues[6], err = strconv.Atoi(match[6][1:]); err != nil {
				timeValues[6] = 0
			}
		} else if count == 7 && match[6] != "Z" {
			if offset, ok = parseOffset(match[7]); !ok {
				return time.Time{}, false
			}
			loc = time.FixedZone("", offset)
		} else if count == 8 {
			if timeValues[6], err = strconv.Atoi(match[6][1:]); err != nil {
				fmt.Println(err)
				return time.Time{}, false
			}
			if offset, ok = parseOffset(match[7]); !ok {
				return time.Time{}, false
			}
			loc = time.FixedZone("", offset)
		}

		var month time.Month = time.Month(timeValues[1])
		date := time.Date(timeValues[0], month, timeValues[2], timeValues[3], timeValues[4], timeValues[5], timeValues[6]*1000000, loc)
		fmt.Println("Date success: ", date, date.Nanosecond())

		return date, true
	} else if date, err := time.Parse(time.ANSIC, datestring); err == nil {
		return date, true
	}

	return time.Time{}, false
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

func parseOffset(offset string) (int, bool) {
	if offset == "" || offset == "Z" {
		return 0, false
	}

	var (
		neg  int = 1
		hour int
		min  int
	)
	if offset[0] == '-' {
		neg = -1
		offset = offset[1:]
	} else if offset[0] == '+' {
		offset = offset[1:]
	}

	check, err := GetRegexRegistry().Compile(DATE_REGEX_OFFSET)
	if err != nil {
		panic(err)
	}

	match := check.FindStringSubmatch(offset)
	if match == nil || len(match) != 3 {
		return 0, false
	}

	if hour, err = strconv.Atoi(match[1]); err != nil {
		return 0, false
	}

	if min, err = strconv.Atoi(match[2]); err != nil {
		return 0, false
	}

	return neg * (hour*3600 + min*60), true
}
