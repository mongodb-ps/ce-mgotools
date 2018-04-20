package parser

import (
	"fmt"
	"strconv"
	"testing"
)

func BenchmarkShort(b *testing.B) {
	t := LogVersionDefinition{2, 4, 1}
	for n, f := range map[string]func(LogVersionDefinition) string{
		"VersionFmtString":    VersionFmtString,
		"VersoinCompoundItoa": VersionCompoundItoa,
		"VersionBytesItoa":    VersionBytesItoa,
		"VersionBytes":        VersionBytes,
	} {
		b.Run(n+strconv.Itoa(t.Major)+"."+strconv.Itoa(t.Minor), func(c *testing.B) {
			for i := 0; i < c.N; i += 1 {
				_ = f(t)
			}
		})
	}
}

func BenchmarkMediumShort(b *testing.B) {
	t := LogVersionDefinition{10, 2, 2}
	for n, f := range map[string]func(LogVersionDefinition) string{
		"VersionFmtString":    VersionFmtString,
		"VersoinCompoundItoa": VersionCompoundItoa,
		"VersionBytesItoa":    VersionBytesItoa,
		"VersionBytes":        VersionBytes,
	} {
		b.Run(n+strconv.Itoa(t.Major)+"."+strconv.Itoa(t.Minor), func(c *testing.B) {
			for i := 0; i < c.N; i += 1 {
				_ = f(t)
			}
		})
	}

}

func BenchmarkShortMedium(b *testing.B) {
	t := LogVersionDefinition{2, 10, 2}
	for n, f := range map[string]func(LogVersionDefinition) string{
		"VersionFmtString":    VersionFmtString,
		"VersoinCompoundItoa": VersionCompoundItoa,
		"VersionBytesItoa":    VersionBytesItoa,
		"VersionBytes":        VersionBytes,
	} {
		b.Run(n+strconv.Itoa(t.Major)+"."+strconv.Itoa(t.Minor), func(c *testing.B) {
			for i := 0; i < c.N; i += 1 {
				_ = f(t)
			}
		})
	}

}

func BenchmarkLong(b *testing.B) {
	t := LogVersionDefinition{10, 50, 1}
	for n, f := range map[string]func(LogVersionDefinition) string{
		"VersionFmtString":    VersionFmtString,
		"VersoinCompoundItoa": VersionCompoundItoa,
		"VersionBytesItoa":    VersionBytesItoa,
		"VersionBytes":        VersionBytes,
	} {
		b.Run(n+strconv.Itoa(t.Major)+"."+strconv.Itoa(t.Minor), func(c *testing.B) {
			for i := 0; i < c.N; i += 1 {
				_ = f(t)
			}
		})
	}
}

func VersionFmtString(version LogVersionDefinition) string {
	switch version.Binary {
	case LOG_VERSION_MONGOD:
		return fmt.Sprintf("mongod %d.%d", version.Major, version.Minor)
	case LOG_VERSION_MONGOS:
		return fmt.Sprintf("mongos %d.%d", version.Major, version.Minor)
	default:
		panic("unexpected binary")
	}
}

func VersionCompoundItoa(version LogVersionDefinition) string {
	switch version.Binary {
	case LOG_VERSION_MONGOD:
		return "mongod " + strconv.Itoa(version.Major) + "." + strconv.Itoa(version.Minor)
	case LOG_VERSION_MONGOS:
		return "mongos " + strconv.Itoa(version.Major) + "." + strconv.Itoa(version.Minor)
	default:
		panic("unexpected binary")
	}
}

func VersionBytesItoa(version LogVersionDefinition) string {
	var dst []byte
	switch version.Binary {
	case LOG_VERSION_MONGOD:
		dst = []byte{'m', 'o', 'n', 'g', 'o', 'd', ' '}
	case LOG_VERSION_MONGOS:
		dst = []byte{'m', 'o', 'n', 'g', 'o', 's', ' '}
	default:
		panic("unexpected binary")
	}
	dst = strconv.AppendInt(dst, int64(version.Major), 10)
	dst = append(dst, '.')
	dst = strconv.AppendInt(dst, int64(version.Major), 10)
	return string(dst)
}

func VersionBytes(version LogVersionDefinition) string {
	var dst [12]byte
	offset := 0

	switch version.Binary {
	case LOG_VERSION_MONGOD:
		dst = [12]byte{'m', 'o', 'n', 'g', 'o', 'd', ' ', 0, '.', '.'}
	case LOG_VERSION_MONGOS:
		dst = [12]byte{'m', 'o', 'n', 'g', 'o', 's', ' ', 0, '.', '.'}
	default:
		panic("unexpected binary")
	}

	if version.Major < 10 {
		dst[7] = byte(version.Major) + 0x30
	} else if version.Major < 100 {
		dst[7] = byte(version.Major/10) + 0x30
		dst[8] = byte(version.Major%10) + 0x30
		offset = 1
	} else {
		panic("major version too large")
	}

	if version.Minor < 10 {
		dst[9+offset] = byte(version.Minor) + 0x30
	} else if version.Minor < 100 {
		dst[9+offset] = byte(version.Minor/10) + 0x30
		dst[10+offset] = byte(version.Minor%10) + 0x30
		offset = 2
	} else {
		panic("minor version too large")
	}
	return string(dst[:12-2+offset])
}
