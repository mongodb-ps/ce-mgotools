package version

import (
	"fmt"
	"strconv"
	"testing"

	"mgotools/parser/record"
)

func BenchmarkShort(b *testing.B) {
	t := Definition{2, 4, 1}
	for n, f := range map[string]func(Definition) string{
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
	t := Definition{10, 2, 2}
	for n, f := range map[string]func(Definition) string{
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
	t := Definition{2, 10, 2}
	for n, f := range map[string]func(Definition) string{
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
	t := Definition{10, 50, 1}
	for n, f := range map[string]func(Definition) string{
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

func VersionFmtString(version Definition) string {
	switch version.Binary {
	case record.BinaryMongod:
		return fmt.Sprintf("mongod %d.%d", version.Major, version.Minor)
	case record.BinaryMongos:
		return fmt.Sprintf("mongos %d.%d", version.Major, version.Minor)
	default:
		panic("unexpected binary")
	}
}

func VersionCompoundItoa(version Definition) string {
	switch version.Binary {
	case record.BinaryMongod:
		return "mongod " + strconv.Itoa(version.Major) + "." + strconv.Itoa(version.Minor)
	case record.BinaryMongos:
		return "mongos " + strconv.Itoa(version.Major) + "." + strconv.Itoa(version.Minor)
	default:
		panic("unexpected binary")
	}
}

func VersionBytesItoa(version Definition) string {
	var dst []byte
	switch version.Binary {
	case record.BinaryMongod:
		dst = []byte{'m', 'o', 'n', 'g', 'o', 'd', ' '}
	case record.BinaryMongos:
		dst = []byte{'m', 'o', 'n', 'g', 'o', 's', ' '}
	default:
		panic("unexpected binary")
	}
	dst = strconv.AppendInt(dst, int64(version.Major), 10)
	dst = append(dst, '.')
	dst = strconv.AppendInt(dst, int64(version.Major), 10)
	return string(dst)
}

func VersionBytes(version Definition) string {
	var dst [12]byte
	offset := 0

	switch version.Binary {
	case record.BinaryMongod:
		dst = [12]byte{'m', 'o', 'n', 'g', 'o', 'd', ' ', 0, '.', '.'}
	case record.BinaryMongos:
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
