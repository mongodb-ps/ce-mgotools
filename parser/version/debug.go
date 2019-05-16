// This package exists to support enhanced debugging. It's a package wrapper
// to expose an private static method globally.
//
// +build debug

package version

import (
	"mgotools/parser/record"
)

func (c *Context) Convert(base record.Base, parser Parser) (record.Entry, error) {
	return c.convert(base, parser)
}
