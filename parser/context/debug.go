// This package exists to support enhanced debugging. It's a package wrapper
// to expose an private static method globally.
//
// +build debug

package context

import (
	"mgotools/parser"
	"mgotools/record"
)

func (c *Context) Convert(base record.Base, parser parser.VersionParser) (record.Entry, error) {
	return c.convert(base, parser)
}
