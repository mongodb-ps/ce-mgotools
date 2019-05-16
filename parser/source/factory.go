package source

import "mgotools/parser/record"

type Factory interface {
	Next() bool
	Get() (record.Base, error)
	Close() error
}
