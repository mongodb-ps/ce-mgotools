package mongo

import "time"

type MaxKey struct{}
type MinKey struct{}
type ObjectId []byte
type Timestamp time.Time
type Undefined struct{}

type BinData struct {
	BinData []byte
	Type    string
}

type Regex struct {
	Regex   string
	Options string
}

type Ref struct {
	Name string
	Id   interface{}
}
