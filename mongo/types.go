package mongo

import (
	"encoding/hex"
	"time"
)

type MaxKey struct{}
type MinKey struct{}
type Timestamp time.Time
type Undefined struct{}

type BinData struct {
	BinData []byte
	Type    byte
}

type Regex struct {
	Regex   string
	Options string
}

type Ref struct {
	Name string
	Id   ObjectId
}

type ObjectId []byte

func NewObjectId(s []byte) (ObjectId, bool) {
	var d = make([]byte, 24, 24)
	if len(s) != 24 {
		return nil, false
	} else if l, err := hex.Decode(d, s); l != 12 || err != nil {
		return nil, false
	} else {
		return ObjectId(d), true
	}
}
