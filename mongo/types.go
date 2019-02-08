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

type ObjectId [12]byte

func NewObjectId(s string) (ObjectId, bool) {
	var d = make([]byte, 12, 12)
	if len(s) != 24 {
		return ObjectId{}, false
	} else if l, err := hex.Decode(d, []byte(s)); l != 12 || err != nil {
		return ObjectId{}, false
	} else {
		var v ObjectId
		copy(v[:], d[:12])
		return v, true
	}
}

func (o ObjectId) Slice() []byte {
	var s = make([]byte, 12, 12)
	copy(s, o[:12])
	return s
}

func (o ObjectId) Equals(a ObjectId) bool {
	return o == a
}
