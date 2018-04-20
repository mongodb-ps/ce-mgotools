package util

import "encoding/hex"

type Bitmap [8]byte

func (bits *Bitmap) IsSet(i int) bool { i -= 1; return bits[i/8]&(1<<uint(7-i%8)) != 0 }
func (bits *Bitmap) Set(i int)        { i -= 1; bits[i/8] |= 1 << uint(7-i%8) }
func (bits *Bitmap) Clear(i int)      { i -= 1; bits[i/8] &^= 1 << uint(7-i%8) }

func (bits *Bitmap) Sets(xs ...int) {
	for _, x := range xs {
		bits.Set(x)
	}
}

func (bits Bitmap) String() string { return hex.EncodeToString(bits[:]) }
