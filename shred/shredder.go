package shred

import (
	"bytes"
	"errors"
	"fmt"
)

var ErrTooFewDataShreds = errors.New("too few data shreds")

func Deshred(shreds []Shred) ([]byte, error) {
	if len(shreds) == 0 {
		return nil, ErrTooFewDataShreds
	}

	index := shreds[0].CommonHeader().Index
	aligned := true
	for i, shred := range shreds {
		if shred.CommonHeader().Index != index+uint32(i) {
			aligned = false
			break
		}
	}
	lastShred := shreds[len(shreds)-1]
	dataComplete := lastShred.DataComplete() || lastShred.DataHeader().LastInSlot()
	if !dataComplete || !aligned {
		return nil, ErrTooFewDataShreds
	}

	var buf bytes.Buffer
	for _, shred := range shreds {
		data, ok := shred.Data()
		if !ok {
			return nil, fmt.Errorf("invalid data shred")
		}
		buf.Write(data)
	}
	// TODO Some empty shred handling idk

	return buf.Bytes(), nil
}
