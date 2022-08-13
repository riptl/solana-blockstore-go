package blockstore

import "github.com/linxGnu/grocksdb"

type IterBincode[T any] struct {
	*grocksdb.Iterator
}

func (i IterBincode[T]) Element() (*T, error) {
	return parseBincode[T](i.Value().Data())
}
