package blockstore

import (
	bin "github.com/gagliardetto/binary"
	"github.com/linxGnu/grocksdb"
)

func parseBincode[T any](data []byte) (*T, error) {
	dec := bin.NewBinDecoder(data)
	val := new(T)
	err := dec.Decode(val)
	return val, err
}

func getBincode[T any](db *grocksdb.DB, cf *grocksdb.ColumnFamilyHandle, key []byte) (*T, error) {
	opts := grocksdb.NewDefaultReadOptions()
	res, err := db.GetCF(opts, cf, key[:])
	if err != nil {
		return nil, err
	}
	if !res.Exists() {
		return nil, ErrNoRow
	}
	defer res.Free()
	return parseBincode[T](res.Data())
}
