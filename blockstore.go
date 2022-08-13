// Package blockstore is a read-only client for the Solana blockstore database.
//
// For the reference implementation in Rust, see here:
// https://docs.rs/solana-ledger/latest/solana_ledger/blockstore_db/struct.Database.html
//
// Supported Solana versions: v1.12.0
//
// Supported columns: meta, root
package blockstore

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/gagliardetto/binary"
	"github.com/linxGnu/grocksdb"
)

// DB wraps a RocksDB database handle.
type DB struct {
	db *grocksdb.DB

	cfMeta      *grocksdb.ColumnFamilyHandle
	cfRoot      *grocksdb.ColumnFamilyHandle
	cfDataShred *grocksdb.ColumnFamilyHandle
	cfCodeShred *grocksdb.ColumnFamilyHandle
}

const (
	CfDefault   = "default"
	CfMeta      = "meta"
	CfRoot      = "root"
	CfDataShred = "data_shred"
	CfCodeShred = "code_shred"
)

// ErrNoRow is returned when no row is found.
var ErrNoRow = errors.New("not found")

// OpenReadOnly attaches to a blockstore in read-only mode.
//
// Attaching to running validators is supported but the DB will only be a
// point-in-time view at the time of attaching.
func OpenReadOnly(path string) (*DB, error) {
	opts, cfNames, cfOpts := getOpts()

	rawDB, cfHandles, err := grocksdb.OpenDbForReadOnlyColumnFamilies(
		opts,
		path,
		cfNames,
		cfOpts,
		/*errorIfWalFileExists*/ false,
	)
	if err != nil {
		return nil, err
	}

	return newDB(rawDB, cfHandles)
}

// OpenSecondary attaches to a blockstore in secondary mode.
//
// Only read operations are allowed.
// Unlike OpenReadOnly, allows the user to catch up the DB using DB.TryCatchUpWithPrimary.
//
// `secondaryPath` points to a directory where the secondary instance stores its info log.
func OpenSecondary(path string, secondaryPath string) (*DB, error) {
	opts, cfNames, cfOpts := getOpts()

	rawDB, cfHandles, err := grocksdb.OpenDbAsSecondaryColumnFamilies(
		opts,
		path,
		secondaryPath,
		cfNames,
		cfOpts,
	)
	if err != nil {
		return nil, err
	}

	return newDB(rawDB, cfHandles)
}

var columnFamilyNames = []string{
	CfDefault,
	CfMeta,
	CfRoot,
	CfDataShred,
	CfCodeShred,
}

func getOpts() (opts *grocksdb.Options, cfNames []string, cfOpts []*grocksdb.Options) {
	opts = grocksdb.NewDefaultOptions()
	cfNames = columnFamilyNames
	cfOpts = []*grocksdb.Options{
		grocksdb.NewDefaultOptions(),
		grocksdb.NewDefaultOptions(),
		grocksdb.NewDefaultOptions(),
		grocksdb.NewDefaultOptions(),
		grocksdb.NewDefaultOptions(),
	}
	return
}

func newDB(rawDB *grocksdb.DB, cfHandles []*grocksdb.ColumnFamilyHandle) (*DB, error) {
	if len(columnFamilyNames) != len(cfHandles) {
		rawDB.Close()
		return nil, fmt.Errorf("unexpected number of column families: %d", len(cfHandles))
	}
	db := &DB{
		db:          rawDB,
		cfMeta:      cfHandles[1],
		cfRoot:      cfHandles[2],
		cfDataShred: cfHandles[3],
		cfCodeShred: cfHandles[4],
	}
	return db, nil
}

// TryCatchUpWithPrimary updates the client's view of the database with the latest information.
//
// Only works with DB opened using OpenSecondary.
func (d *DB) TryCatchUpWithPrimary() error {
	return d.db.TryCatchUpWithPrimary()
}

// Close releases the RocksDB client.
func (d *DB) Close() {
	d.db.Close()
}

// MaxRoot returns the last known root slot.
func (d *DB) MaxRoot() (uint64, error) {
	opts := grocksdb.NewDefaultReadOptions()
	iter := d.db.NewIteratorCF(opts, d.cfRoot)
	defer iter.Close()
	iter.SeekToLast()
	if !iter.Valid() {
		return 0, ErrNoRow
	}
	return parseSlotKey(iter.Key())
}

// GetBlockHeight returns the last known root slot.
func (d *DB) GetBlockHeight() (uint64, error) {
	opts := grocksdb.NewDefaultReadOptions()
	iter := d.db.NewIteratorCF(opts, d.cfRoot)
	defer iter.Close()
	iter.SeekToLast()
	if !iter.Valid() {
		return 0, ErrNoRow
	}
	return parseSlotKey(iter.Key())
}

func parseSlotKey(key *grocksdb.Slice) (uint64, error) {
	return binary.BigEndian.Uint64(key.Data()), nil
}

func makeSlotKey(slot uint64) (key [8]byte) {
	binary.BigEndian.PutUint64(key[0:8], slot)
	return
}

func makeShredKey(slot, index uint64) (key [16]byte) {
	binary.BigEndian.PutUint64(key[0:8], slot)
	binary.BigEndian.PutUint64(key[8:16], index)
	return
}

func (d *DB) GetSlotMeta(slot uint64) (*SlotMeta, error) {
	opts := grocksdb.NewDefaultReadOptions()
	key := makeSlotKey(slot)
	res, err := d.db.GetCF(opts, d.cfMeta, key[:])
	if err != nil {
		return nil, err
	}
	if !res.Exists() {
		return nil, ErrNoRow
	}
	defer res.Free()

	dec := bin.NewBinDecoder(res.Data())
	meta := new(SlotMeta)
	err = dec.Decode(meta)
	return meta, err
}

func (d *DB) GetDataShred(slot, index uint64) (*grocksdb.Slice, error) {
	opts := grocksdb.NewDefaultReadOptions()
	key := makeShredKey(slot, index)
	return d.db.GetCF(opts, d.cfDataShred, key[:])
}

/*
func (d *DB) GetDataShreds(slot, fromIndex, toIndex uint64) ([]byte, error) {
	opts := grocksdb.NewDefaultReadOptions()
	key := makeShredKey(slot, index)
	return d.db.GetCF(opts, d.cfDataShred, key[:])
}
*/
