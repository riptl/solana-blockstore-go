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

	"github.com/linxGnu/grocksdb"
)

// DB wraps a RocksDB database handle.
type DB struct {
	db *grocksdb.DB

	cfMeta        *grocksdb.ColumnFamilyHandle
	cfRoot        *grocksdb.ColumnFamilyHandle
	cfBlockHeight *grocksdb.ColumnFamilyHandle
	cfDataShred   *grocksdb.ColumnFamilyHandle
	cfCodeShred   *grocksdb.ColumnFamilyHandle
}

// Column families
const (
	CfDefault     = "default"
	CfMeta        = "meta"
	CfRoot        = "root"
	CfBlockHeight = "block_height"
	CfDataShred   = "data_shred"
	CfCodeShred   = "code_shred"
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
	CfBlockHeight,
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
		db:            rawDB,
		cfMeta:        cfHandles[1],
		cfRoot:        cfHandles[2],
		cfBlockHeight: cfHandles[3],
		cfDataShred:   cfHandles[4],
		cfCodeShred:   cfHandles[5],
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
	iter := d.db.NewIteratorCF(opts, d.cfBlockHeight)
	defer iter.Close()
	iter.SeekToLast()
	if !iter.Valid() {
		return 0, ErrNoRow
	}
	return binary.LittleEndian.Uint64(iter.Value().Data()), nil
}

func parseSlotKey(key *grocksdb.Slice) (uint64, error) {
	return binary.BigEndian.Uint64(key.Data()), nil
}

// MakeSlotKey creates the RocksDB key for CfMeta, CfRoot.
func MakeSlotKey(slot uint64) (key [8]byte) {
	binary.BigEndian.PutUint64(key[0:8], slot)
	return
}

// MakeShredKey creates the RocksDB key for CfDataShred or CfCodeShred.
func MakeShredKey(slot, index uint64) (key [16]byte) {
	binary.BigEndian.PutUint64(key[0:8], slot)
	binary.BigEndian.PutUint64(key[8:16], index)
	return
}

// GetSlotMeta returns the shredding metadata of a given slot.
func (d *DB) GetSlotMeta(slot uint64) (*SlotMeta, error) {
	key := MakeSlotKey(slot)
	return getBincode[SlotMeta](d.db, d.cfMeta, key[:])
}

// GetDataShred returns the content of a given data shred.
func (d *DB) GetDataShred(slot, index uint64) (*grocksdb.Slice, error) {
	opts := grocksdb.NewDefaultReadOptions()
	key := MakeShredKey(slot, index)
	return d.db.GetCF(opts, d.cfDataShred, key[:])
}

// GetCodingShred returns the content of a given coding shred.
func (d *DB) GetCodingShred(slot, index uint64) (*grocksdb.Slice, error) {
	opts := grocksdb.NewDefaultReadOptions()
	key := MakeShredKey(slot, index)
	return d.db.GetCF(opts, d.cfCodeShred, key[:])
}

// IterDataShreds creates an iterator over CfDataShred.
//
// Use MakeSlotKey to construct a prefix,
// or MakeShredKey to seek to a specific shred.
//
// It's the caller's responsibility to close the iterator.
func (d *DB) IterDataShreds(opts *grocksdb.ReadOptions) *grocksdb.Iterator {
	return d.iterShreds(opts, d.cfDataShred)
}

// IterCodingShreds creates an iterator over CfCodeShred.
//
// Use MakeSlotKey to construct a prefix,
// or MakeShredKey to seek to a specific shred.
//
// It's the caller's responsibility to close the iterator.
func (d *DB) IterCodingShreds(opts *grocksdb.ReadOptions) *grocksdb.Iterator {
	return d.iterShreds(opts, d.cfCodeShred)
}

func (d *DB) iterShreds(opts *grocksdb.ReadOptions, cf *grocksdb.ColumnFamilyHandle) *grocksdb.Iterator {
	if opts == nil {
		opts = grocksdb.NewDefaultReadOptions()
	}
	return d.db.NewIteratorCF(opts, cf)
}
