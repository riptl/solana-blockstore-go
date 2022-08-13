// Package blockstore is a read-only client for the Solana blockstore database.
//
// For the reference implementation in Rust, see here:
// https://docs.rs/solana-ledger/latest/solana_ledger/blockstore/struct.Blockstore.html
//
// Supported Solana versions: v1.12.0
package blockstore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/linxGnu/grocksdb"
	"github.com/terorie/solana-blockstore-go/shred"
	"golang.org/x/exp/constraints"
)

// DB wraps a RocksDB database handle.
type DB struct {
	db *grocksdb.DB

	cfMeta        *grocksdb.ColumnFamilyHandle
	cfRoot        *grocksdb.ColumnFamilyHandle
	cfDeadSlots   *grocksdb.ColumnFamilyHandle
	cfBlockHeight *grocksdb.ColumnFamilyHandle
	cfDataShred   *grocksdb.ColumnFamilyHandle
	cfCodeShred   *grocksdb.ColumnFamilyHandle
}

// Column families
const (
	CfDefault     = "default"
	CfMeta        = "meta"
	CfRoot        = "root"
	CfDeadSlots   = "dead_slots"
	CfBlockHeight = "block_height"
	CfDataShred   = "data_shred"
	CfCodeShred   = "code_shred"
)

// ErrNotFound is returned when no row is found.
var ErrNotFound = errors.New("not found")

var ErrDeadSlot = errors.New("dead slot")

var ErrInvalidShredData = errors.New("invalid shred data")

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
	CfDeadSlots,
	CfBlockHeight,
	CfDataShred,
	CfCodeShred,
}

func getOpts() (opts *grocksdb.Options, cfNames []string, cfOpts []*grocksdb.Options) {
	opts = grocksdb.NewDefaultOptions()
	cfNames = columnFamilyNames
	cfOpts = []*grocksdb.Options{
		grocksdb.NewDefaultOptions(), // CfDefault
		grocksdb.NewDefaultOptions(), // CfMeta
		grocksdb.NewDefaultOptions(), // CfRoot
		grocksdb.NewDefaultOptions(), // CfDeadSlots
		grocksdb.NewDefaultOptions(), // CfBlockHeight
		grocksdb.NewDefaultOptions(), // CfDataShred
		grocksdb.NewDefaultOptions(), // CfCodeShred
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
		cfDeadSlots:   cfHandles[3],
		cfBlockHeight: cfHandles[4],
		cfDataShred:   cfHandles[5],
		cfCodeShred:   cfHandles[6],
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
		return 0, ErrNotFound
	}
	return ParseSlotKey(iter.Key().Data())
}

// GetBlockHeight returns the last known root slot.
func (d *DB) GetBlockHeight() (uint64, error) {
	opts := grocksdb.NewDefaultReadOptions()
	iter := d.db.NewIteratorCF(opts, d.cfBlockHeight)
	defer iter.Close()
	iter.SeekToLast()
	if !iter.Valid() {
		return 0, ErrNotFound
	}
	return binary.LittleEndian.Uint64(iter.Value().Data()), nil
}

func ParseSlotKey(key []byte) (uint64, error) {
	return binary.BigEndian.Uint64(key), nil
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
	return GetBincode[SlotMeta](d.db, d.cfMeta, key[:])
}

// MultiGetSlotMeta does multiple GetSlotMeta calls.
func (d *DB) MultiGetSlotMeta(slots ...uint64) ([]*SlotMeta, error) {
	keys := make([][]byte, len(slots))
	for i, slot := range slots {
		key := MakeSlotKey(slot)
		keys[i] = key[:] // heap escape
	}
	return MultiGetBincode[SlotMeta](d.db, d.cfMeta, keys...)
}

// IterSlotMetas creates an iterator over CfMeta.
//
// Use MakeSlotKey to seek to a specific slot.
//
// It's the caller's responsibility to close the iterator.
func (d *DB) IterSlotMetas(opts *grocksdb.ReadOptions) IterBincode[SlotMeta] {
	if opts == nil {
		opts = grocksdb.NewDefaultReadOptions()
	}
	rawIter := d.db.NewIteratorCF(opts, d.cfMeta)
	return IterBincode[SlotMeta]{Iterator: rawIter}
}

func (d *DB) IsSlotDead(slot uint64) (bool, error) {
	opts := grocksdb.NewDefaultReadOptions()
	key := MakeSlotKey(slot)
	res, err := d.db.GetCF(opts, d.cfDeadSlots, key[:])
	if err != nil {
		return false, err
	}
	return res.Exists() && bytes.Equal(res.Data(), []byte{1}), nil
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

func (d *DB) GetBlock(slot uint64) (*Block, error) {
	// TODO Retrieving slot meta twice, which sucks
	meta, err := d.GetSlotMeta(slot)
	if err != nil {
		return nil, err
	}
	if !meta.IsFull() {
		return nil, ErrNotFound
	}
	entries, _, _, err := d.GetSlotEntries(slot, 0, false)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, ErrNotFound
	}
	blockHash := entries[len(entries)-1].Hash
	var txns []solana.Transaction
	for _, entry := range entries {
		txns = append(txns, entry.Transactions...)
	}
	block := &Block{
		BlockHash:    blockHash,
		ParentSlot:   meta.ParentSlot,
		Transactions: txns,
	}
	return block, nil
}

// GetSlotEntries returns the entry vector for the slot starting
// with `shred_start_index`, the number of shreds that comprise the entry
// vector, and whether the slot is full (consumed all shreds).
//
// See https://docs.rs/solana-ledger/latest/solana_ledger/blockstore/struct.Blockstore.html#method.get_slot_entries_with_shred_info
func (d *DB) GetSlotEntries(
	slot uint64,
	startIndex uint64,
	allowDeadSlots bool,
) (entries []Entry, numShreds uint64, isFull bool, err error) {
	completedRanges, slotMeta, err := d.getCompletedRanges(slot, startIndex)
	if err != nil {
		return nil, 0, false, err
	}

	if allowDeadSlots {
		isDead, err := d.IsSlotDead(slot)
		if err != nil {
			return nil, 0, false, err
		}
		if isDead {
			return nil, 0, false, ErrDeadSlot
		}
	}

	if len(completedRanges) > 0 {
		numShreds = uint64(completedRanges[len(completedRanges)-1].EndIndex) - startIndex + 1
	}

	// TODO parallel
	for _, completed := range completedRanges {
		subEntries, err := d.GetEntriesInDataBlock(slot, completed.StartIndex, completed.EndIndex)
		if err != nil {
			return entries, numShreds, false, err
		}
		entries = append(entries, subEntries...)
	}

	isFull = slotMeta.IsFull()
	return
}

func (d *DB) getCompletedRanges(slot uint64, startIndex uint64) ([]CompletedRange, *SlotMeta, error) {
	// The validator locks here to prevent purges.
	// We're not in the validator's memory space, so we cannot acquire a lock here.
	meta, err := d.GetSlotMeta(slot)
	if errors.Is(err, ErrNotFound) {
		return nil, meta, nil // ok
	} else if err != nil {
		return nil, nil, err
	}
	// Find all the ranges for the completed data blocks
	completedRanges := getCompletedDataRanges(uint32(startIndex), meta.CompletedDataIndexes, uint32(meta.Consumed))
	return completedRanges, meta, nil
}

// Get the range of indexes [start_index, end_index] of every completed data block
func getCompletedDataRanges(
	startIndex uint32,
	completedDataIndexes []uint32,
	consumed uint32,
) []CompletedRange {
	completedDataIndexes = sliceSortedByRange(completedDataIndexes, startIndex, consumed)
	var ranges []CompletedRange
	begin := startIndex
	for _, index := range completedDataIndexes {
		ranges = append(ranges, CompletedRange{begin, index})
		begin++
	}
	return ranges
}

func (d *DB) GetEntriesInDataBlock(slot uint64, startIndex uint32, endIndex uint32) ([]Entry, error) {
	iter := d.db.NewIteratorCF(grocksdb.NewDefaultReadOptions(), d.cfDataShred)
	key := MakeShredKey(slot, uint64(startIndex))
	iter.Seek(key[:])
	var shreds []shred.Shred
	for i := uint64(startIndex); i <= uint64(endIndex); i++ {
		var slot, index uint64
		valid := iter.Valid()
		if valid {
			slot = binary.BigEndian.Uint64(iter.Key().Data())
			index = binary.BigEndian.Uint64(iter.Key().Data()[8:])
		}
		if !valid || index != i {
			return nil, fmt.Errorf("%w: missing shred for slot %d, index %d", ErrInvalidShredData, slot, index)
		}
		s := shred.NewShredFromSerialized(iter.Value().Data())
		if s == nil {
			return nil, fmt.Errorf("failed to deserialize shred %d/%d", slot, i)
		}
		shreds = append(shreds, s)
	}

	payload, err := shred.Deshred(shreds)
	if err != nil {
		return nil, err
	}

	var entries struct {
		Count   uint64 `bin:"sizeof=Entries"`
		Entries []Entry
	}
	dec := bin.NewBinDecoder(payload)
	err = dec.Decode(&entries)
	return entries.Entries, err
}

func sliceSortedByRange[T constraints.Ordered](list []T, start T, stop T) []T {
	for len(list) > 0 && list[0] < start {
		list = list[1:]
	}
	for len(list) > 0 && list[len(list)-1] >= stop {
		list = list[:len(list)-1]
	}
	return list
}
