package blockstore

import (
	"math"

	"github.com/gagliardetto/solana-go"
)

type SlotMeta struct {
	Slot                    uint64   `yaml:"-"`
	Consumed                uint64   `yaml:"consumed"`
	Received                uint64   `yaml:"received"`
	FirstShredTimestamp     uint64   `yaml:"first_shred_timestamp"`
	LastIndex               uint64   `yaml:"last_index"`  // optional, None being math.MaxUint64
	ParentSlot              uint64   `yaml:"parent_slot"` // optional, None being math.MaxUint64
	NumNextSlots            uint64   `bin:"sizeof=NextSlots" yaml:"-"`
	NextSlots               []uint64 `yaml:"next_slots"`
	IsConnected             bool     `yaml:"is_connected"`
	NumCompletedDataIndexes uint64   `bin:"sizeof=CompletedDataIndexes" yaml:"-"`
	CompletedDataIndexes    []uint32 `yaml:"completed_data_indexes"`
}

func (s *SlotMeta) IsFull() bool {
	// last_index is math.MaxUint64 when it has no information
	// about how many shreds will fill this slot.
	if s.LastIndex == math.MaxUint64 {
		return false
	}
	return s.Consumed == s.LastIndex+1
}

type Block struct {
	BlockHash    solana.Hash
	ParentSlot   uint64
	Transactions []solana.Transaction
}

type CompletedRange struct {
	StartIndex uint32
	EndIndex   uint32
}

type Entry struct {
	NumHashes    uint64               `yaml:"num_hashes"`
	Hash         solana.Hash          `yaml:"hash"`
	NumTxns      uint64               `bin:"sizeof=Transactions" yaml:"-"`
	Transactions []solana.Transaction `yaml:"transactions"`
}
