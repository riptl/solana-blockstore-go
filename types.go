package blockstore

type SlotMeta struct {
	Slot                    uint64   `yaml:"-"`
	Consumed                uint64   `yaml:"consumed"`
	Received                uint64   `yaml:"received"`
	FirstShredTimestamp     uint64   `yaml:"first_shred_timestamp"`
	LastIndex               uint64   `yaml:"last_index"`
	ParentSlot              uint64   `yaml:"parent_slot"`
	NumNextSlots            uint64   `bin:"sizeof=NextSlots" yaml:"-"`
	NextSlots               []uint64 `yaml:"next_slots"`
	IsConnected             bool     `yaml:"is_connected"`
	NumCompletedDataIndexes uint64   `bin:"sizeof=CompletedDataIndexes" yaml:"-"`
	CompletedDataIndexes    []uint32 `yaml:"completed_data_indexes"`
}
