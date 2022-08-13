// Ledgertool is basic Go counterpart to solana-ledger-tool.
package main

import (
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/dfuse-io/logging"
	"github.com/linxGnu/grocksdb"
	"github.com/segmentio/textio"
	"github.com/spf13/pflag"
	blockstore "github.com/terorie/solana-blockstore-go"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

func main() {
	var (
		flagDBPath             string
		flagListColumnFamilies bool
		flagRoot               bool
		flagHeight             bool
		flagAllSlots           bool
		flagSlotMetas          []uint
		flagBlock              uint64
		flagGetDataShred       string
		flagGetCodeShred       string
	)

	pflag.Usage = func() {
		fmt.Fprint(flag.CommandLine.Output(), `USAGE
    ledgertool extracts info from a Solana ledger blockstore (RocksDB).
    Requested info is dumped in YAML format.

AUTHOR
    Richard Patel <me@terorie.dev>

FLAGS
`)
		pflag.PrintDefaults()
	}
	pflag.StringVar(&flagDBPath, "db", "", "Path to ledger/rocksdb dir (required)")
	pflag.BoolVar(&flagListColumnFamilies, "list-cfs", false, "List column families")
	pflag.BoolVar(&flagRoot, "root", false, "Show root slot")
	pflag.BoolVar(&flagHeight, "height", false, "Show block height")
	pflag.BoolVar(&flagAllSlots, "all-slots", false, "Get all slot metadatas")
	pflag.UintSliceVar(&flagSlotMetas, "slot", nil, "Get slot metadata")
	pflag.Uint64Var(&flagBlock, "block", 0, "Get block")
	pflag.StringVar(&flagGetDataShred, "data-shreds", "", "Dump data shreds (space-separated list of `slot` or `slot:index`)")
	pflag.StringVar(&flagGetCodeShred, "coding-shreds", "", "Dump coding shreds")
	pflag.Parse()

	if pflag.NArg() > 0 {
		flag.Usage()
		os.Exit(2)
	}
	if flagDBPath == "" {
		flag.Usage()
		fmt.Fprintln(flag.CommandLine.Output(), "missing --db flag")
		os.Exit(2)
	}

	logConfig := zap.NewDevelopmentConfig()
	logConfig.Level.SetLevel(zap.DebugLevel)
	log, err := logConfig.Build()
	if err != nil {
		panic(err)
	}
	logging.Set(log)

	if flagListColumnFamilies {
		listColumnFamilies(flagDBPath)
	}

	db, err := blockstore.OpenReadOnly(flagDBPath)
	if err != nil {
		log.Fatal("Failed to open blockstore", zap.Error(err))
	}
	defer db.Close()

	ok := true

	if flagRoot {
		ok = ok && showRoot(db)
	}
	if flagHeight {
		ok = ok && showBlockHeight(db)
	}
	if flagAllSlots {
		ok = ok && getAllSlotMetas(db)
	} else if len(flagSlotMetas) > 0 {
		ok = ok && getSlotMetas(db, flagSlotMetas)
	}
	if flagBlock != 0 {
		ok = ok && getBlock(db, flagBlock)
	}
	if flagGetDataShred != "" {
		ok = ok && getShreds(db, flagGetDataShred, false)
	}
	if flagGetCodeShred != "" {
		ok = ok && getShreds(db, flagGetDataShred, true)
	}

	if !ok {
		os.Exit(1)
	}
}

func listColumnFamilies(path string) bool {
	opts := grocksdb.NewDefaultOptions()
	names, err := grocksdb.ListColumnFamilies(opts, path)
	if err != nil {
		log.Print("Failed to list column families: ", err)
		return false
	}
	fmt.Println("column_families:")
	for _, name := range names {
		fmt.Println("  - " + name)
	}
	return true
}

func showRoot(db *blockstore.DB) bool {
	root, err := db.MaxRoot()
	if err != nil {
		log.Print("Failed to get root: ", err)
		return false
	}
	fmt.Println("root:", root)
	return true
}

func showBlockHeight(db *blockstore.DB) bool {
	height, err := db.GetBlockHeight()
	if err != nil {
		log.Print("Failed to get block height: ", err)
		return false
	}
	fmt.Println("block_height:", height)
	return true
}

func parseShredIndex(shredStr string) (slot, index uint64, ok bool) {
	sep := strings.IndexRune(shredStr, ':')
	if sep < 0 {
		return
	}
	var err error
	if slot, err = strconv.ParseUint(shredStr[:sep], 10, 64); err != nil {
		return
	}
	if index, err = strconv.ParseUint(shredStr[sep+1:], 10, 64); err != nil {
		return
	}
	ok = true
	return
}

func getAllSlotMetas(db *blockstore.DB) (ok bool) {
	ok = true
	iter := db.IterSlotMetas(grocksdb.NewDefaultReadOptions())
	defer iter.Close()

	// Get low bound
	var lowSlot, highSlot uint64
	iter.SeekToFirst()
	if iter.Valid() {
		lowSlot, _ = blockstore.ParseSlotKey(iter.Key().Data())
	}

	// Collect all slots to map
	metaMap := make(map[uint64]*blockstore.SlotMeta)
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		slot, err := blockstore.ParseSlotKey(iter.Key().Data())
		if err != nil {
			log.Printf("Ignoring slot meta key: %x", iter.Key().Data())
			ok = false
			continue
		}
		meta, err := iter.Element()
		if err != nil {
			log.Printf("While ranging slot metas (%x): %s", iter.Key().Data(), err)
			ok = false
			continue
		}
		metaMap[slot] = meta
	}

	// Get high bound
	iter.SeekToLast()
	if iter.Valid() {
		highSlot, _ = blockstore.ParseSlotKey(iter.Key().Data())
	}
	fmt.Println("slot_meta_range:")
	fmt.Println("  first:", lowSlot)
	fmt.Println("  last:", highSlot)

	dumpSlots(metaMap)
	return ok
}

func getSlotMetas(db *blockstore.DB, slots []uint) bool {
	slots64 := make([]uint64, len(slots))
	for i, s := range slots {
		slots64[i] = uint64(s)
	}

	metas, err := db.MultiGetSlotMeta(slots64...)
	if err != nil {
		log.Println("Failed to get slot metas:", err)
	}
	fmt.Println("slot_meta")

	metaMap := make(map[uint64]*blockstore.SlotMeta)
	for i, meta := range metas {
		metaMap[slots64[i]] = meta
	}
	dumpSlots(metaMap)
	return true
}

func dumpSlots(metaMap map[uint64]*blockstore.SlotMeta) {
	fmt.Println("slots:")
	enc := yaml.NewEncoder(textio.NewPrefixWriter(os.Stdout, "  "))
	enc.SetIndent(2)
	if err := enc.Encode(metaMap); err != nil {
		panic(err.Error())
	}
}

func getBlock(db *blockstore.DB, slot uint64) bool {
	block, err := db.GetBlock(slot)
	if err != nil {
		log.Printf("Failed to get block %d: %s", slot, err)
		return false
	}

	// super ugly but whatever
	// Need this hack to have instruction data ([]byte) serialized as base64, not a massive byte-by-byte list
	blockStr := jsonStr(block)
	var x any
	_ = json.Unmarshal([]byte(blockStr), &x)
	fmt.Println("blocks:")
	fmt.Printf("  %d:\n", slot)
	enc := yaml.NewEncoder(textio.NewPrefixWriter(os.Stdout, "    "))
	enc.SetIndent(2)
	enc.Encode(x)
	return true
}

func getShreds(db *blockstore.DB, shredStr string, coding bool) bool {
	slot, index, ok := parseShredIndex(shredStr)
	if !ok {
		log.Print("Invalid data shred index: ", shredStr)
		return false
	}

	var shred *grocksdb.Slice
	var err error
	if coding {
		shred, err = db.GetCodingShred(slot, index)
	} else {
		shred, err = db.GetDataShred(slot, index)
	}
	if err != nil {
		log.Printf("Can't get shred %s: %s", shredStr, err)
		return false
	}
	if !shred.Exists() {
		log.Println("No such shred:", shredStr)
		return false
	}
	defer shred.Free()

	var shredType string
	if coding {
		shredType = "coding_shred"
	} else {
		shredType = "data_shred"
	}

	fmt.Printf(`%s:
  %s: |
    %s
`,
		shredType,
		jsonStr(shredStr),
		base64.StdEncoding.EncodeToString(shred.Data()))

	return true
}

func jsonStr(v any) string {
	buf, _ := json.Marshal(v)
	return string(buf)
}
