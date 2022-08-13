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
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
	"terorie.dev/solana/blockstore"
)

func main() {
	var (
		flagDBPath             string
		flagListColumnFamilies bool
		flagRoot               bool
		flagHeight             bool
		flagGetDataShred       string
		flagSlotMeta           uint64
	)

	flag.Usage = func() {
		fmt.Fprint(flag.CommandLine.Output(), `USAGE
    ledgertool extracts info from a Solana ledger blockstore (RocksDB).
    Requested info is dumped in YAML format.

AUTHOR
    Richard Patel <me@terorie.dev>

FLAGS
`)
		flag.PrintDefaults()
	}
	flag.StringVar(&flagDBPath, "db", "", "Path to ledger/rocksdb dir (required)")
	flag.BoolVar(&flagListColumnFamilies, "list-cfs", false, "List column families")
	flag.BoolVar(&flagRoot, "root", false, "Show root slot")
	flag.BoolVar(&flagHeight, "height", false, "Show block height")
	flag.Uint64Var(&flagSlotMeta, "slot", 0, "Get slot metadata")
	flag.StringVar(&flagGetDataShred, "data-shred", "", "Dump specific data shred (slot:index)")
	flag.Parse()

	if flagDBPath == "" {
		fmt.Fprintln(flag.CommandLine.Output(), "missing -db flag")
		flag.Usage()
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
	if flagSlotMeta > 0 {
		ok = ok && getSlotMeta(db, flagSlotMeta)
	}
	if flagGetDataShred != "" {
		ok = ok && getDataShred(db, flagGetDataShred)
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
	if slot, err = strconv.ParseUint(shredStr[:sep-1], 10, 64); err != nil {
		return
	}
	if index, err = strconv.ParseUint(shredStr[sep+1:], 10, 64); err != nil {
		return
	}
	ok = true
	return
}

func getSlotMeta(db *blockstore.DB, slot uint64) bool {
	meta, err := db.GetSlotMeta(slot)
	if err != nil {
		log.Printf("Failed to get slot meta %d: %s\n", slot, err)
	}
	fmt.Printf(`slot_meta:
  %d:
`, slot)
	enc := yaml.NewEncoder(textio.NewPrefixWriter(os.Stdout, "    "))
	enc.SetIndent(2)
	defer enc.Close()
	if err := enc.Encode(meta); err != nil {
		panic(err.Error())
	}
	return true
}

func getDataShred(db *blockstore.DB, shredStr string) bool {
	slot, index, ok := parseShredIndex(shredStr)
	if !ok {
		log.Print("Invalid data shred index: ", shredStr)
		return false
	}

	shred, err := db.GetDataShred(slot, index)
	if err != nil {
		log.Printf("Can't get shred %s: %s", shredStr, err)
		return false
	}
	if !shred.Exists() {
		log.Println("No such shred:", shredStr)
		return false
	}
	defer shred.Free()

	fmt.Printf(`data_shred:
  %s: |
    %s
`, jsonStr(shredStr), base64.StdEncoding.EncodeToString(shred.Data()))

	return true
}

func jsonStr(v any) string {
	buf, _ := json.Marshal(v)
	return string(buf)
}
