// Ledgertool is basic Go counterpart to solana-ledger-tool.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/linxGnu/grocksdb"
	"terorie.dev/solana/blockstore"
)

func main() {
	var (
		flagDBPath             string
		flagListColumnFamilies bool
		flagRoot               bool
	)

	flag.StringVar(&flagDBPath, "db", "", "Path to ledger/rocksdb dir")
	flag.BoolVar(&flagListColumnFamilies, "list-cfs", false, "List column families")
	flag.BoolVar(&flagRoot, "root", false, "Show root slow")
	flag.Parse()

	if flagDBPath == "" {
		flag.Usage()
		os.Exit(1)
	}

	if flagListColumnFamilies {
		listColumnFamilies(flagDBPath)
	}

	db, err := blockstore.OpenReadOnly(flagDBPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if flagRoot {
		showRoot(db)
	}
}

func listColumnFamilies(path string) {
	opts := grocksdb.NewDefaultOptions()
	names, err := grocksdb.ListColumnFamilies(opts, path)
	if err != nil {
		log.Print("Failed to list column families: ", err)
		return
	}
	fmt.Println("Column Families:")
	for _, name := range names {
		fmt.Println("  " + name)
	}
}

func showRoot(db *blockstore.DB) {
	root, err := db.MaxRoot()
	if err != nil {
		log.Print("Failed to get root: ", err)
		return
	}
	fmt.Println("Root slot:", root)
}
