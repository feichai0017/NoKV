package main

import (
	"fmt"
	"log"
	"os"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/utils"
)

func main() {
	workDir := "./no-kv-iter-demo"
	defer os.RemoveAll(workDir)

	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = workDir
	db := NoKV.Open(opt)
	defer db.Close()

	// 1. Bulk insert some ordered data.
	prefix := "product:"
	products := map[string]string{
		"001": "Apple",
		"002": "Banana",
		"003": "Cherry",
		"004": "Date",
		"005": "Elderberry",
	}

	fmt.Println("Seeding data...")
	for id, name := range products {
		key := []byte(prefix + id)
		if err := db.Set(key, []byte(name)); err != nil {
			log.Fatal(err)
		}
	}

	// 2. Create an iterator.
	// Configure iterator options; IsAsc=true means forward order.
	iterOpt := &utils.Options{
		IsAsc: true,
	}
	iter := db.NewIterator(iterOpt)
	defer iter.Close()

	// 3. Range scan.
	// Find all keys prefixed with "product:".
	// Seek jumps to the first key >= the target.
	startKey := []byte(prefix)
	fmt.Println("\nScanning all products:")

	iter.Seek(startKey)
	if !iter.Valid() {
		fmt.Println("Iterator is invalid immediately after Seek.")
	}

	for ; iter.Valid(); iter.Next() {
		item := iter.Item()

		// Verify the key still has the prefix; otherwise we're out of range.
		// (All data matches the prefix here, but real workloads may not.)
		// Note: NoKV iterators return entries with the user key already decoded.
		entry := item.Entry()
		keyStr := string(entry.Key)
		valStr := string(entry.Value)

		// Simple prefix check.
		if len(keyStr) < len(prefix) || keyStr[:len(prefix)] != prefix {
			fmt.Printf("Stopping at key: %s (prefix mismatch)\n", keyStr)
			entry.DecrRef()
			break
		}

		fmt.Printf("- %s: %s\n", keyStr, valStr)

		// Release the entry reference.
		entry.DecrRef()
	}

	// Note: NoKV's MemTable implementation (SkipList) currently ignores the IsAsc option
	// and only supports forward iteration. Reverse scanning will not work correctly
	// for data that is still in memory. It might work for on-disk SSTables if supported there.
	// For this example, we skip the reverse scan demo.

	/*
		// 4. Reverse scan - not currently supported for MemTables.
		fmt.Println("\nReverse scanning (Top 2):")
		iterDesc := db.NewIterator(&utils.Options{IsAsc: false})
		defer iterDesc.Close()

		// Start from the last possible key (byte after product:005).
		iterDesc.Seek([]byte(prefix + "\xff"))

		count := 0
		for ; iterDesc.Valid(); iterDesc.Next() {
			entry := iterDesc.Item().Entry()
			keyStr := string(entry.Key)
			if len(keyStr) < len(prefix) || keyStr[:len(prefix)] != prefix {
				entry.DecrRef()
				break
			}

			fmt.Printf("- %s: %s\n", keyStr, string(entry.Value))
			entry.DecrRef()

			count++
			if count >= 2 {
				break
			}
		}
	*/
}
