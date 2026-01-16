package main

import (
	"fmt"
	"log"
	"os"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/utils"
)

func main() {
	// 1. Configure options.
	// Use a temporary directory as the workdir for this demo.
	workDir := "./no-kv-basic-demo"
	defer os.RemoveAll(workDir) // Clean up demo data; not required in production.

	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = workDir

	// 2. Open the database.
	fmt.Println("Opening DB...")
	db := NoKV.Open(opt)
	// Always close the DB on exit to flush data to disk.
	defer db.Close()

	// 3. Write data (Set).
	key := []byte("user:1001")
	value := []byte("Alice")
	fmt.Printf("Writing key='%s', value='%s'\n", key, value)

	if err := db.Set(key, value); err != nil {
		log.Fatalf("Set failed: %v", err)
	}

	// 4. Read data (Get).
	fmt.Println("Reading data...")
	entry, err := db.Get(key)
	if err != nil {
		log.Fatalf("Get failed: %v", err)
	}
	defer entry.DecrRef() // Important: release entry references when done.

	fmt.Printf("Read success! key='%s', value='%s', version=%d\n",
		entry.Key, entry.Value, entry.Version)

	// 5. Update data.
	newValue := []byte("Alice Wonderland")
	fmt.Printf("Updating value to '%s'\n", newValue)
	if err := db.Set(key, newValue); err != nil {
		log.Fatalf("Update failed: %v", err)
	}

	// Read again to verify.
	entry2, err := db.Get(key)
	if err != nil {
		log.Fatalf("Get failed: %v", err)
	}
	defer entry2.DecrRef()
	fmt.Printf("Read updated: value='%s'\n", entry2.Value)

	// 6. Delete data.
	fmt.Println("Deleting key...")
	if err := db.Del(key); err != nil {
		log.Fatalf("Delete failed: %v", err)
	}

	// Verify deletion.
	_, err = db.Get(key)
	if err == utils.ErrKeyNotFound {
		fmt.Println("Verified: Key not found after delete.")
	} else if err != nil {
		log.Fatalf("Unexpected error: %v", err)
	} else {
		log.Fatal("Error: Key should be deleted but was found!")
	}
}
