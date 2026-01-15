package main

import (
	"fmt"
	"log"
	"os"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/utils"
)

func main() {
	// 1. 配置选项
	// 这里使用一个临时目录作为工作目录
	workDir := "./no-kv-basic-demo"
	defer os.RemoveAll(workDir) // 清理数据，实际使用中通常不需要

	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = workDir

	// 2. 打开数据库
	fmt.Println("Opening DB...")
	db := NoKV.Open(opt)
	// 务必确保在程序退出前关闭 DB，以保证数据 flush 到磁盘
	defer db.Close()

	// 3. 写入数据 (Set)
	key := []byte("user:1001")
	value := []byte("Alice")
	fmt.Printf("Writing key='%s', value='%s'\n", key, value)

	if err := db.Set(key, value); err != nil {
		log.Fatalf("Set failed: %v", err)
	}

	// 4. 读取数据 (Get)
	fmt.Println("Reading data...")
	entry, err := db.Get(key)
	if err != nil {
		log.Fatalf("Get failed: %v", err)
	}
	defer entry.DecrRef() // 重要：使用完 Entry 后必须减少引用计数

	fmt.Printf("Read success! key='%s', value='%s', version=%d\n",
		entry.Key, entry.Value, entry.Version)

	// 5. 更新数据
	newValue := []byte("Alice Wonderland")
	fmt.Printf("Updating value to '%s'\n", newValue)
	if err := db.Set(key, newValue); err != nil {
		log.Fatalf("Update failed: %v", err)
	}

	// 再次读取验证
	entry2, err := db.Get(key)
	if err != nil {
		log.Fatalf("Get failed: %v", err)
	}
	defer entry2.DecrRef()
	fmt.Printf("Read updated: value='%s'\n", entry2.Value)

	// 6. 删除数据
	fmt.Println("Deleting key...")
	if err := db.Del(key); err != nil {
		log.Fatalf("Delete failed: %v", err)
	}

	// 验证删除
	_, err = db.Get(key)
	if err == utils.ErrKeyNotFound {
		fmt.Println("Verified: Key not found after delete.")
	} else if err != nil {
		log.Fatalf("Unexpected error: %v", err)
	} else {
		log.Fatal("Error: Key should be deleted but was found!")
	}
}
