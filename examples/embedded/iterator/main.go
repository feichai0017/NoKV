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

	// 1. 批量写入一些有序数据
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

	// 2. 创建迭代器
	// 配置迭代器选项，IsAsc: true 表示正序遍历
	iterOpt := &utils.Options{
		IsAsc: true,
	}
	iter := db.NewIterator(iterOpt)
	defer iter.Close()

	// 3. 范围查询
	// 寻找所有以 "product:" 开头的 key
	// Seek 会跳转到第一个 >= 给定 key 的位置
	startKey := []byte(prefix)
	fmt.Println("\nScanning all products:")

	iter.Seek(startKey)
	if !iter.Valid() {
		fmt.Println("Iterator is invalid immediately after Seek.")
	}

	for ; iter.Valid(); iter.Next() {
		item := iter.Item()
		
		// 检查 key 是否还以 prefix 开头，如果不是说明已经遍历完了目标范围
		// (虽然在这个例子里所有数据都是这个 prefix，但在真实场景中很重要)
		// 注意：这里的 item.Key() 返回的是 InternalKey，需要处理一下或者只看 User Key
		// NoKV 的迭代器直接返回包含 User Key 的 Entry
		entry := item.Entry()
		keyStr := string(entry.Key)
		valStr := string(entry.Value)

		// 简单检查前缀
		if len(keyStr) < len(prefix) || keyStr[:len(prefix)] != prefix {
			fmt.Printf("Stopping at key: %s (prefix mismatch)\n", keyStr)
			entry.DecrRef()
			break
		}

		fmt.Printf("- %s: %s\n", keyStr, valStr)
		
		// 记得释放 Entry
		entry.DecrRef()
	}
	
	// Note: NoKV's MemTable implementation (SkipList) currently ignores the IsAsc option 
	// and only supports forward iteration. Reverse scanning will not work correctly 
	// for data that is still in memory. It might work for on-disk SSTables if supported there.
	// For this example, we skip the reverse scan demo.

	/*
	// 4. 倒序遍历 (Reverse Scan) - Not currently supported for MemTables
	fmt.Println("\nReverse scanning (Top 2):")
	iterDesc := db.NewIterator(&utils.Options{IsAsc: false})
	defer iterDesc.Close()
	
	// 从最后一个可能的 key 开始 (product:005 的下一个字节)
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
