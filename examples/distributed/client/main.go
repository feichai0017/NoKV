package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

func main() {
	// 1. 初始化 Redis 客户端
	// NoKV 的 Redis Gateway 默认监听 6380
	rdb := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6380",
		Password: "", // NoKV 目前没有实现鉴权
		DB:       0,  // 默认 DB
	})

	ctx := context.Background()

	// 2. 测试连接
	fmt.Println("Connecting to NoKV Cluster via Redis Protocol...")
	pong, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("连接失败 (请确保 ./scripts/run_local_cluster.sh 和 nokv-redis 网关已启动): %v", err)
	}
	fmt.Printf("Connected: %s\n", pong)

	// 3. 写入数据 (SET)
	key := "user:10086"
	value := "Gopher"
	fmt.Printf("\n> SET %s %s\n", key, value)
	err = rdb.Set(ctx, key, value, 0).Err()
	if err != nil {
		log.Fatalf("SET failed: %v", err)
	}
	fmt.Println("Set success!")

	// 4. 读取数据 (GET)
	fmt.Printf("\n> GET %s\n", key)
	val, err := rdb.Get(ctx, key).Result()
	if err != nil {
		log.Fatalf("GET failed: %v", err)
	}
	fmt.Printf("Result: %s\n", val)

	// 5. 设置过期时间 (EXPIRE / TTL)
	// NoKV 支持 TTL
	fmt.Printf("\n> EXPIRE %s 60s\n", key)
	ok, err := rdb.Expire(ctx, key, 60*time.Second).Result()
	if err != nil {
		log.Printf("Expire failed (NoKV might imply TTL in SET, or check support): %v", err)
	} else {
		fmt.Printf("Expire set: %v\n", ok)
	}

	// 6. 原子计数 (INCR)
	// 这通常涉及到分布式事务处理
	counterKey := "page_view"
	fmt.Printf("\n> INCR %s\n", counterKey)
	newVal, err := rdb.Incr(ctx, counterKey).Result()
	if err != nil {
		log.Printf("INCR failed: %v", err)
	} else {
		fmt.Printf("Counter is now: %d\n", newVal)
	}

	// 7. 删除数据 (DEL)
	fmt.Printf("\n> DEL %s\n", key)
	deleted, err := rdb.Del(ctx, key).Result()
	if err != nil {
		log.Fatalf("DEL failed: %v", err)
	}
	fmt.Printf("Deleted keys: %d\n", deleted)

	// 8. 验证删除
	_, err = rdb.Get(ctx, key).Result()
	if err == redis.Nil {
		fmt.Println("Verified: Key does not exist.")
	} else if err != nil {
		log.Printf("Unexpected error: %v", err)
	} else {
		log.Printf("Error: Key still exists!")
	}
	
	fmt.Println("\nDemo finished successfully.")
}
