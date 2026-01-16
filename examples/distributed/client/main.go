package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

func main() {
	// 1. Initialize a Redis client.
	// NoKV's Redis gateway listens on 6380 by default.
	rdb := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6380",
		Password: "", // NoKV does not implement auth yet.
		DB:       0,  // Default DB.
	})

	ctx := context.Background()

	// 2. Test the connection.
	fmt.Println("Connecting to NoKV Cluster via Redis Protocol...")
	pong, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("连接失败 (请确保 ./scripts/run_local_cluster.sh 和 nokv-redis 网关已启动): %v", err)
	}
	fmt.Printf("Connected: %s\n", pong)

	// 3. Write data (SET).
	key := "user:10086"
	value := "Gopher"
	fmt.Printf("\n> SET %s %s\n", key, value)
	err = rdb.Set(ctx, key, value, 0).Err()
	if err != nil {
		log.Fatalf("SET failed: %v", err)
	}
	fmt.Println("Set success!")

	// 4. Read data (GET).
	fmt.Printf("\n> GET %s\n", key)
	val, err := rdb.Get(ctx, key).Result()
	if err != nil {
		log.Fatalf("GET failed: %v", err)
	}
	fmt.Printf("Result: %s\n", val)

	// 5. Set expiry (EXPIRE / TTL).
	// NoKV supports TTL.
	fmt.Printf("\n> EXPIRE %s 60s\n", key)
	ok, err := rdb.Expire(ctx, key, 60*time.Second).Result()
	if err != nil {
		log.Printf("Expire failed (NoKV might imply TTL in SET, or check support): %v", err)
	} else {
		fmt.Printf("Expire set: %v\n", ok)
	}

	// 6. Atomic counter (INCR).
	// This often involves distributed transaction coordination.
	counterKey := "page_view"
	fmt.Printf("\n> INCR %s\n", counterKey)
	newVal, err := rdb.Incr(ctx, counterKey).Result()
	if err != nil {
		log.Printf("INCR failed: %v", err)
	} else {
		fmt.Printf("Counter is now: %d\n", newVal)
	}

	// 7. Delete data (DEL).
	fmt.Printf("\n> DEL %s\n", key)
	deleted, err := rdb.Del(ctx, key).Result()
	if err != nil {
		log.Fatalf("DEL failed: %v", err)
	}
	fmt.Printf("Deleted keys: %d\n", deleted)

	// 8. Verify deletion.
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
