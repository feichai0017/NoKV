package config

import (
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
)

// 加载 .env 文件
func init() {
	err := godotenv.Load("/usr/local/Distributed_system/cloud_distributed_storage/Backend/.env")
	if err != nil {
		log.Fatal("Error loading .env file")
		fmt.Println(err.Error())
	}
}

var (
	// MinioAccessKey : 访问Key
	MinioAccessKey = os.Getenv("MINIO_ACCESS_KEY")
	// MinioSecretKey : 访问密钥
	MinioSecretKey = os.Getenv("MINIO_SECRET_KEY")
	// MinioEndpoint : gateway地址
	MinioEndpoint = "172.22.0.20:9000"
	MinioUseSSL   = false
)
