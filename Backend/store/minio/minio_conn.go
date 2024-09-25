package minio

import (
	"bytes"
	"cloud_distributed_storage/Backend/config"
	"context"
	"io/ioutil"
	"log"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var minioClient *minio.Client

func GetMinioClient() *minio.Client {
	if minioClient != nil {
		return minioClient
	}

	var err error
	minioClient, err = minio.New(config.MinioEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(config.MinioAccessKey, config.MinioSecretKey, ""),
		Secure: config.MinioUseSSL,
	})
	if err != nil {
		log.Fatalf("Failed to create MinIO client: %v", err)
	}

	return minioClient
}

func PutObject(bucketName, objectName string, data []byte, contentType string) error {
	ctx := context.Background()
	_, err := GetMinioClient().PutObject(ctx, bucketName, objectName, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{ContentType: contentType})
	return err
}

func GetObject(bucketName, objectName string) ([]byte, error) {
	ctx := context.Background()
	object, err := GetMinioClient().GetObject(ctx, bucketName, objectName, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer object.Close()

	return ioutil.ReadAll(object)
}
