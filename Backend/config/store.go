package config

import (
	cmn "cloud_distributed_storage/Backend/common"
)

const (
	// TempLocalRootDir : 本地临时存储地址的路径
	TempLocalRootDir = "/tmp/fileserver/"
	// TempPartRootDir : 分块文件在本地临时存储地址的路径
	TempPartRootDir = "/tmp/fileserver_part/"
	// MinioRootDir : Minio的存储路径prefix
	MinioRootDir = "/minio"
	// S3RootDir : S3的存储路径prefix
	S3RootDir = "S3/"
	// CurrentStoreType : 设置当前文件的存储类型
	CurrentStoreType = cmn.StoreLocal
)
